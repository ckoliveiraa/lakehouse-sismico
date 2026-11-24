# Databricks notebook source
# MAGIC %md
# MAGIC # bronze_ingestao · terremotos, de qualquer fonte, da API até a Bronze
# MAGIC
# MAGIC **Lakehouse Sísmico · aula 3 · substitui o `01_extract` e o `02_bronze` da aula 2**
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Parâmetros** | `fonte` (`usgs` ou `emsc`) · `ano` · `extrair` (`sim` ou `nao`, para quem usa o plano B) |
# MAGIC | **Entra** | API FDSN da fonte (`/count` e `/query`) |
# MAGIC | **Sai** | `landing/terremotos/<fonte>/ano=AAAA/mes=MM.geojson` e `sismos.bronze.<fonte>_raw` |
# MAGIC | **Memória** | `checkpoints/terremotos/<fonte>`: um checkpoint por fonte |
# MAGIC | **Regra** | **um notebook de Bronze por tópico.** Nova fonte de terremoto = nova entrada em `FONTES`, nunca um notebook copiado |
# MAGIC
# MAGIC O que muda de uma fonte para outra mora **só** no dicionário `FONTES`. O resto do código não sabe de onde o dado vem.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS sismos;
# MAGIC CREATE SCHEMA  IF NOT EXISTS sismos.bronze;
# MAGIC CREATE VOLUME  IF NOT EXISTS sismos.bronze.landing;
# MAGIC CREATE VOLUME  IF NOT EXISTS sismos.bronze.checkpoints;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · O contrato de cada fonte
# MAGIC "Mesmo padrão FDSN" não quer dizer "mesmo contrato":
# MAGIC
# MAGIC | | USGS | EMSC |
# MAGIC |---|---|---|
# MAGIC | `format` | `geojson` | `json` (sem ele, o `/count` responde **XML**) |
# MAGIC | `/count` | `{"count", "maxAllowed"}` | só `{"count"}`: o limite (20.000) não vem na resposta |
# MAGIC | pedido grande demais | `400` | `413` |
# MAGIC | `endtime=2025-08-01` | até 01/08 00:00 | **inclui o dia 01/08 inteiro**: com data pura, os meses se sobrepõem |
# MAGIC | id do evento | `id` | `id` = `properties.unid` |
# MAGIC | horário / revisão | `time`, `updated` em **ms desde 1970** | `time`, `lastupdate` em **texto ISO 8601** |
# MAGIC | tipo | `type` = `earthquake` | `evtype` = `ke` (*known earthquake*) |
# MAGIC | lugar | `place` | `flynn_region` |
# MAGIC
# MAGIC A janela sempre vai com hora (`T00:00:00`): resolve o EMSC e não muda nada no USGS.
# MAGIC Os nomes de campo **não** são problema da Bronze, que guarda como veio: ficam para a Silver.

# COMMAND ----------

import json
import os
import time

import requests
from pyspark.sql import functions as F

FONTES = {
    "usgs": {
        "api":    "https://earthquake.usgs.gov/fdsnws/event/1",
        "format": "geojson",
        "limite": None,                   # o /count informa (maxAllowed)
    },
    "emsc": {
        "api":    "https://www.seismicportal.eu/fdsnws/event/1",
        "format": "json",
        "limite": 20000,                  # documentado, não vem no /count
    },
}

dbutils.widgets.dropdown("fonte", "usgs", list(FONTES))
dbutils.widgets.text("ano", "2025")
dbutils.widgets.dropdown("extrair", "sim", ["sim", "nao"])

FONTE   = dbutils.widgets.get("fonte")
ANO     = int(dbutils.widgets.get("ano"))
EXTRAIR = dbutils.widgets.get("extrair") == "sim"

TOPICO  = "terremotos"
cfg     = FONTES[FONTE]
LANDING = f"/Volumes/sismos/bronze/landing/{TOPICO}/{FONTE}"       # terremotos/usgs, terremotos/emsc
CKPT    = f"/Volumes/sismos/bronze/checkpoints/{TOPICO}/{FONTE}"   # um checkpoint POR FONTE
TABELA  = f"sismos.bronze.{FONTE}_raw"                            # usgs_raw, emsc_raw

print(f"fonte={FONTE}  ano={ANO}  extrair={EXTRAIR}")
print(f"  {cfg['api']}\n  -> {LANDING}/ano={ANO}/\n  -> {TABELA}\n  ckpt {CKPT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Extração genérica: API → landing
# MAGIC A mesma lógica do `01_extract`, agora recebendo a configuração da fonte.

# COMMAND ----------

def chamar(cfg, endpoint, params, tentativas=5):
    for i in range(tentativas):
        r = requests.get(f"{cfg['api']}/{endpoint}", params=params, timeout=120)
        if r.status_code == 429 or r.status_code >= 500:
            print(f"  {r.status_code} em /{endpoint}, nova tentativa em {2 ** i}s")
            time.sleep(2 ** i)
            continue
        r.raise_for_status()          # 400/413 e outros 4xx: o pedido está errado
        return r
    r.raise_for_status()


def janela_do_mes(cfg, ano, mes):
    fim = f"{ano + 1}-01-01" if mes == 12 else f"{ano}-{mes + 1:02d}-01"
    return {
        "format":       cfg["format"],
        "minmagnitude": 2.5,
        "orderby":      "time-asc",
        "starttime":    f"{ano}-{mes:02d}-01T00:00:00",
        "endtime":      f"{fim}T00:00:00",     # com hora: o EMSC trata data pura como o dia inteiro
    }


def extrair_mes(cfg, ano, mes):
    janela   = janela_do_mes(cfg, ano, mes)
    contagem = chamar(cfg, "count", janela).json()
    limite   = contagem.get("maxAllowed", cfg["limite"])
    if contagem["count"] > limite:
        raise ValueError(f"{contagem['count']} eventos passa do limite de {limite}")

    resposta  = chamar(cfg, "query", janela)
    recebidos = len(resposta.json()["features"])
    if recebidos != contagem["count"]:
        raise ValueError(f"/count disse {contagem['count']}, vieram {recebidos}")

    destino = f"{LANDING}/ano={ano}/mes={mes:02d}.geojson"
    os.makedirs(os.path.dirname(destino), exist_ok=True)
    with open(destino, "wb") as f:
        f.write(resposta.content)
    return recebidos


def extrair_ano(cfg, ano):
    falhas, total = [], 0
    for mes in range(1, 13):
        try:
            n = extrair_mes(cfg, ano, mes)
            total += n
            print(f"✓ {ano}-{mes:02d}  {n:>6,}".replace(",", "."))
        except Exception as erro:
            falhas.append((f"{ano}-{mes:02d}", str(erro)))
            print(f"✗ {ano}-{mes:02d}  {erro}")
        time.sleep(1)
    print(f"total  {total:,} eventos".replace(",", "."))
    if falhas:
        raise Exception(f"{len(falhas)} mês(es) falharam: {falhas}")

# COMMAND ----------

if EXTRAIR:
    extrair_ano(cfg, ANO)
else:
    print("extrair=nao: usando o que já está na landing (plano B)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Carga genérica: landing → Bronze (Auto Loader)
# MAGIC O mesmo `02_bronze`, com caminho, tabela e checkpoint vindos da fonte.
# MAGIC
# MAGIC **A armadilha do checkpoint.** O checkpoint é a memória de *uma* consulta de streaming: quais arquivos
# MAGIC ela já viu, em que ponto parou e com que schema (`_schema`). Testado no Free Edition: carga do EMSC com um
# MAGIC checkpoint novo funciona; depois, a carga do USGS **com o mesmo checkpoint** falha com
# MAGIC `LOCATION_OVERLAP`, e o arquivo citado no erro é `emsc/ano=2025/mes=11.geojson`, não um do USGS. A
# MAGIC consulta nova herdou a memória da outra fonte. Fora do Unity Catalog, o mesmo erro pode passar **calado**:
# MAGIC arquivos tratados como já lidos, schema de uma fonte aplicado à outra. E não tem conserto fino: só apagando
# MAGIC o checkpoint, o que obriga a reprocessar a primeira fonte inteira.
# MAGIC
# MAGIC Regra: **um checkpoint por fonte**. Aqui ele deriva do mesmo `<tópico>/<fonte>` da landing, então não há como errar.

# COMMAND ----------

def carregar_bronze(landing, tabela, ckpt):
    antes = spark.table(tabela).count() if spark.catalog.tableExists(tabela) else 0
    (
        spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("multiLine", "true")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaLocation", f"{ckpt}/_schema")
            .option("cloudFiles.partitionColumns", "ano")
            .load(landing + "/")
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.col("_metadata.file_path"))
        .writeStream
            .option("checkpointLocation", ckpt)
            .trigger(availableNow=True)
            .toTable(tabela)
    ).awaitTermination()

    r = (spark.table(tabela)
            .agg(F.count("*").alias("linhas"), F.sum(F.size("features")).alias("eventos"))
            .first())
    print(tabela)
    print(f"  nesta execução:     {r.linhas - antes} linhas")
    print(f"  total na tabela:    {r.linhas} linhas")
    print(f"  eventos (features): {r.eventos:,}".replace(",", "."))
    return r.linhas - antes


carregar_bronze(LANDING, TABELA, CKPT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Conferências

# COMMAND ----------

display(spark.sql(f"""
    SELECT regexp_extract(_source_file, 'mes=([0-9]+)', 1) AS mes,
           metadata.count                               AS diz_a_api,
           size(features)                               AS na_bronze,
           metadata.count = size(features)              AS bate
    FROM {TABELA}
    ORDER BY mes
"""))

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {TABELA}")
        .select("version", "timestamp", "operation",
                F.col("operationMetrics")["numOutputRows"].alias("linhas_gravadas")))
