# Databricks notebook source
# MAGIC %md
# MAGIC # 01_extract · da API para a landing
# MAGIC
# MAGIC **Lakehouse Sísmico · aula 2 de 4**
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Entra** | API FDSN do USGS (`/count` e `/query`) |
# MAGIC | **Sai** | 12 arquivos em `/Volumes/sismos/bronze/landing/terremotos/ano=2025/mes=MM.geojson` |
# MAGIC | **Regra** | rodar de novo sobrescreve os mesmos 12 arquivos, nunca duplica |
# MAGIC
# MAGIC As quatro decisões que este notebook carrega:
# MAGIC 1. **Landing antes da tabela.** A resposta vai para o volume byte a byte. Se o USGS revisar o passado amanhã, você ainda tem o que ele disse hoje.
# MAGIC 2. **Fatia por mês, conferida.** A API devolve no máximo 20.000 eventos por consulta. Antes de cada mês, o `/count` diz quantos virão, e o código confere se vieram todos.
# MAGIC 3. **Caminho determinístico.** `ano=2025/mes=07.geojson`: o nome do arquivo depende só da janela pedida.
# MAGIC 4. **Falha que fica vermelha.** Retry só para 429 e 5xx. Um 400 é pedido errado, e repetir não conserta.
# MAGIC
# MAGIC > **Plano B?** Se você subiu o `landing_2025.zip` na aula 1, **pule este notebook** e vá direto para o `02_bronze`. O resultado final é idêntico.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0 · Garantir a estrutura
# MAGIC Este notebook só escreve no volume `landing`. Ele garante o caminho até lá sozinho, sem depender de alguém ter
# MAGIC criado antes. Com `IF NOT EXISTS`, rodar de novo não muda nada.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS sismos;
# MAGIC CREATE SCHEMA  IF NOT EXISTS sismos.bronze;
# MAGIC CREATE VOLUME  IF NOT EXISTS sismos.bronze.landing;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Parâmetros
# MAGIC O widget `ano` aparece no topo do notebook. Na aula 4, ele vira parâmetro do Job.

# COMMAND ----------

import json
import os
import time

import requests

dbutils.widgets.text("ano", "2025")
ANO = int(dbutils.widgets.get("ano"))

API     = "https://earthquake.usgs.gov/fdsnws/event/1"
LANDING = "/Volumes/sismos/bronze/landing/terremotos"
FILTRO  = {"format": "geojson", "minmagnitude": 2.5, "orderby": "time-asc"}

print(f"ano={ANO}  ->  {LANDING}/ano={ANO}/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Conhecer a API antes de escrever o loop
# MAGIC Duas perguntas, uma chamada cada:
# MAGIC - **Quanto vem?** O `/count` responde em milissegundos e não baixa nenhum evento.
# MAGIC - **Como vem?** Um `/query` com `limit=1` mostra a anatomia de um evento.

# COMMAND ----------

ano_inteiro = {**FILTRO, "starttime": f"{ANO}-01-01", "endtime": f"{ANO + 1}-01-01"}
r = requests.get(f"{API}/count", params=ano_inteiro, timeout=30)
print(r.status_code, r.json())

# COMMAND ----------

# MAGIC %md
# MAGIC `count` passa de `maxAllowed`. O que acontece se pedirmos o ano inteiro mesmo assim?

# COMMAND ----------

r = requests.get(f"{API}/query", params=ano_inteiro, timeout=60)
print(r.status_code)
print(r.text[:400])

# COMMAND ----------

# MAGIC %md
# MAGIC **400**, com o motivo escrito: `exceeds search limit of 20000`. É um erro do *pedido*, e não do servidor:
# MAGIC tentar de novo daria o mesmo 400. Por isso o ano vai em **12 fatias**, uma por mês.
# MAGIC
# MAGIC Agora um evento só, de julho, para ver o formato:

# COMMAND ----------

julho   = {**FILTRO, "starttime": f"{ANO}-07-01", "endtime": f"{ANO}-08-01", "limit": 1}
amostra = requests.get(f"{API}/query", params=julho, timeout=30).json()

print("chaves do topo:", list(amostra.keys()))
print("metadata:", amostra["metadata"])
print()
print(json.dumps(amostra["features"][0], indent=2, ensure_ascii=False))

# COMMAND ----------

# MAGIC %md
# MAGIC Repare, porque é assunto da aula 3:
# MAGIC - `time` e `updated` são **milissegundos desde 1970**, e não datas.
# MAGIC - `geometry.coordinates` é `[longitude, latitude, profundidade_km]`, nessa ordem.
# MAGIC - `properties.type` nem sempre é `earthquake`.
# MAGIC - `properties.status` é `automatic` (estimativa do computador) ou `reviewed` (conferido por um sismólogo).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Uma chamada que sabe falhar
# MAGIC | Resposta | O que significa | O que fazer |
# MAGIC |---|---|---|
# MAGIC | `200` | deu certo | seguir |
# MAGIC | `429` | chamadas demais | esperar e tentar de novo |
# MAGIC | `5xx` | servidor com problema | esperar e tentar de novo |
# MAGIC | `400` e outros `4xx` | o **pedido** está errado | falhar na hora: repetir não conserta |
# MAGIC
# MAGIC A espera dobra a cada tentativa (*backoff exponencial*): 1, 2, 4, 8, 16 segundos.

# COMMAND ----------

def chamar(endpoint, params, tentativas=5):
    for i in range(tentativas):
        r = requests.get(f"{API}/{endpoint}", params=params, timeout=60)
        if r.status_code == 429 or r.status_code >= 500:
            print(f"  {r.status_code} em /{endpoint}, nova tentativa em {2 ** i}s")
            time.sleep(2 ** i)
            continue
        r.raise_for_status()   # 400 e outros 4xx: falha na hora
        return r
    r.raise_for_status()       # esgotou as tentativas: falha com o último erro

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Um mês: perguntar, baixar, conferir, gravar
# MAGIC A função faz uma fatia inteira. Se qualquer conferência falhar, ela levanta erro **antes** de gravar:
# MAGIC arquivo pela metade nunca chega à landing.

# COMMAND ----------

def janela_do_mes(ano, mes):
    inicio = f"{ano}-{mes:02d}-01"
    fim    = f"{ano + 1}-01-01" if mes == 12 else f"{ano}-{mes + 1:02d}-01"
    return {**FILTRO, "starttime": inicio, "endtime": fim}


def extrair_mes(ano, mes):
    janela = janela_do_mes(ano, mes)

    contagem = chamar("count", janela).json()
    if contagem["count"] > contagem["maxAllowed"]:
        raise ValueError(f"{contagem['count']} eventos passa do limite de {contagem['maxAllowed']}")

    resposta  = chamar("query", janela)
    recebidos = len(resposta.json()["features"])
    if recebidos != contagem["count"]:
        raise ValueError(f"/count disse {contagem['count']}, vieram {recebidos}")

    destino = f"{LANDING}/ano={ano}/mes={mes:02d}.geojson"
    os.makedirs(os.path.dirname(destino), exist_ok=True)
    with open(destino, "wb") as f:
        f.write(resposta.content)   # byte a byte, como veio

    return contagem["count"], recebidos


# teste com um mês só antes do loop
print(extrair_mes(ANO, 7))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · O ano inteiro
# MAGIC Um mês que falha **não para** os outros: o erro é anotado e o loop segue. No fim, se a lista de falhas
# MAGIC não estiver vazia, o notebook termina em vermelho. É isso que vai fazer o Job da aula 4 avisar alguém.
# MAGIC
# MAGIC A pausa de 1 segundo entre meses é educação: a API é pública, e a turma inteira sai pelo mesmo endereço.

# COMMAND ----------

falhas, total = [], 0
print("          /count  recebidos")
for mes in range(1, 13):
    try:
        esperado, recebidos = extrair_mes(ANO, mes)
        total += recebidos
        print(f"✓ {ANO}-{mes:02d}  {esperado:>6,}  {recebidos:>6,}".replace(",", "."))
    except Exception as erro:
        falhas.append((f"{ANO}-{mes:02d}", str(erro)))
        print(f"✗ {ANO}-{mes:02d}  {erro}")
    time.sleep(1)

print(f"total  {total:,} eventos".replace(",", "."))
if falhas:
    raise Exception(f"{len(falhas)} mês(es) falharam: {falhas}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Conferir a landing
# MAGIC Esperado: **12 arquivos**, uns 21 MB no total. Rode o notebook inteiro de novo e confira: continuam 12.

# COMMAND ----------

arquivos = dbutils.fs.ls(f"{LANDING}/ano={ANO}/")
for a in arquivos:
    print(f"{a.name:<16} {a.size / 1e6:>5.1f} MB")
print(f"{len(arquivos)} arquivos · {sum(a.size for a in arquivos) / 1e6:.1f} MB")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Para pensar antes do `02_bronze`
# MAGIC Rodar este notebook de novo **sobrescreve** os 12 arquivos, possivelmente com dados revisados pelo USGS.
# MAGIC O Auto Loader vai perceber? A resposta aparece no fim do próximo notebook.
