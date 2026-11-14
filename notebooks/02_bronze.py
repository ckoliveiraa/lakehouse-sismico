# Databricks notebook source
# MAGIC %md
# MAGIC # 02_bronze · da landing para a Bronze
# MAGIC
# MAGIC **Lakehouse Sísmico · aula 2 de 4**
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Entra** | os arquivos `.geojson` de `/Volumes/sismos/bronze/landing/terremotos/` |
# MAGIC | **Sai** | `sismos.bronze.terremotos_raw` · 1 linha = 1 resposta da API |
# MAGIC | **Memória** | `/Volumes/sismos/bronze/checkpoints/terremotos_raw` |
# MAGIC | **Regra** | rodar de novo lê só arquivo novo: a 2ª execução grava **zero linhas** |
# MAGIC
# MAGIC A Bronze guarda a resposta **como veio**: nenhuma coluna renomeada, nenhum tipo convertido, nenhum
# MAGIC evento filtrado. Só ganha duas colunas de auditoria: quando entrou e de qual arquivo veio.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0 · Garantir a estrutura
# MAGIC O notebook não confia que alguém criou o catálogo antes: ele garante sozinho. Com `IF NOT EXISTS`, rodar de
# MAGIC novo não muda nada. É o que deixa o notebook funcionar num workspace novo, ou como primeira tarefa de um Job.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS sismos;
# MAGIC CREATE SCHEMA  IF NOT EXISTS sismos.bronze;
# MAGIC CREATE VOLUME  IF NOT EXISTS sismos.bronze.landing;
# MAGIC CREATE VOLUME  IF NOT EXISTS sismos.bronze.checkpoints;

# COMMAND ----------

from pyspark.sql import functions as F

LANDING = "/Volumes/sismos/bronze/landing/terremotos/"
CKPT    = "/Volumes/sismos/bronze/checkpoints/terremotos_raw"
TABELA  = "sismos.bronze.terremotos_raw"

UM_ARQUIVO = f"{LANDING}ano=2025/mes=07.geojson"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Por que `multiLine`
# MAGIC Por padrão, o Spark lê JSON como **um registro por linha** (o formato *JSON Lines*). A resposta do USGS
# MAGIC é **um** objeto só, quebrado em milhares de linhas: uma por evento. Veja o que acontece lendo do jeito padrão:

# COMMAND ----------

errado = spark.read.json(UM_ARQUIVO)
print("colunas:", errado.columns)
print("registros:", errado.count())

# COMMAND ----------

# MAGIC %md
# MAGIC Uma coluna só, `_corrupt_record`, e milhares de "registros" quebrados: cada linha do arquivo foi lida como
# MAGIC um JSON incompleto. Agora com `multiLine`:

# COMMAND ----------

certo = spark.read.option("multiLine", "true").json(UM_ARQUIVO)
print("colunas:", certo.columns)
print("registros:", certo.count())
display(certo.select("type", "metadata.count", F.size("features").alias("eventos")))

# COMMAND ----------

# MAGIC %md
# MAGIC **1 arquivo = 1 linha**, com os eventos todos dentro do array `features`. É exatamente esse o grão da Bronze.
# MAGIC Explodir o array em uma linha por evento é trabalho da Silver, na aula 3.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Auto Loader
# MAGIC O `spark.read` de cima lê **tudo, toda vez**. O Auto Loader (`format("cloudFiles")`) lê **só o que ainda não leu**,
# MAGIC e anota o que já leu no checkpoint.
# MAGIC
# MAGIC | Opção | Para quê |
# MAGIC |---|---|
# MAGIC | `cloudFiles.format = json` | o formato dos arquivos |
# MAGIC | `multiLine = true` | 1 arquivo = 1 objeto |
# MAGIC | `cloudFiles.inferColumnTypes = true` | sem ela, o Auto Loader grava todo campo de JSON como **texto**, inclusive o array `features` inteiro |
# MAGIC | `cloudFiles.schemaLocation` | onde o schema inferido fica guardado, para a próxima execução não adivinhar de novo |
# MAGIC | `cloudFiles.partitionColumns = ano` | transforma a pasta `ano=2025` numa coluna `ano` |
# MAGIC | `checkpointLocation` | a memória: quais arquivos já foram gravados na tabela |
# MAGIC | `trigger(availableNow=True)` | processa o que existe agora e **para**. É streaming que se comporta como batch |

# COMMAND ----------

antes = spark.table(TABELA).count() if spark.catalog.tableExists(TABELA) else 0

consulta = (
    spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine", "true")                       # 1 arquivo = 1 objeto
        .option("cloudFiles.inferColumnTypes", "true")     # array continua array, número continua número
        .option("cloudFiles.schemaLocation", f"{CKPT}/_schema")
        .option("cloudFiles.partitionColumns", "ano")      # pasta ano=2025 vira coluna
        .load(LANDING)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    .writeStream
        .option("checkpointLocation", CKPT)
        .trigger(availableNow=True)                        # processa o que há e para
        .toTable(TABELA)
)
consulta.awaitTermination()

# COMMAND ----------

r = (spark.table(TABELA)
        .agg(F.count("*").alias("linhas"), F.sum(F.size("features")).alias("eventos"))
        .first())

print(TABELA)
print(f"  nesta execução:     {r.linhas - antes} linhas")
print(f"  total na tabela:    {r.linhas} linhas")
print(f"  eventos (features): {r.eventos:,}".replace(",", "."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · O momento da aula
# MAGIC Volte à célula do Auto Loader e rode **de novo**, junto com a de cima. Esperado:
# MAGIC ```
# MAGIC   nesta execução:     0 linhas
# MAGIC   total na tabela:    12 linhas
# MAGIC ```
# MAGIC O checkpoint lembra quais arquivos já foram lidos. É idempotência **sem apagar nada**, e é o que vai deixar o
# MAGIC Job da aula 4 tentar de novo sozinho, de madrugada.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · O que o Delta registrou
# MAGIC A versão 0 é o `CREATE TABLE`. Depois vêm as versões `STREAMING UPDATE` da primeira execução: o Auto Loader
# MAGIC pode dividir os 12 arquivos em mais de um lote (por exemplo, 10 + 2), e cada lote vira uma versão. Some a coluna
# MAGIC `linhas_gravadas`: dá 12. A segunda execução **não cria versão nenhuma**: sem arquivo novo, não há commit.

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {TABELA}")
        .select("version", "timestamp", "operation",
                F.col("operationMetrics")["numOutputRows"].alias("linhas_gravadas")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Conferências
# MAGIC Toda resposta do USGS diz, no `metadata.count`, quantos eventos ela mesma contém. A Bronze tem que concordar
# MAGIC arquivo por arquivo.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT regexp_extract(_source_file, 'mes=([0-9]+)', 1) AS mes,
# MAGIC        metadata.count                               AS diz_a_api,
# MAGIC        size(features)                               AS na_bronze,
# MAGIC        metadata.count = size(features)              AS bate
# MAGIC FROM sismos.bronze.terremotos_raw
# MAGIC ORDER BY mes

# COMMAND ----------

# MAGIC %md
# MAGIC E o schema. Repare em três coisas:
# MAGIC - `features` é `ARRAY<STRUCT<...>>`, igual à API, com os tipos que o próprio JSON tem: `mag` é `DOUBLE`, `time` é
# MAGIC   `BIGINT`. Isso é ler o JSON direito, e não converter: `time` continua em milissegundos, e virar data é trabalho da Silver.
# MAGIC   Sem o `inferColumnTypes`, `features` inteiro viria como uma `STRING` gigante.
# MAGIC - `ano` veio do nome da pasta.
# MAGIC - `_rescued_data` foi criada sozinha: é para onde vai um campo que não cabe no schema, em vez de ser perdido.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE sismos.bronze.terremotos_raw

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Primeira olhada nos eventos
# MAGIC Ainda sem Silver: `explode` na hora da consulta, só para conhecer o dado. Duas descobertas que viram decisão na aula 3.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- nem todo evento é terremoto
# MAGIC SELECT f.properties.type AS tipo, count(*) AS eventos
# MAGIC FROM (SELECT explode(features) AS f FROM sismos.bronze.terremotos_raw)
# MAGIC GROUP BY ALL
# MAGIC ORDER BY eventos DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- os maiores do ano: time ainda é milissegundo, timestamp_millis só traduz na consulta
# MAGIC SELECT f.id,
# MAGIC        f.properties.place                                  AS lugar,
# MAGIC        f.properties.mag                   AS magnitude,
# MAGIC        timestamp_millis(f.properties.time) AS horario_utc,
# MAGIC        f.properties.status
# MAGIC FROM (SELECT explode(features) AS f FROM sismos.bronze.terremotos_raw)
# MAGIC ORDER BY magnitude DESC
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · A resposta da pergunta do `01_extract`
# MAGIC Rode o `01_extract` de novo e depois este notebook: **0 linhas novas**, mesmo que o USGS tenha revisado
# MAGIC eventos e os arquivos tenham mudado.
# MAGIC
# MAGIC O Auto Loader reconhece arquivo pelo **caminho**. Arquivo reescrito com o mesmo nome não é relido
# MAGIC (`cloudFiles.allowOverwrites` é `false` por padrão). É ótimo para não duplicar, e ruim para quem quer a revisão.
# MAGIC Na aula 4, a carga incremental resolve isso do jeito certo: cada execução grava um arquivo **novo**, só com o
# MAGIC que mudou desde a última (`updatedafter`).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recomeçar do zero (só se precisar)
# MAGIC Apagar a tabela **e** o checkpoint, juntos. Apagar só um dos dois deixa o par inconsistente:
# MAGIC - só a tabela: o checkpoint acha que já leu tudo, e nada volta para a tabela;
# MAGIC - só o checkpoint: tudo é relido e a tabela fica com 24 linhas.
# MAGIC
# MAGIC Tire o `#` das duas linhas abaixo para rodar.

# COMMAND ----------

# spark.sql(f"DROP TABLE IF EXISTS {TABELA}")
# dbutils.fs.rm(CKPT, recurse=True)
