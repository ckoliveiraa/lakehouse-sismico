# Databricks notebook source
# MAGIC %md
# MAGIC # 03_silver · da Bronze para a Silver, as duas fontes no mesmo contrato
# MAGIC
# MAGIC **Lakehouse Sísmico · aula 3 de 4**
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Parâmetros** | `fonte` (`usgs` ou `emsc`) · `simular_revisao` (`nao` ou `sim`, só na aula, só com `usgs`) |
# MAGIC | **Entra** | `sismos.bronze.<fonte>_raw` · 1 linha = 1 resposta da API |
# MAGIC | **Sai** | `sismos.silver.eventos` · 1 linha = 1 evento de uma fonte, na versão mais nova |
# MAGIC | **Chave** | `(fonte, id)` |
# MAGIC | **Regra** | rodar de novo não insere nem atualiza nada: a 2ª execução mexe em **zero linhas** |
# MAGIC
# MAGIC As quatro promessas da Silver:
# MAGIC 1. **Grão.** Uma linha por evento: o array `features` vira linhas.
# MAGIC 2. **Tipo.** Cada fonte manda datas, nomes e códigos do seu jeito. A Silver traduz as duas para **um** contrato:
# MAGIC    mesmos nomes, mesmos tipos, mesmo vocabulário.
# MAGIC 3. **Escopo.** Tudo o que a fonte mandou fica, com a coluna `tipo` dizendo o que é. Quem filtra é a Gold.
# MAGIC 4. **Versão.** `MERGE` por `(fonte, id)`: evento novo entra, revisão mais nova atualiza, versão velha é ignorada.
# MAGIC
# MAGIC Na Bronze, a regra era **um notebook, a fonte é parâmetro**, e o código não sabia de onde o dado vinha.
# MAGIC Aqui o notebook continua um só, mas a tradução **é** por fonte: uma função `normalizar_<fonte>` para cada uma.
# MAGIC Todo o resto (escopo, deduplicação, `MERGE`, conferências) é comum.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0 · Garantir a estrutura
# MAGIC A tabela é criada com schema **explícito**, e não inferido. Esse `CREATE TABLE` é o contrato da Silver: quem
# MAGIC consome sabe os nomes, os tipos e o que cada coluna quer dizer (os `COMMENT` aparecem no Catalog Explorer).
# MAGIC
# MAGIC Quatro detalhes:
# MAGIC - `fonte` faz parte da chave. O mesmo terremoto tem um `id` em cada catálogo, e nada garante que dois catálogos
# MAGIC   nunca usem o mesmo `id` para eventos diferentes. A chave é **o par**.
# MAGIC - `PRIMARY KEY` no Unity Catalog é **informativa**: documenta a chave, mas não impede duplicata. Quem impede é o `MERGE`.
# MAGIC - `delta.enableChangeDataFeed` faz o Delta guardar o antes e o depois de cada linha alterada. Fica ligado desde já: é ele que vai medir, na aula 4, quanto a primeira estimativa erra.
# MAGIC - `IF NOT EXISTS`: rodar de novo não muda nada.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS sismos;
# MAGIC CREATE SCHEMA  IF NOT EXISTS sismos.silver;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS sismos.silver.eventos (
# MAGIC   fonte                 STRING    NOT NULL COMMENT 'Catálogo de origem: usgs ou emsc',
# MAGIC   id                    STRING    NOT NULL COMMENT 'Chave do evento no catálogo da fonte',
# MAGIC   evento_em             TIMESTAMP COMMENT 'Quando o chão tremeu, em UTC',
# MAGIC   atualizado_em         TIMESTAMP COMMENT 'Última revisão na fonte, em UTC. É o que decide o MERGE',
# MAGIC   magnitude             DOUBLE,
# MAGIC   tipo_magnitude        STRING    COMMENT 'Escala usada, em minúsculas: mb, ml, md, mww...',
# MAGIC   lugar                 STRING    COMMENT 'Texto livre. USGS: place. EMSC: flynn_region, região Flinn-Engdahl em maiúsculas',
# MAGIC   longitude             DOUBLE,
# MAGIC   latitude              DOUBLE,
# MAGIC   profundidade_km       DOUBLE    COMMENT 'Positiva para baixo. Pode ser negativa: evento acima do nível do mar',
# MAGIC   tipo                  STRING    COMMENT 'earthquake, mining explosion... Códigos do EMSC sem equivalente ficam como emsc:<código>',
# MAGIC   status                STRING    COMMENT 'USGS: automatic ou reviewed. EMSC não publica: nulo',
# MAGIC   tsunami               BOOLEAN   COMMENT 'Flag do USGS para evento grande em região oceânica. Não quer dizer que houve tsunami. EMSC: nulo',
# MAGIC   significancia         INT       COMMENT 'sig do USGS, de 0 a ~3000. EMSC: nulo',
# MAGIC   rede                  STRING    COMMENT 'Quem publicou a solução: net no USGS, auth no EMSC',
# MAGIC   alerta                STRING    COMMENT 'Nível PAGER do USGS: green, yellow, orange, red. Quase sempre nulo. EMSC: nulo',
# MAGIC   url                   STRING,
# MAGIC   _arquivo_origem       STRING    COMMENT 'Arquivo da landing de onde a versão atual veio',
# MAGIC   _atualizado_na_silver TIMESTAMP COMMENT 'Quando o MERGE gravou esta versão',
# MAGIC   CONSTRAINT eventos_pk PRIMARY KEY (fonte, id)
# MAGIC )
# MAGIC COMMENT 'Um evento sísmico por linha e por fonte (USGS, EMSC), na revisão mais nova. Magnitude >= 2,5'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

dbutils.widgets.dropdown("fonte", "usgs", ["usgs", "emsc"])
dbutils.widgets.dropdown("simular_revisao", "nao", ["nao", "sim"])

FONTE    = dbutils.widgets.get("fonte")
SIMULAR  = dbutils.widgets.get("simular_revisao") == "sim" and FONTE == "usgs"
BRONZE   = f"sismos.bronze.{FONTE}_raw"
SILVER   = "sismos.silver.eventos"
COLUNAS  = spark.table(SILVER).columns          # o contrato, lido da própria tabela

print(f"fonte={FONTE}  {BRONZE} -> {SILVER}  simular_revisao={SIMULAR}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Grão: uma linha por evento
# MAGIC Na Bronze, julho é **uma** linha com milhares de eventos dentro do array `features`. O `explode` transforma
# MAGIC cada elemento do array numa linha, e repete nas linhas novas as colunas que você levar junto (aqui, `_source_file`).
# MAGIC Este passo é igual para as duas fontes: as duas mandam um `FeatureCollection`.

# COMMAND ----------

bronze = spark.table(BRONZE)
explodido = bronze.select(F.explode("features").alias("f"), "_source_file")

print(f"Bronze:    {bronze.count():>6} linhas")
print(f"explodido: {explodido.count():>6,} linhas".replace(",", "."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Tipo: uma tradução por fonte, um contrato só
# MAGIC Cada `cast` aqui é uma decisão, e não um enfeite. E cada fonte pede decisões diferentes:
# MAGIC
# MAGIC | Coluna da Silver | USGS | EMSC |
# MAGIC |---|---|---|
# MAGIC | `id` | `id` | `id` (igual a `properties.unid`) |
# MAGIC | `evento_em` | `time`: **ms desde 1970** → `timestamp_millis` | `time`: **texto ISO 8601** (`2025-07-29T23:24:51.44Z`) → `to_timestamp` |
# MAGIC | `atualizado_em` | `updated`, em ms | `lastupdate`, texto ISO |
# MAGIC | `tipo_magnitude` | `magType` (`mww`, `mb`...) | `magtype`. Os dois passam por `lower` |
# MAGIC | `lugar` | `place`: `140 km E of Petropavlovsk-Kamchatsky, Russia` | `flynn_region`: `OFF EAST COAST OF KAMCHATKA` |
# MAGIC | `longitude`, `latitude` | `geometry.coordinates[0]`, `[1]` | `properties.lon`, `lat` |
# MAGIC | `profundidade_km` | `coordinates[2]`, positiva para baixo | `properties.depth`, positiva para baixo. **Não** use o `coordinates[2]` do EMSC: lá ela vem negativa |
# MAGIC | `tipo` | `type`: `earthquake`, `mining explosion`... | `evtype`, em código: `ke` e `fe` viram `earthquake`, o resto fica `emsc:<código>` |
# MAGIC | `status`, `tsunami`, `significancia`, `alerta` | `status`, `tsunami` (0/1), `sig`, `alert` | não existem: **nulo**, e o `COMMENT` avisa |
# MAGIC | `rede` | `net` | `auth` |
# MAGIC | `url` | `url` | montada a partir do `unid` |
# MAGIC
# MAGIC O código do `evtype` tem duas letras: a primeira diz a certeza (`k` conhecido, `s` suspeito, `f` sentido,
# MAGIC `u` sem confirmação), a segunda o que é (`e` terremoto, `i` induzido). Só os de certeza alta viram
# MAGIC `earthquake`. Os demais (uma centena em 79 mil) ficam com o código, à vista: decisão de escopo escrita, e não
# MAGIC um filtro escondido.

# COMMAND ----------

def normalizar_usgs(df):
    return df.select(
        F.col("f.id").alias("id"),
        F.timestamp_millis(F.col("f.properties.time")).alias("evento_em"),
        F.timestamp_millis(F.col("f.properties.updated")).alias("atualizado_em"),
        F.col("f.properties.mag").cast("double").alias("magnitude"),
        F.lower(F.col("f.properties.magType")).alias("tipo_magnitude"),
        F.col("f.properties.place").alias("lugar"),
        F.col("f.geometry.coordinates")[0].cast("double").alias("longitude"),
        F.col("f.geometry.coordinates")[1].cast("double").alias("latitude"),
        F.col("f.geometry.coordinates")[2].cast("double").alias("profundidade_km"),
        F.col("f.properties.type").alias("tipo"),
        F.col("f.properties.status").alias("status"),
        (F.col("f.properties.tsunami") == 1).alias("tsunami"),
        F.col("f.properties.sig").cast("int").alias("significancia"),
        F.col("f.properties.net").alias("rede"),
        F.col("f.properties.alert").alias("alerta"),
        F.col("f.properties.url").alias("url"),
        F.col("_source_file").alias("_arquivo_origem"),
    )


def normalizar_emsc(df):
    evtype = F.col("f.properties.evtype")
    return df.select(
        F.col("f.id").alias("id"),
        F.to_timestamp(F.col("f.properties.time")).alias("evento_em"),
        F.to_timestamp(F.col("f.properties.lastupdate")).alias("atualizado_em"),
        F.col("f.properties.mag").cast("double").alias("magnitude"),
        F.lower(F.col("f.properties.magtype")).alias("tipo_magnitude"),
        F.col("f.properties.flynn_region").alias("lugar"),
        F.col("f.properties.lon").cast("double").alias("longitude"),
        F.col("f.properties.lat").cast("double").alias("latitude"),
        F.col("f.properties.depth").cast("double").alias("profundidade_km"),
        F.when(evtype.isin("ke", "fe"), "earthquake")
         .otherwise(F.concat(F.lit("emsc:"), evtype)).alias("tipo"),
        F.lit(None).cast("string").alias("status"),
        F.lit(None).cast("boolean").alias("tsunami"),
        F.lit(None).cast("int").alias("significancia"),
        F.col("f.properties.auth").alias("rede"),
        F.lit(None).cast("string").alias("alerta"),
        F.concat(F.lit("https://www.seismicportal.eu/eventdetails.html?unid="),
                 F.col("f.properties.unid")).alias("url"),
        F.col("_source_file").alias("_arquivo_origem"),
    )


NORMALIZAR = {"usgs": normalizar_usgs, "emsc": normalizar_emsc}

eventos = (NORMALIZAR[FONTE](explodido)
           .withColumn("fonte", F.lit(FONTE))
           .withColumn("_atualizado_na_silver", F.current_timestamp())
           .select(*COLUNAS))                    # mesma ordem e mesmos nomes da tabela, ou erro aqui

display(eventos.where("magnitude >= 8.5"))

# COMMAND ----------

# MAGIC %md
# MAGIC O terremoto de Kamchatka, nas duas fontes: `2025-07-29 23:24:52` no USGS, `23:24:51` no EMSC, **UTC** nos dois.
# MAGIC Em Kamchatka (UTC+12), já eram 11h24 do dia 30. A Silver guarda UTC sempre: converter para o fuso de quem lê é
# MAGIC trabalho da consulta. Mesmo evento, dois `id`, um segundo de diferença: por isso a chave tem a `fonte`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Escopo: o que fica
# MAGIC Nem todo evento é terremoto. A decisão do curso: **tudo fica na Silver**, com a coluna `tipo`. A Silver é o
# MAGIC registro limpo do que a fonte disse, e apagar explosão de mina aqui decide por todo mundo que vier depois.
# MAGIC Quem precisa só de terremoto filtra na Gold, onde a pergunta é conhecida.

# COMMAND ----------

display(eventos.groupBy("tipo").count().orderBy(F.desc("count")))

# COMMAND ----------

# MAGIC %md
# MAGIC Uma descoberta de quem olha o dado antes de escrever regra: tem evento com **profundidade negativa** nas duas
# MAGIC fontes. Não é erro. A profundidade é medida a partir do nível do mar, e um tremor sob uma montanha pode estar
# MAGIC acima dele. Uma regra "profundidade >= 0" pareceria óbvia, e jogaria fora dado bom.

# COMMAND ----------

display(eventos.where("profundidade_km < 0")
               .select("fonte", "id", "lugar", "profundidade_km", "tipo")
               .orderBy("profundidade_km"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Versão: uma linha por `id` antes do MERGE
# MAGIC O `MERGE` exige que cada linha da Silver case com **no máximo uma** linha da origem. Se o mesmo `id` vier duas
# MAGIC vezes, ele falha com `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE`: não sabe qual das duas gravar.
# MAGIC
# MAGIC Hoje a Bronze não tem `id` repetido. Na aula 4 vai ter: cada carga incremental grava um arquivo novo com as
# MAGIC revisões, e o mesmo evento passa a aparecer em mais de uma linha da Bronze. A janela abaixo fica com a versão de
# MAGIC `atualizado_em` mais recente de cada `(fonte, id)`.

# COMMAND ----------

mais_nova = (Window.partitionBy("fonte", "id")
                   .orderBy(F.col("atualizado_em").desc(), F.col("_arquivo_origem").desc()))

origem = (eventos
          .withColumn("_ordem", F.row_number().over(mais_nova))
          .where("_ordem = 1")
          .drop("_ordem"))

print(f"antes:  {eventos.count():>6,} linhas".replace(",", "."))
print(f"depois: {origem.count():>6,} linhas, uma por (fonte, id)".replace(",", "."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · MERGE
# MAGIC Três casos, uma instrução:
# MAGIC
# MAGIC | A origem traz | A Silver tem | O que acontece |
# MAGIC |---|---|---|
# MAGIC | um `(fonte, id)` novo | nada | `WHEN NOT MATCHED` → **INSERT** |
# MAGIC | o `(fonte, id)`, revisado depois | a versão anterior | `WHEN MATCHED AND s.atualizado_em > t.atualizado_em` → **UPDATE** |
# MAGIC | o `(fonte, id)`, mesma revisão ou mais velha | o `(fonte, id)` | casa, mas a condição é falsa → **nada** |
# MAGIC
# MAGIC Sem a condição no `WHEN MATCHED`, toda execução reescreveria todas as linhas, mesmo sem nada ter mudado, e uma
# MAGIC versão velha relida da landing apagaria a revisão nova.
# MAGIC
# MAGIC `UPDATE SET *` e `INSERT *` casam as colunas **pelo nome**: a origem precisa ter exatamente as colunas da tabela.
# MAGIC O `t.fonte = '{FONTE}'` no `ON` não muda o resultado, mas diz ao Delta que esta execução só toca as linhas de
# MAGIC uma fonte.

# COMMAND ----------

def merge_eventos(df):
    df.createOrReplaceTempView("origem")
    return spark.sql(f"""
        MERGE INTO {SILVER} AS t
        USING origem AS s
        ON t.fonte = '{FONTE}' AND t.fonte = s.fonte AND t.id = s.id
        WHEN MATCHED AND s.atualizado_em > t.atualizado_em THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

display(merge_eventos(origem))

# COMMAND ----------

# MAGIC %md
# MAGIC Na primeira execução de cada fonte: todas as linhas inseridas, nenhuma atualizada.
# MAGIC
# MAGIC ## 6 · O momento da aula
# MAGIC Rode a célula do `MERGE` **de novo**. Esperado: `num_affected_rows = 0`. Todos os `(fonte, id)` casam, mas
# MAGIC nenhum chegou com `atualizado_em` mais novo. Nada é inserido, nada é reescrito.
# MAGIC
# MAGIC Depois troque o widget `fonte` para `emsc` e rode o notebook inteiro: a mesma tabela recebe a segunda fonte, e a
# MAGIC do USGS não é tocada.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Uma revisão chegando (só na aula: `fonte = usgs` e `simular_revisao = sim`)
# MAGIC As fontes revisam eventos o tempo todo, mas não dá para esperar um acontecer durante a aula. Então vamos simular:
# MAGIC o terremoto de Kamchatka chega no USGS com magnitude **8,9** e revisado agora. Antes, anotamos a versão atual
# MAGIC da tabela, para desfazer no fim.

# COMMAND ----------

if SIMULAR:
    versao_antes = spark.sql(f"DESCRIBE HISTORY {SILVER} LIMIT 1").first().version
    print("versão antes da simulação:", versao_antes)

    revisao = (spark.table(SILVER)
               .where("fonte = 'usgs' AND id = 'us6000qw60'")
               .withColumn("magnitude", F.lit(8.9))
               .withColumn("atualizado_em", F.current_timestamp())
               .withColumn("_arquivo_origem", F.lit("simulacao_em_aula"))
               .withColumn("_atualizado_na_silver", F.current_timestamp()))

    display(merge_eventos(revisao))
else:
    print("simulação desligada (simular_revisao = nao, ou fonte diferente de usgs)")

# COMMAND ----------

# MAGIC %md
# MAGIC `num_updated_rows = 1`. O Change Data Feed guardou o **antes e o depois** da linha:

# COMMAND ----------

if SIMULAR:
    display(spark.sql(f"SELECT * FROM table_changes('{SILVER}', {versao_antes + 1})")
            .select("_change_type", "_commit_version", "fonte", "id", "magnitude", "atualizado_em", "_arquivo_origem"))

# COMMAND ----------

# MAGIC %md
# MAGIC `update_preimage` com 8,8 e `update_postimage` com 8,9. É exatamente isso que a aula 4 vai somar para responder
# MAGIC "quanto a primeira estimativa erra".
# MAGIC
# MAGIC Agora releia a Bronze inteira, como o Job vai fazer todo dia. A Bronze diz 8,8, revisado em outubro de 2026.
# MAGIC Quem ganha?

# COMMAND ----------

if SIMULAR:
    display(merge_eventos(origem))
    display(spark.table(SILVER).where("fonte = 'usgs' AND id = 'us6000qw60'")
            .select("fonte", "id", "magnitude", "atualizado_em", "_arquivo_origem"))

# COMMAND ----------

# MAGIC %md
# MAGIC Continua 8,9: `num_updated_rows = 0`. A versão da Bronze é **mais velha** que a simulada, e a condição do
# MAGIC `MERGE` não deixa o passado sobrescrever o presente. Reler a fonte não desfaz revisão.
# MAGIC
# MAGIC ## 8 · Desfazer a simulação
# MAGIC A revisão era de mentira, então a tabela volta para a versão anotada. `RESTORE` cria uma versão **nova** com o
# MAGIC conteúdo antigo: o histórico guarda até o desfazer.

# COMMAND ----------

if SIMULAR:
    display(spark.sql(f"RESTORE TABLE {SILVER} TO VERSION AS OF {versao_antes}"))
    display(spark.table(SILVER).where("fonte = 'usgs' AND id = 'us6000qw60'")
            .select("fonte", "id", "magnitude", "atualizado_em", "_arquivo_origem"))

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {SILVER}")
        .select("version", "timestamp", "operation",
                F.col("operationMetrics")["numTargetRowsInserted"].alias("inseridas"),
                F.col("operationMetrics")["numTargetRowsUpdated"].alias("atualizadas")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9 · Conferências
# MAGIC A Silver tem que concordar com a Bronze **de cada fonte**: o mesmo número de eventos, um por `(fonte, id)`,
# MAGIC nenhuma chave nula.

# COMMAND ----------

display(spark.sql(f"""
    SELECT '{FONTE}'                                                        AS fonte,
           (SELECT sum(size(features)) FROM {BRONZE})                       AS eventos_na_bronze,
           count(*)                                                         AS linhas_na_silver,
           count(DISTINCT id)                                               AS ids_distintos,
           count_if(id IS NULL OR evento_em IS NULL OR magnitude IS NULL)   AS chaves_ou_medidas_nulas
    FROM {SILVER}
    WHERE fonte = '{FONTE}'
"""))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- o escopo, por escrito, das duas fontes: quantas linhas cada decisão afeta
# MAGIC SELECT fonte, tipo, count(*) AS eventos
# MAGIC FROM sismos.silver.eventos
# MAGIC GROUP BY ALL
# MAGIC ORDER BY fonte, eventos DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recomeçar do zero (só se precisar)
# MAGIC A Silver não tem checkpoint: ela é derivada da Bronze inteira a cada execução. Apagar a tabela e rodar o notebook
# MAGIC de novo, uma vez por fonte, reconstrói tudo. Tire o `#` da linha abaixo para rodar.

# COMMAND ----------

# spark.sql(f"DROP TABLE IF EXISTS {SILVER}")
