# Databricks notebook source
# MAGIC %md
# MAGIC # 04_gold · da Silver para a primeira resposta
# MAGIC
# MAGIC **Lakehouse Sísmico · aula 3 de 4**
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Entra** | `sismos.silver.eventos` · as duas fontes |
# MAGIC | **Sai** | `sismos.gold.terremotos_por_mes` · 1 linha por mês **e por fonte** |
# MAGIC | **Filtro** | só `tipo = 'earthquake'`: aqui a pergunta é sobre terremoto |
# MAGIC | **Regra** | `CREATE OR REPLACE`: a Gold é recalculada inteira a cada execução |
# MAGIC
# MAGIC A pergunta da resseguradora: **onde o chão treme, com que força, com que frequência, e quanto a primeira
# MAGIC estimativa erra.** Em aula sai a frequência. **Com que força** e **onde** são o desafio 3, num notebook só de SQL.
# MAGIC A quarta precisa do Change Data Feed acumulando revisões, e fica para a aula 4.
# MAGIC
# MAGIC Por que recalcular inteira, se na Bronze e na Silver a gente fez de tudo para não reprocessar? Porque a Gold é
# MAGIC **pequena** (dezenas de linhas) e **derivada**: tudo o que ela tem dá para refazer a partir da Silver. Na
# MAGIC Bronze, o `overwrite` jogaria fora o histórico. Aqui, não há o que perder. E o Delta guarda as versões anteriores
# MAGIC mesmo assim.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS sismos;
# MAGIC CREATE SCHEMA  IF NOT EXISTS sismos.gold;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Com que frequência: terremotos por mês e por fonte
# MAGIC A `fonte` entra no `GROUP BY` e vira coluna da Gold. Somar USGS com EMSC daria um número sem sentido: o mesmo
# MAGIC terremoto grande está nos dois catálogos e seria contado duas vezes. A Gold mostra as duas visões lado a lado,
# MAGIC e quem lê escolhe.
# MAGIC
# MAGIC `count_if` conta as linhas em que a condição é verdadeira: dá várias contagens numa passada só.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE sismos.gold.terremotos_por_mes
# MAGIC COMMENT 'Terremotos de magnitude >= 2,5 por mês (UTC) e por fonte. Não some as fontes: o mesmo evento está nas duas. Origem: sismos.silver.eventos, tipo = earthquake'
# MAGIC AS
# MAGIC SELECT CAST(date_trunc('MONTH', evento_em) AS DATE) AS mes,
# MAGIC        fonte,
# MAGIC        count(*)                                    AS terremotos,
# MAGIC        round(avg(magnitude), 2)                    AS magnitude_media,
# MAGIC        max(magnitude)                              AS magnitude_max,
# MAGIC        count_if(magnitude >= 5)                    AS m5_ou_mais
# MAGIC FROM sismos.silver.eventos
# MAGIC WHERE tipo = 'earthquake'
# MAGIC GROUP BY ALL
# MAGIC ORDER BY mes, fonte;
# MAGIC
# MAGIC SELECT * FROM sismos.gold.terremotos_por_mes ORDER BY mes, fonte;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · As duas fontes lado a lado
# MAGIC A Gold tem uma linha por mês e fonte, o formato certo para guardar. Para **ler**, um analista quer as fontes em
# MAGIC colunas. `FILTER (WHERE ...)` faz isso sem `PIVOT`: cada agregação olha só as linhas da sua fonte.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT mes,
# MAGIC        sum(terremotos) FILTER (WHERE fonte = 'usgs') AS usgs,
# MAGIC        sum(terremotos) FILTER (WHERE fonte = 'emsc') AS emsc,
# MAGIC        sum(m5_ou_mais) FILTER (WHERE fonte = 'usgs') AS usgs_m5,
# MAGIC        sum(m5_ou_mais) FILTER (WHERE fonte = 'emsc') AS emsc_m5
# MAGIC FROM sismos.gold.terremotos_por_mes
# MAGIC GROUP BY mes
# MAGIC ORDER BY mes

# COMMAND ----------

# MAGIC %md
# MAGIC Duas leituras:
# MAGIC - **No total**, o EMSC vê quase o triplo. Ele agrega as redes regionais da Europa e do Mediterrâneo, que
# MAGIC   registram muito terremoto pequeno que o USGS não cataloga fora dos EUA.
# MAGIC - **Na magnitude 5 ou mais**, os dois ficam perto. Terremoto grande, a rede global inteira vê.
# MAGIC
# MAGIC Quem conta terremoto pequeno mede onde tem sismógrafo, e não onde o chão treme. É o **viés de detecção**, e o
# MAGIC desafio 3 vai medir isso grau a grau.
# MAGIC
# MAGIC Julho tem o maior número de magnitude 5 ou mais nas duas fontes: são as réplicas de Kamchatka. Uma média anual
# MAGIC esconderia isso.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Conferências
# MAGIC A Gold tem que somar, por fonte, o mesmo que a Silver filtrada. Se a consulta perder linha no caminho (um
# MAGIC `JOIN` errado, um filtro a mais), a soma denuncia.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT s.fonte,
# MAGIC        s.na_silver,
# MAGIC        g.na_gold,
# MAGIC        s.na_silver = g.na_gold AS bate
# MAGIC FROM (SELECT fonte, count(*) AS na_silver FROM sismos.silver.eventos
# MAGIC       WHERE tipo = 'earthquake' GROUP BY fonte) AS s
# MAGIC JOIN (SELECT fonte, sum(terremotos) AS na_gold FROM sismos.gold.terremotos_por_mes
# MAGIC       GROUP BY fonte) AS g
# MAGIC   ON s.fonte = g.fonte
# MAGIC ORDER BY s.fonte

# COMMAND ----------

# MAGIC %md
# MAGIC Rode o notebook de novo: a tabela é recriada e os números não mudam. `DESCRIBE HISTORY` mostra uma versão
# MAGIC `CREATE OR REPLACE TABLE AS SELECT` por execução.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY sismos.gold.terremotos_por_mes
