# Lakehouse Sísmico — dados do curso

Material do projeto **Lakehouse Sísmico** da trilha Engenheiro de Dados Pleno (Empregadados):
quatro encontros de 1h20 construindo um pipeline medalhão no **Databricks Free Edition**,
da API de terremotos do USGS à camada Gold.

📖 **Guia do curso:** [ckoliveiraa.github.io/lakehouse-sismico](https://ckoliveiraa.github.io/lakehouse-sismico/)
— publicado via GitHub Pages a partir de [`docs/`](docs/).

Este repositório guarda **só o material de apoio**. O código você escreve durante as aulas.

## O que tem aqui

| Arquivo | Para quê |
|---|---|
| `landing_2025.zip` | **Plano B.** As 12 respostas da API (2025, magnitude ≥ 2,5), byte a byte, no layout `terremotos/ano=2025/mes=MM.geojson`. Use só se o seu workspace não conseguir chamar a API. |
| `notebooks/01_extract.py`, `notebooks/02_bronze.py` | Gabarito da aula 2 (substituído pelo `bronze_ingestao` a partir da aula 3), no formato de notebook do Databricks (baixe e importe em *Workspace › Import*). Publicado depois do encontro. |
| `notebooks/03_silver.py`, `notebooks/04_gold.py` | Notebooks da aula 3. Silver: as duas fontes em `silver.eventos`, chave `(fonte, id)`, parâmetro `fonte` e uma tradução por fonte, com `MERGE` pela revisão mais nova. Gold: `terremotos_por_mes`, por fonte. As demais Gold são o desafio 3. |
| `notebooks/bronze_ingestao.py` | Gabarito do desafio 2 e padrão da Bronze a partir da aula 3: **um** notebook de Bronze por tópico. Parâmetro `fonte` (`usgs` ou `emsc`); landing em `terremotos/<fonte>/`, checkpoint em `checkpoints/terremotos/<fonte>`. |
| `scripts/gerar_snapshot.py` | Como o plano B foi gerado: mesmos parâmetros do notebook da aula, com cada mês conferido contra o `/count`. |

## Contagens esperadas

Medidas em 06/11/2026. O USGS ainda revisa 2025, então uma extração nova pode diferir em
algumas unidades — o que precisa bater é o `/count` do seu dia com o que você baixou.

| Onde | Esperado |
|---|---:|
| arquivos em `landing/terremotos/usgs/` (até a aula 2: `landing/terremotos/`) | 12 |
| linhas em `sismos.bronze.usgs_raw` (até a aula 2: `terremotos_raw`) | 12 |
| `sum(size(features))` | 29.265 |
| linhas novas na 2ª execução da Bronze | 0 |
| linhas em `sismos.silver.eventos` com `fonte = 'usgs'` | 29.265 |
| linhas em `sismos.silver.eventos` com `fonte = 'emsc'` (extração de 23/11/2026) | 79.492 |
| linhas mexidas no 2º `MERGE` da Silver | 0 |
| terremotos em `gold.terremotos_por_mes`, por fonte | usgs 29.022 · emsc 79.378 |

## Origem e licença

Dados do [U.S. Geological Survey — Earthquake Hazards Program](https://earthquake.usgs.gov/),
via [API FDSN de eventos](https://earthquake.usgs.gov/fdsnws/event/1/). Domínio público.
Catálogo europeu do [EMSC](https://www.seismicportal.eu/), via a [API FDSN do Seismic Portal](https://www.seismicportal.eu/fdsnws/event/1/).
