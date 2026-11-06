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
| `scripts/gerar_snapshot.py` | Como o plano B foi gerado: mesmos parâmetros do notebook da aula, com cada mês conferido contra o `/count`. |

## Contagens esperadas

Medidas em 06/11/2026. O USGS ainda revisa 2025, então uma extração nova pode diferir em
algumas unidades — o que precisa bater é o `/count` do seu dia com o que você baixou.

| Onde | Esperado |
|---|---:|
| arquivos em `landing/terremotos/` | 12 |
| linhas em `sismos.bronze.terremotos_raw` | 12 |
| `sum(size(features))` | 29.265 |
| linhas novas na 2ª execução da Bronze | 0 |

## Origem e licença

Dados do [U.S. Geological Survey — Earthquake Hazards Program](https://earthquake.usgs.gov/),
via [API FDSN de eventos](https://earthquake.usgs.gov/fdsnws/event/1/). Domínio público.
