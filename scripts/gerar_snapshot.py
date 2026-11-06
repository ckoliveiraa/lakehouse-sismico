"""Gera o snapshot do plano B: as mesmas respostas que o notebook 01_extract
baixa da API do USGS, salvas byte a byte, no mesmo layout da landing.

Uso (na raiz do repositório):
    python scripts/gerar_snapshot.py

Saída: landing/terremotos/ano=2025/mes=MM.geojson  (12 arquivos)
       landing_2025.zip                              (os mesmos 12, zipados)
"""

import json
import sys
import time
import urllib.parse
import urllib.request
import zipfile
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
API = "https://earthquake.usgs.gov/fdsnws/event/1"
ANO = 2025
MAG_MIN = 2.5
LIMITE_API = 20000


def chamar(endpoint: str, inicio: str, fim: str) -> bytes:
    params = urllib.parse.urlencode({
        "format": "geojson", "starttime": inicio, "endtime": fim,
        "minmagnitude": MAG_MIN, "orderby": "time-asc",
    })
    for tentativa in range(5):
        try:
            with urllib.request.urlopen(f"{API}/{endpoint}?{params}", timeout=60) as r:
                return r.read()
        except Exception as erro:  # noqa: BLE001 - retry simples com backoff
            espera = 2 ** tentativa
            print(f"  falhou ({erro}); nova tentativa em {espera}s", file=sys.stderr)
            time.sleep(espera)
    raise RuntimeError(f"desisti: {endpoint} {inicio}..{fim}")


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")
    total = 0
    arquivos = []
    for mes in range(1, 13):
        inicio = f"{ANO}-{mes:02d}-01"
        fim = f"{ANO + (mes == 12)}-{mes % 12 + 1:02d}-01"
        esperado = json.loads(chamar("count", inicio, fim))["count"]
        if esperado > LIMITE_API:
            raise RuntimeError(f"{inicio}: {esperado} eventos passa do limite da API")
        corpo = chamar("query", inicio, fim)
        recebido = len(json.loads(corpo)["features"])
        if recebido != esperado:
            raise RuntimeError(f"{inicio}: /count disse {esperado}, vieram {recebido}")
        destino = RAIZ / "landing" / "terremotos" / f"ano={ANO}" / f"mes={mes:02d}.geojson"
        destino.parent.mkdir(parents=True, exist_ok=True)
        destino.write_bytes(corpo)
        arquivos.append(destino)
        total += recebido
        print(f"{inicio[:7]}  {recebido:>6,} eventos".replace(",", "."))
        time.sleep(1)

    with zipfile.ZipFile(RAIZ / "landing_2025.zip", "w", zipfile.ZIP_DEFLATED) as z:
        for a in arquivos:
            z.write(a, a.relative_to(RAIZ / "landing").as_posix())
    print(f"total    {total:,} eventos em {len(arquivos)} arquivos".replace(",", "."))


if __name__ == "__main__":
    main()
