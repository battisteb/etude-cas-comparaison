"""Télécharge les données de l'étude de cas (accidents de la route, Australie-Méridionale, 2012-2019).

Source d'origine : Department of Planning, Transport and Infrastructure (DPTI), https://data.sa.gov.au
Copie utilisée par l'article de référence (Rakesh Nain), figée sur un commit pour la reproductibilité :
https://github.com/RakeshNain/Big-data-Analysis-of-Road-Crash-Data-using-PySpark-with-PySpark-Tutorial

Usage :
    python src/telecharger_donnees.py            # -> data/crash_data/*.csv, data/units_data/*.csv, data/metadata-for-road-crash.pdf
"""
import os
import sys
import urllib.request

DEPOT = "RakeshNain/Big-data-Analysis-of-Road-Crash-Data-using-PySpark-with-PySpark-Tutorial"
COMMIT = "dab8e6466747903bf1e5bd21ce63fea31bfb6dcc"
ANNEES = range(2012, 2020)
FICHIERS = ([f"crash_data/{a}_DATA_SA_Crash.csv" for a in ANNEES]
            + [f"units_data/{a}_DATA_SA_Units.csv" for a in ANNEES]
            + ["metadata-for-road-crash.pdf"])

RACINE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def telecharger(destination=os.path.join(RACINE, "data")):
    for chemin in FICHIERS:
        cible = os.path.join(destination, chemin)
        if os.path.exists(cible) and os.path.getsize(cible) > 0:
            continue
        os.makedirs(os.path.dirname(cible), exist_ok=True)
        url = f"https://raw.githubusercontent.com/{DEPOT}/{COMMIT}/{chemin}"
        urllib.request.urlretrieve(url, cible + ".part")
        os.replace(cible + ".part", cible)
        print(f"{os.path.getsize(cible) / 1e6:6.2f} Mo  {chemin}", flush=True)
    return destination


if __name__ == "__main__":
    telecharger(sys.argv[1] if len(sys.argv) > 1 else os.path.join(RACINE, "data"))
