"""
Utilitaires partagés par les scripts d'ingestion : `fetch_csv` (téléchargement vers bronze/ puis
relecture depuis le disque) et `get_connection`.
"""
import os
import urllib.request

import duckdb
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BRONZE_DIR = os.path.join(BASE_DIR, "bronze")
DB_PATH = os.path.join(BASE_DIR, "warehouse.duckdb")
RAW_BASE = "https://raw.githubusercontent.com/kevinl75/tp-polytech-dataset/main"


def fetch_csv(subdir: str, filename: str) -> pd.DataFrame:
    """Renvoie le contenu de <subdir>/<filename> sous forme de DataFrame pandas.

    Le fichier est téléchargé dans BRONZE_DIR/<subdir>/<filename> s'il n'y est pas déjà, puis
    toujours relu depuis le disque : bronze = copie brute et immuable de la source.
    """
    local_dir = os.path.join(BRONZE_DIR, subdir)
    local_path = os.path.join(local_dir, filename)
    os.makedirs(local_dir, exist_ok=True)

    if not os.path.exists(local_path):
        # Octets copiés à l'identique (pas de passage par pandas, qui réécrirait le fichier).
        # Écriture dans un .tmp puis renommage : un téléchargement interrompu ne laisse jamais
        # un fichier tronqué que les exécutions suivantes prendraient pour un fichier complet.
        tmp_path = local_path + ".tmp"
        with urllib.request.urlopen(f"{RAW_BASE}/{subdir}/{filename}") as resp:
            content = resp.read()
        with open(tmp_path, "wb") as f:
            f.write(content)
        os.replace(tmp_path, local_path)

    # Tout en chaîne de caractères : le typage est la responsabilité du silver.
    return pd.read_csv(local_path, dtype=str)


def age_sql(birth_expr: str, ref_expr: str) -> str:
    """Expression SQL de l'âge exact (en années révolues) à la date `ref_expr`.

    date_sub compte les années complètes écoulées, contrairement à date_diff qui compte les
    changements d'année civile (date_diff donnerait 1 an entre le 31/12 et le 01/01).
    """
    return f"date_sub('year', {birth_expr}, {ref_expr})"


def age_group_sql(age_expr: str) -> str:
    """Expression SQL de la tranche d'âge, partagée par dim_passenger et fact_booking."""
    return f"""CASE
        WHEN {age_expr} < 18 THEN '<18'
        WHEN {age_expr} < 25 THEN '18-24'
        WHEN {age_expr} < 35 THEN '25-34'
        WHEN {age_expr} < 50 THEN '35-49'
        WHEN {age_expr} < 65 THEN '50-64'
        ELSE '65+'
    END"""


def get_connection() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(DB_PATH)
