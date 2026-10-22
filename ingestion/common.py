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


def is_stale_snapshot(con, table: str, snapshot_date) -> bool:
    """Vrai si un snapshot PLUS RÉCENT que `snapshot_date` a déjà été chargé dans `table`.

    L'historique SCD2 et la désactivation supposent que les snapshots complets sont traités dans
    l'ordre chronologique : rejouer un ancien snapshot après un plus récent (ex. relancer
    run_month.py, qui repasse par l'init) réactiverait des lignes supprimées et ouvrirait de
    fausses versions. Rejouer le DERNIER snapshot reste autorisé (c'est sans effet).
    """
    con.execute("""
        CREATE TABLE IF NOT EXISTS silver_load_log (
            table_name VARCHAR,
            snapshot_date DATE,
            insert_timestamp TIMESTAMP,
            update_timestamp TIMESTAMP,
            PRIMARY KEY (table_name, snapshot_date)
        )
    """)
    last = con.execute(
        "SELECT max(snapshot_date) FROM silver_load_log WHERE table_name = ?", [table]
    ).fetchall()[0][0]
    if last is not None and snapshot_date < last:
        print(f"  {table} : snapshot du {snapshot_date} ignoré (déjà chargé jusqu'au {last})")
        return True
    return False


def log_snapshot(con, table: str, snapshot_date):
    """Enregistre que le snapshot `snapshot_date` a été chargé dans `table`."""
    con.execute(
        """
        INSERT INTO silver_load_log VALUES (?, ?, now(), now())
        ON CONFLICT (table_name, snapshot_date) DO UPDATE SET update_timestamp = now()
        """,
        [table, snapshot_date],
    )


def update_scd2_history(con, history_table: str, key: str, cols: list, snapshot_date):
    """Met à jour l'historique SCD2 `history_table` à partir de la table temporaire `snapshot`.

    `snapshot` contient l'état complet de la source au `snapshot_date`, déjà typé. Une version
    est valide sur [valid_from, valid_to[ ; valid_to est NULL pour la version courante.
    - clé dont le contenu a changé : la version courante est close, une nouvelle est ouverte ;
    - clé absente du snapshot : la version courante est close (sans nouvelle version) ;
    - clé nouvelle (ou qui réapparaît) : une version est ouverte ;
    - rien n'a changé : rien n'est écrit, donc rejouer le même snapshot est sans effet.
    """
    # Empreinte des attributs suivis : comparer un hash évite une comparaison colonne à colonne.
    # coalesce : sans lui, concat_ws ignorerait les NULL et 'a', NULL, 'b' = 'a', 'b', NULL.
    hash_expr = "md5(concat_ws('|', " + ", ".join(
        f"coalesce(CAST({c} AS VARCHAR), '<NULL>')" for c in cols
    ) + "))"
    con.execute(f"CREATE OR REPLACE TEMP TABLE snapshot_hashed AS SELECT *, {hash_expr} AS row_hash FROM snapshot")

    # 1. Clôture des versions courantes qui ne sont plus vraies (modifiées ou disparues).
    con.execute(
        f"""
        UPDATE {history_table} SET valid_to = ?, is_current = false, update_timestamp = now()
        WHERE is_current AND NOT EXISTS (
            SELECT 1 FROM snapshot_hashed s
            WHERE s.{key} = {history_table}.{key} AND s.row_hash = {history_table}.row_hash
        )
        """,
        [snapshot_date],
    )

    # 2. Ouverture d'une version pour chaque clé du snapshot sans version courante : nouvelles
    # clés, clés modifiées (leur version vient d'être close) et clés réapparues.
    col_list = ", ".join(cols)
    con.execute(
        f"""
        INSERT INTO {history_table} (
            {key}, {col_list}, row_hash, valid_from, valid_to, is_current, insert_timestamp, update_timestamp
        )
        SELECT {key}, {col_list}, row_hash, ?, NULL, true, now(), now()
        FROM snapshot_hashed s
        WHERE NOT EXISTS (
            SELECT 1 FROM {history_table} h WHERE h.{key} = s.{key} AND h.is_current
        )
        ORDER BY {key}
        """,
        [snapshot_date],
    )


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
