"""
Ingestion de l'entité `passengers`, découpée en 3 fonctions bronze/silver/gold (même structure que
`ingest_airports.py`).

Les passagers viennent de deux systèmes sources aux formats différents :
- EN : colonnes en anglais, genre `Male/Female`, dates ISO `YYYY-MM-DD` ;
- FR : colonnes en français, genre `Homme/Femme`, dates `DD/MM/YYYY`.
Les ids sont disjoints entre les deux systèmes. Chacun exporte un snapshot COMPLET de ses passagers.
Silver = un schéma unique (union normalisée EN + FR), upsert + désactivation.
"""
from datetime import date

from common import age_group_sql, age_sql, fetch_csv, get_connection

# Les passagers viennent de deux systèmes sources, chacun avec son propre export.
SOURCES = ["en", "fr"]

# Date de référence de l'âge « actuel » dans dim_passenger : dernier jour de la période ingérée.
AGE_REFERENCE_DATE = date(2025, 9, 30)

PASSENGER_COLS = [
    "first_name", "last_name", "gender", "nationality", "email",
    "birth_date", "signup_date", "source_system",
]


def _snapshot_file(lang: str, day: date = None, init: bool = False):
    if init:
        return "init", f"passengers_{lang}.csv"
    assert day is not None
    return "2025-09", f"passengers_{lang}_{day.isoformat()}.csv"


def ingest_bronze(day: date = None, init: bool = False):
    """Rapatrie les deux snapshots passagers (EN et FR) du jour (ou de init/) dans bronze/."""
    for lang in SOURCES:
        fetch_csv(*_snapshot_file(lang, day, init))


def create_silver_table(con):
    # source_system ('EN' | 'FR') garde la trace du système d'origine de chaque passager.
    con.execute("""
        CREATE TABLE IF NOT EXISTS silver_passengers (
            passenger_id VARCHAR PRIMARY KEY,
            first_name VARCHAR,
            last_name VARCHAR,
            gender VARCHAR,
            nationality VARCHAR,
            email VARCHAR,
            birth_date DATE,
            signup_date DATE,
            source_system VARCHAR,
            is_active BOOLEAN,
            deleted_date DATE,
            insert_timestamp TIMESTAMP,
            update_timestamp TIMESTAMP
        )
    """)


def ingest_silver(day: date = None, init: bool = False):
    """Relit les snapshots EN et FR depuis bronze/, les unifie et les upsert dans silver_passengers."""
    df_en = fetch_csv(*_snapshot_file("en", day, init))
    df_fr = fetch_csv(*_snapshot_file("fr", day, init))
    snapshot_date = date(2025, 8, 31) if init else day

    con = get_connection()
    create_silver_table(con)
    con.register("snapshot_en", df_en)
    con.register("snapshot_fr", df_fr)

    # Normalisation des deux formats vers le schéma silver unique (noms EN, genre Male/Female,
    # dates typées). Le CASE n'a pas de ELSE : une valeur de genre inattendue donnerait NULL
    # plutôt qu'une valeur non normalisée.
    con.execute("""
        CREATE OR REPLACE TEMP TABLE snapshot AS
        SELECT
            passenger_id, first_name, last_name, gender, nationality, email,
            CAST(birth_date AS DATE) AS birth_date,
            CAST(signup_date AS DATE) AS signup_date,
            'EN' AS source_system
        FROM snapshot_en
        UNION ALL
        SELECT
            id_passager, prenom, nom,
            CASE genre WHEN 'Homme' THEN 'Male' WHEN 'Femme' THEN 'Female' END,
            nationalite, email,
            CAST(strptime(date_naissance, '%d/%m/%Y') AS DATE),
            CAST(strptime(date_inscription, '%d/%m/%Y') AS DATE),
            'FR'
        FROM snapshot_fr
    """)

    # Upsert seulement si la ligne a réellement changé (cf. ingest_flights.py).
    set_clause = ", ".join(f"{c} = excluded.{c}" for c in PASSENGER_COLS)
    set_clause += ", is_active = true, deleted_date = NULL, update_timestamp = now()"
    changed = " OR ".join(f"silver_passengers.{c} IS DISTINCT FROM excluded.{c}" for c in PASSENGER_COLS)
    changed += " OR NOT silver_passengers.is_active"

    con.execute(f"""
        INSERT INTO silver_passengers (
            passenger_id, {", ".join(PASSENGER_COLS)}, is_active, deleted_date, insert_timestamp, update_timestamp
        )
        SELECT passenger_id, {", ".join(PASSENGER_COLS)}, true, NULL, now(), now()
        FROM snapshot
        ON CONFLICT (passenger_id) DO UPDATE SET {set_clause}
        WHERE {changed}
    """)

    # Désactivation sur l'UNION EN + FR : faite fichier par fichier, le snapshot EN
    # désactiverait tous les passagers FR (et inversement), puisque les ids sont disjoints.
    con.execute(
        """
        UPDATE silver_passengers SET is_active = false, deleted_date = ?, update_timestamp = now()
        WHERE is_active = true AND passenger_id NOT IN (SELECT passenger_id FROM snapshot)
        """,
        [snapshot_date],
    )

    con.unregister("snapshot_en")
    con.unregister("snapshot_fr")
    con.close()


def ingest_gold():
    """Reconstruit dim_passenger à partir de l'état courant de silver_passengers."""
    con = get_connection()
    # L'âge dépend de la date à laquelle on le calcule : ici une date de référence FIXE (fin de
    # la période couverte) plutôt que current_date, pour que le gold reste reproductible. L'âge
    # au moment de chaque réservation, lui, est calculé dans fact_booking.
    age = age_sql("birth_date", f"DATE '{AGE_REFERENCE_DATE.isoformat()}'")
    con.execute(f"""
        CREATE OR REPLACE TABLE dim_passenger AS
        SELECT
            passenger_id, first_name, last_name, gender, nationality, email,
            birth_date, signup_date, source_system,
            {age} AS age,
            {age_group_sql(age)} AS age_group,
            is_active, deleted_date, insert_timestamp, update_timestamp
        FROM silver_passengers
    """)
    con.close()


def init():
    ingest_bronze(init=True)
    ingest_silver(init=True)
    ingest_gold()
    print("Passagers (init) ingérés.")


if __name__ == "__main__":
    init()
