"""
Ingestion de l'entité `flights`, découpée en 3 fonctions bronze/silver/gold (même structure que
`ingest_airports.py`).

Comme les aéroports, chaque fichier est un snapshot COMPLET de la table des vols : un vol absent du
snapshot du jour a été supprimé à la source. Silver = upsert + désactivation des clés absentes.
"""
from datetime import date

from common import fetch_csv, get_connection

FLIGHT_COLS = [
    "flight_number", "airline", "origin_airport_id", "destination_airport_id",
    "flight_date", "departure_time", "arrival_time", "aircraft_type",
]


def _snapshot_file(day: date = None, init: bool = False):
    if init:
        return "init", "flights.csv"
    assert day is not None
    return "2025-09", f"flights_{day.isoformat()}.csv"


def ingest_bronze(day: date = None, init: bool = False):
    """Rapatrie le snapshot flights du jour (ou de init/) dans bronze/."""
    fetch_csv(*_snapshot_file(day, init))


def create_silver_table(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS silver_flights (
            flight_id VARCHAR PRIMARY KEY,
            flight_number VARCHAR,
            airline VARCHAR,
            origin_airport_id VARCHAR,
            destination_airport_id VARCHAR,
            flight_date DATE,
            departure_time TIME,
            arrival_time TIME,
            aircraft_type VARCHAR,
            is_active BOOLEAN,
            deleted_date DATE,
            insert_timestamp TIMESTAMP,
            update_timestamp TIMESTAMP
        )
    """)


def ingest_silver(day: date = None, init: bool = False):
    """Relit le snapshot depuis bronze/ et l'upsert dans silver_flights."""
    subdir, filename = _snapshot_file(day, init)
    df = fetch_csv(subdir, filename)  # déjà en cache local : pas de nouveau téléchargement
    snapshot_date = date(2025, 8, 31) if init else day

    con = get_connection()
    create_silver_table(con)
    con.register("snapshot_raw", df)

    # Typage : le bronze est lu tout en VARCHAR, c'est ici qu'on caste (heures au format HH:MM).
    con.execute("""
        CREATE OR REPLACE TEMP TABLE snapshot AS
        SELECT
            flight_id, flight_number, airline, origin_airport_id, destination_airport_id,
            CAST(flight_date AS DATE) AS flight_date,
            CAST(departure_time AS TIME) AS departure_time,
            CAST(arrival_time AS TIME) AS arrival_time,
            aircraft_type
        FROM snapshot_raw
    """)

    # Upsert, comme pour airports, avec une différence : le DO UPDATE n'est appliqué que si la
    # ligne a réellement changé (une valeur différente, ou une ligne désactivée qui réapparaît).
    # update_timestamp reflète donc la date du dernier vrai changement, et rejouer un jour déjà
    # ingéré ne touche aucune ligne. IS DISTINCT FROM (et non <>) pour gérer proprement les NULL.
    set_clause = ", ".join(f"{c} = excluded.{c}" for c in FLIGHT_COLS)
    set_clause += ", is_active = true, deleted_date = NULL, update_timestamp = now()"
    changed = " OR ".join(f"silver_flights.{c} IS DISTINCT FROM excluded.{c}" for c in FLIGHT_COLS)
    changed += " OR NOT silver_flights.is_active"

    con.execute(f"""
        INSERT INTO silver_flights (
            flight_id, {", ".join(FLIGHT_COLS)}, is_active, deleted_date, insert_timestamp, update_timestamp
        )
        SELECT flight_id, {", ".join(FLIGHT_COLS)}, true, NULL, now(), now()
        FROM snapshot
        ON CONFLICT (flight_id) DO UPDATE SET {set_clause}
        WHERE {changed}
    """)

    # Vols absents du snapshot : désactivés (jamais supprimés, des réservations les référencent).
    # Le filtre is_active = true fige deleted_date au premier jour où la disparition est constatée.
    con.execute(
        """
        UPDATE silver_flights SET is_active = false, deleted_date = ?, update_timestamp = now()
        WHERE is_active = true AND flight_id NOT IN (SELECT flight_id FROM snapshot)
        """,
        [snapshot_date],
    )

    con.unregister("snapshot_raw")
    con.close()


def ingest_gold():
    """Reconstruit dim_flight à partir de l'état courant de silver_flights."""
    con = get_connection()
    # Durée en minutes : ~28 % des vols arrivent à une heure « antérieure » à leur départ, ce
    # sont des vols de nuit qui arrivent le lendemain → +1440 min (24 h) quand l'écart est négatif.
    # Les vols désactivés restent dans la dimension (is_active = false) : des réservations
    # passées les référencent encore.
    con.execute("""
        CREATE OR REPLACE TABLE dim_flight AS
        SELECT
            flight_id, flight_number, airline, origin_airport_id, destination_airport_id,
            flight_date, departure_time, arrival_time,
            CASE
                WHEN date_diff('minute', departure_time, arrival_time) < 0
                    THEN date_diff('minute', departure_time, arrival_time) + 1440
                ELSE date_diff('minute', departure_time, arrival_time)
            END AS flight_duration_min,
            aircraft_type, is_active, deleted_date, insert_timestamp, update_timestamp
        FROM silver_flights
    """)
    con.close()


def init():
    ingest_bronze(init=True)
    ingest_silver(init=True)
    ingest_gold()
    print("Vols (init) ingérés.")


if __name__ == "__main__":
    init()
