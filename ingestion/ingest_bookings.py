"""
Ingestion de l'entité `bookings`, découpée en 3 fonctions bronze/silver/gold (même structure que
`ingest_airports.py`).

Contrairement aux 3 autres entités, les réservations sont des évènements IMMUABLES exportés en
INCRÉMENTAL : chaque fichier ne contient que les réservations du jour. Silver = insert simple
(idempotent), sans upsert ni désactivation : une réservation absente du fichier du jour n'a pas
été supprimée, elle a simplement été faite un autre jour.
"""
from datetime import date

from common import fetch_csv, get_connection

BOOKING_COLS = [
    "passenger_id", "flight_id", "airport_id", "seat_class", "amount", "currency",
    "booking_date", "booking_channel",
]


def _daily_file(day: date = None, init: bool = False):
    # Contrairement aux 3 autres entités, ce n'est pas un snapshot complet : chaque fichier ne
    # contient que les réservations du jour (init/ = l'historique d'avant septembre).
    if init:
        return "init", "bookings.csv"
    assert day is not None
    return "2025-09", f"bookings_{day.isoformat()}.csv"


def ingest_bronze(day: date = None, init: bool = False):
    """Rapatrie le fichier de réservations du jour (ou de init/) dans bronze/."""
    fetch_csv(*_daily_file(day, init))


def create_silver_table(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS silver_bookings (
            booking_id VARCHAR PRIMARY KEY,
            passenger_id VARCHAR,
            flight_id VARCHAR,
            airport_id VARCHAR,
            seat_class VARCHAR,
            amount DECIMAL(10, 2),
            currency VARCHAR,
            booking_date DATE,
            booking_channel VARCHAR,
            insert_timestamp TIMESTAMP,
            update_timestamp TIMESTAMP
        )
    """)


def ingest_silver(day: date = None, init: bool = False):
    """Relit le fichier du jour depuis bronze/ et insère ses réservations dans silver_bookings."""
    df = fetch_csv(*_daily_file(day, init))

    con = get_connection()
    create_silver_table(con)
    con.register("daily", df)

    # Insert pur : ON CONFLICT DO NOTHING rend le rejeu d'un jour sans effet (pas de doublon,
    # et les timestamps d'une réservation déjà connue ne bougent pas). update_timestamp vaut
    # insert_timestamp, puisqu'une réservation n'est jamais modifiée ensuite.
    con.execute(f"""
        INSERT INTO silver_bookings (
            booking_id, {", ".join(BOOKING_COLS)}, insert_timestamp, update_timestamp
        )
        SELECT
            booking_id, passenger_id, flight_id, airport_id, seat_class,
            CAST(amount AS DECIMAL(10, 2)), currency, CAST(booking_date AS DATE), booking_channel,
            now(), now()
        FROM daily
        ON CONFLICT (booking_id) DO NOTHING
    """)

    con.unregister("daily")
    con.close()


def ingest_gold():
    # TODO (jalon 3) : reconstruire fact_booking et l'agrégat agg_revenue_daily_airline
    raise NotImplementedError


def init():
    ingest_bronze(init=True)
    ingest_silver(init=True)
    ingest_gold()
    print("Réservations (init) ingérés.")


if __name__ == "__main__":
    init()
