"""
Ingestion de l'entité `bookings`, découpée en 3 fonctions bronze/silver/gold (même structure que
`ingest_airports.py`).

Contrairement aux 3 autres entités, les réservations sont des évènements IMMUABLES exportés en
INCRÉMENTAL : chaque fichier ne contient que les réservations du jour. Silver = insert simple
(idempotent), sans upsert ni désactivation : une réservation absente du fichier du jour n'a pas
été supprimée, elle a simplement été faite un autre jour.
"""
from datetime import date

from common import age_group_sql, age_sql, fetch_csv, get_connection

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


def build_fact_booking(con):
    # Grain : une ligne par réservation. Les clés étrangères pointent vers dim_passenger,
    # dim_flight, dim_airport, dim_date et dim_currency, toutes reconstruites avant ce gold
    # (bookings est la dernière entité de run_month.py).
    # passenger_sk / flight_sk : la VERSION (SCD2) du passager et du vol valide à la date de
    # réservation, i.e. valid_from <= booking_date < valid_to. Les réservations antérieures à la
    # première version connue (celles d'août, avant le snapshot init du 2025-08-31) sont
    # rattachées à la version 1, le plus ancien état disponible. passenger_id / flight_id sont
    # gardés en plus, pour joindre directement sur la clé métier (toutes versions confondues).
    # airport_id est celui enregistré dans la réservation : il est égal à l'aéroport de départ
    # du vol réservé (vérifié sur tout le dataset).
    # amount_eur : conversion au taux fixe de dim_currency, arrondie au centime.
    # Âge et tranche d'âge AU MOMENT DE LA RÉSERVATION (et non à la date de référence de
    # dim_passenger), avec la date de naissance de la version valide à cette date.
    # Les timestamps sont ceux de silver_bookings : une réservation est immuable, ses
    # timestamps ne bougent donc pas d'une reconstruction à l'autre.
    # LEFT JOIN : une réservation n'est jamais perdue, même si une dimension manquait.
    age = age_sql("p.birth_date", "b.booking_date")
    valid_at_booking = """
        AND b.booking_date < coalesce({d}.valid_to, DATE '9999-12-31')
        AND (b.booking_date >= {d}.valid_from OR {d}.version_number = 1)
    """
    con.execute(f"""
        CREATE OR REPLACE TABLE fact_booking AS
        SELECT
            b.booking_id,
            p.passenger_sk,
            f.flight_sk,
            b.passenger_id,
            b.flight_id,
            b.airport_id,
            CAST(strftime(b.booking_date, '%Y%m%d') AS INTEGER) AS date_key,
            b.currency,
            b.booking_date,
            b.seat_class,
            b.booking_channel,
            b.amount,
            CAST(round(b.amount * c.rate_to_eur, 2) AS DECIMAL(12, 2)) AS amount_eur,
            {age} AS age_at_booking,
            {age_group_sql(age)} AS age_group_at_booking,
            b.insert_timestamp,
            b.update_timestamp
        FROM silver_bookings b
        LEFT JOIN dim_passenger p ON p.passenger_id = b.passenger_id {valid_at_booking.format(d='p')}
        LEFT JOIN dim_flight f ON f.flight_id = b.flight_id {valid_at_booking.format(d='f')}
        LEFT JOIN dim_currency c ON c.currency = b.currency
    """)


def build_dim_date(con):
    # Dimension calendrier, couvrant toutes les dates de réservation et de vol. Une date ne
    # change jamais : contrairement au reste du gold, on ne la reconstruit pas, on ajoute
    # seulement les jours manquants (ON CONFLICT DO NOTHING). insert_timestamp reste ainsi celui
    # de la première apparition du jour, au lieu d'être réécrit à chaque exécution.
    con.execute("""
        CREATE TABLE IF NOT EXISTS dim_date (
            date_key INTEGER PRIMARY KEY,
            full_date DATE,
            year INTEGER,
            month INTEGER,
            month_name VARCHAR,
            day_of_month INTEGER,
            day_of_week INTEGER,
            day_name VARCHAR,
            is_weekend BOOLEAN,
            insert_timestamp TIMESTAMP,
            update_timestamp TIMESTAMP
        )
    """)
    con.execute("""
        INSERT INTO dim_date
        WITH bounds AS (
            SELECT min(d) AS d_min, max(d) AS d_max FROM (
                SELECT booking_date AS d FROM silver_bookings
                UNION ALL
                SELECT flight_date FROM dim_flight
            )
        ),
        days AS (
            SELECT CAST(range AS DATE) AS d
            FROM bounds, range(d_min, d_max + INTERVAL 1 DAY, INTERVAL 1 DAY)
        )
        SELECT
            CAST(strftime(d, '%Y%m%d') AS INTEGER),
            d,
            year(d),
            month(d),
            ['janvier', 'février', 'mars', 'avril', 'mai', 'juin', 'juillet', 'août',
             'septembre', 'octobre', 'novembre', 'décembre'][month(d)],
            day(d),
            isodow(d),  -- 1 = lundi ... 7 = dimanche
            ['lundi', 'mardi', 'mercredi', 'jeudi', 'vendredi', 'samedi', 'dimanche'][isodow(d)],
            isodow(d) >= 6,
            now(),
            now()
        FROM days
        ON CONFLICT (date_key) DO NOTHING
    """)


def build_agg_revenue_daily_airline(con):
    # Table d'agrégation : CA (EUR) et nombre de réservations par jour de réservation et par
    # compagnie. Construite ici, après fact_booking, parce que bookings est la dernière entité
    # traitée chaque jour : le fait et toutes les dimensions sont alors à jour.
    # La compagnie est celle de la version du vol valide à la date de réservation (flight_sk).
    # Timestamps : première insertion et dernière mise à jour parmi les réservations agrégées.
    con.execute("""
        CREATE OR REPLACE TABLE agg_revenue_daily_airline AS
        SELECT
            fb.booking_date,
            fb.date_key,
            f.airline,
            count(*) AS nb_bookings,
            sum(fb.amount_eur) AS revenue_eur,
            min(fb.insert_timestamp) AS insert_timestamp,
            max(fb.update_timestamp) AS update_timestamp
        FROM fact_booking fb
        JOIN dim_flight f ON f.flight_sk = fb.flight_sk
        GROUP BY fb.booking_date, fb.date_key, f.airline
    """)


def ingest_gold():
    """Reconstruit fact_booking, dim_date et agg_revenue_daily_airline depuis le silver/gold courant."""
    con = get_connection()
    build_fact_booking(con)
    build_dim_date(con)
    build_agg_revenue_daily_airline(con)
    con.close()


def init():
    ingest_bronze(init=True)
    ingest_silver(init=True)
    ingest_gold()
    print("Réservations (init) ingérés.")


if __name__ == "__main__":
    init()
