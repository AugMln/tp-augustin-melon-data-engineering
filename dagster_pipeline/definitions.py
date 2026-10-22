"""
Bonus Dagster : le pipeline de run_month.py exprimé en assets, partitionnés par jour (septembre 2025).

Lancement (depuis la racine du repo, venv activé) :
    dagster dev -f dagster_pipeline/definitions.py
puis, dans l'UI : « Materialize all » sur l'ensemble des assets, en sélectionnant les 30 partitions
(backfill). Dagster enchaîne alors init_load, puis bronze + silver sur les 30 jours, puis le gold.

Les assets ne font qu'appeler les fonctions ingest_bronze / ingest_silver / ingest_gold existantes :
chacune relit ce que la précédente a persisté (fichier bronze, table silver), aucune donnée ne
transite par Dagster. Les assets sont annotés `-> None`, ce qui désactive l'IO manager (qui sinon
tenterait de sauvegarder leur valeur de retour, et échoue sur un run couvrant plusieurs partitions).

Choix de modélisation des assets :
- init_load (non partitionné) : dim_currency + snapshot init/ des 4 entités, comme le début de
  run_month.py. En amont de tous les assets bronze.
- bronze_* / silver_* : partitionnés par jour. Les snapshots complets doivent être chargés dans
  l'ordre chronologique (désactivation, historique SCD2), d'où :
    * une auto-dépendance de chaque silver sur son jour J-1 : on ne peut pas matérialiser un jour
      avant la veille ;
    * BackfillPolicy.single_run : un backfill de N jours est exécuté en UN run, qui parcourt les
      jours dans l'ordre (plutôt que N runs que le daemon pourrait intercaler avec le gold).
  Le silver ignore de plus tout snapshot antérieur au dernier chargé (silver_load_log).
- gold_* : NON partitionnés. ingest_gold reconstruit la table depuis l'état COURANT du silver
  (CREATE OR REPLACE) : une partition « gold du jour J » n'aurait pas de sens, elle contiendrait
  ce que le silver contient au moment du run. Le gold dépend de toutes les partitions de son
  silver et se matérialise après elles ; gold_bookings passe après les dimensions gold.
- Un seul écrivain DuckDB : warehouse.duckdb n'accepte qu'un processus en écriture à la fois. Les
  assets qui y écrivent sont dans le pool `duckdb`, limité à 1 dans dagster.yaml (racine du repo).
  Le bronze n'écrit que des fichiers : il reste hors pool.
"""
import os
import sys
from datetime import date

from dagster import (
    AssetDep,
    AssetExecutionContext,
    BackfillPolicy,
    DailyPartitionsDefinition,
    Definitions,
    TimeWindowPartitionMapping,
    asset,
    in_process_executor,
)

# Les scripts d'ingestion importent `from common import ...` : leur dossier doit être dans le path.
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "ingestion"))

import ingest_airports  # noqa: E402
import ingest_bookings  # noqa: E402
import ingest_flights  # noqa: E402
import ingest_passengers  # noqa: E402
from run_month import ENTITIES, init_dim_currency  # noqa: E402

# end_date est exclusive : 30 partitions, du 2025-09-01 au 2025-09-30.
daily = DailyPartitionsDefinition(start_date="2025-09-01", end_date="2025-10-01")

DUCKDB_POOL = "duckdb"


@asset(group_name="init", pool=DUCKDB_POOL, kinds={"duckdb"})
def init_load() -> None:
    """dim_currency + état initial (snapshot init/) des 4 entités : bronze, silver puis gold."""
    init_dim_currency()
    for entity in ENTITIES:
        entity.ingest_bronze(init=True)
    for entity in ENTITIES:
        entity.ingest_silver(init=True)
    for entity in ENTITIES:
        entity.ingest_gold()


def _days(context: AssetExecutionContext) -> list:
    """Jours couverts par le run (un seul en exécution quotidienne, N en backfill), dans l'ordre."""
    return [date.fromisoformat(k) for k in sorted(context.partition_keys)]


def build_entity_assets(name: str, module, gold_extra_deps=()):
    """Les 3 assets d'une entité : bronze et silver journaliers, gold reconstruit en entier."""

    @asset(
        name=f"bronze_{name}",
        partitions_def=daily,
        backfill_policy=BackfillPolicy.single_run(),
        group_name="bronze",
        kinds={"csv"},
        deps=[init_load],
    )
    def bronze(context: AssetExecutionContext) -> None:
        for day in _days(context):
            module.ingest_bronze(day)

    @asset(
        name=f"silver_{name}",
        partitions_def=daily,
        backfill_policy=BackfillPolicy.single_run(),
        group_name="silver",
        pool=DUCKDB_POOL,
        kinds={"duckdb"},
        deps=[
            bronze,
            # Auto-dépendance : le jour J attend que le jour J-1 soit chargé (ordre chronologique).
            AssetDep(f"silver_{name}", partition_mapping=TimeWindowPartitionMapping(start_offset=-1, end_offset=-1)),
        ],
    )
    def silver(context: AssetExecutionContext) -> None:
        for day in _days(context):
            module.ingest_silver(day)

    @asset(
        name=f"gold_{name}",
        group_name="gold",
        pool=DUCKDB_POOL,
        kinds={"duckdb"},
        deps=[silver, *gold_extra_deps],
    )
    def gold() -> None:
        module.ingest_gold()

    return bronze, silver, gold


airports_assets = build_entity_assets("airports", ingest_airports)
flights_assets = build_entity_assets("flights", ingest_flights)
passengers_assets = build_entity_assets("passengers", ingest_passengers)
# Le gold des réservations (fact_booking, dim_date, agrégat) joint les dimensions gold : il doit
# passer après elles (même contrainte que l'ordre de run_month.py).
bookings_assets = build_entity_assets(
    "bookings", ingest_bookings,
    gold_extra_deps=[airports_assets[2], flights_assets[2], passengers_assets[2]],
)

defs = Definitions(
    assets=[init_load, *airports_assets, *flights_assets, *passengers_assets, *bookings_assets],
    # Exécution des étapes d'un run dans un seul processus, l'une après l'autre : plus léger
    # (surtout sous Windows) et cohérent avec l'unique écrivain DuckDB.
    executor=in_process_executor,
)
