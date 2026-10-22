# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Contexte

TP scolaire de data engineering (Polytech) : pipeline d'ingestion en architecture médaillon (bronze / silver / gold) pour une compagnie aérienne fictive, avec DuckDB embarqué. Le sujet complet (consignes, barème, livrables) est dans `README.md` — s'y référer avant toute décision de modélisation. Code, commentaires et docs sont en français.

Livrable attendu en plus du code : un `DOCUMENTATION.md` (choix fait/dimensions, stratégie de mise à jour/suppression, limites connues, diagramme du data model).

## Commandes (Windows / PowerShell)

```powershell
python -m venv venv
.\venv\Scripts\activate
pip install -r requirements.txt

# Les scripts importent `from common import ...` : les lancer depuis ingestion/
cd ingestion
python run_month.py          # pipeline complet : dim_currency + init + 30 jours de septembre 2025
python ingest_airports.py    # init d'une seule entité (chaque script a un __main__ -> init())

# Repartir de zéro
Remove-Item ..\warehouse.duckdb, ..\bronze -Recurse -Force
```

Pas de tests ni de linter. Pour inspecter l'entrepôt : `python -c "import duckdb; print(duckdb.connect('warehouse.duckdb').sql('SELECT ...'))"` depuis la racine. `sql/analysis/queries_analyse.sql` liste des idées de requêtes de validation (non attendues dans le rendu).

## Architecture

- **Source** : CSV distants sur `https://raw.githubusercontent.com/kevinl75/tp-polytech-dataset/main` (`RAW_BASE` dans `common.py`). `init/` = état avant septembre (fichiers sans date, ex. `airports.csv`), `2025-09/` = un export par jour (`<entité>_<YYYY-MM-DD>.csv`). Les passagers viennent de **deux systèmes aux formats différents** (`passengers_en_*` / `passengers_fr_*`) à unifier en un seul schéma silver.
- **`ingestion/common.py`** : `get_connection()` (fichier `warehouse.duckdb` à la racine) et `fetch_csv(subdir, filename)` — télécharge vers `bronze/<subdir>/<filename>` si absent, puis relit depuis le disque (à implémenter).
- **Un script par entité** (`ingest_airports|flights|passengers|bookings.py`), chacun exposant `ingest_bronze(day=None, init=False)`, `ingest_silver(day=None, init=False)`, `ingest_gold()`. Contraintes :
  - chaque fonction ne lit que ce que la couche précédente a **persisté** (fichier bronze, puis table silver) — jamais d'objet Python passé entre fonctions (pour être orchestrable par Dagster) ;
  - bronze et silver sont paramétrés par date et **idempotents** ; `ingest_gold` reconstruit depuis l'état courant du silver (`CREATE OR REPLACE TABLE`) ;
  - la logique SQL vit dans ces fonctions via `con.execute(...)` (pas de dossier `sql/silver` ou `sql/gold`).
- **`ingest_airports.py` est le modèle de référence** (silver/gold complets) : upsert `INSERT ... ON CONFLICT DO UPDATE`, puis désactivation des clés absentes du snapshot (`is_active = false`, `deleted_date` figée) — jamais de suppression physique. La date du snapshot init est `2025-08-31`.
- **Colonnes techniques obligatoires sur toutes les tables silver et gold** : `insert_timestamp` (jamais modifiée après insertion, donc absente du `SET` du conflit) et `update_timestamp` (rafraîchie à chaque upsert/désactivation).
- **`run_month.py`** (fourni) : exécute `sql/init/dim_currency.sql`, puis pour l'init et chaque jour : bronze des 4 entités → silver des 4 → gold des 4 (ordre des `ENTITIES` : airports, flights, passengers, bookings). Le gold des bookings dépend du silver/gold des vols du même jour (`airport_id` d'une réservation = `origin_airport_id` du vol).

## Exigences du modèle gold

- Schéma en étoile (faits / dimensions) — déterminer insert vs upsert pour chaque entité en observant les données (snapshot complet vs incrémental, évènement immuable vs référentiel évolutif).
- Dimension vol : durée de vol calculée en minutes ; dimension passager : tranche d'âge calculée ; fait réservation : `amount_eur = amount * dim_currency.rate_to_eur` (`dim_currency` : EUR/USD/GBP, taux fixes, fournie).
- Une table d'agrégation (ex. CA par jour et par compagnie) — son emplacement dans le pipeline est à choisir.
- Bonus : SCD2 sur `flights`/`passengers` ; orchestration Dagster (assets partitionnés par jour sur septembre 2025, remplaçant `run_month.py`).

`bronze/` et `warehouse.duckdb` sont ignorés par git : tout doit être régénérable depuis un clone propre.

## Plan d'implémentation et décisions prises

Le plan validé avec l'utilisateur est dans `PLAN.md` (jalons, modèle gold cible, contrôles de vérification) — le suivre et le tenir à jour. Mode de travail : Claude implémente jalon par jalon, l'utilisateur relit à chaque étape (commits par jalon).

Faits observés sur le dataset (cf. aussi son `README.md`) qui conditionnent le code :
- airports / flights / passengers = **snapshots complets** (diff J vs J-1 → upsert + désactivation) ; bookings = **incrémental**, immuable → insert idempotent (`ON CONFLICT DO NOTHING`).
- Ids passagers EN/FR disjoints : désactivation à faire sur l'**union** EN+FR, jamais fichier par fichier. FR : colonnes renommées, `Homme/Femme`, dates `DD/MM/YYYY`.
- ~28 % des vols arrivent « avant » leur départ (vols de nuit) : durée = +1440 min si négative.
- Données propres (pas de nulls ni doublons) ; lire les CSV en `dtype=str` et typer en silver.

Décisions : agrégat `agg_revenue_daily_airline` construit dans `ingest_bookings.ingest_gold` ; âge calculé à la date de réservation dans le fait (et au 2025-09-30 dans `dim_passenger`) ; bonus SCD2 (tables `silver_*_history`, dims gold versionnées) et Dagster visés tous les deux — le SCD2 suppose un rejeu des jours **dans l'ordre**.
