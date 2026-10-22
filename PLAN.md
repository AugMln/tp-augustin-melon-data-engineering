# Plan d'implémentation — TP ingestion médaillon (compagnie aérienne)

## Contexte

Les squelettes de `ingestion/` sont à compléter : `fetch_csv`, les 4 `ingest_bronze`, et silver/gold pour flights, passengers et bookings. Il faut aussi la table d'agrégation gold, `DOCUMENTATION.md` avec un diagramme, puis les deux bonus SCD2 et Dagster. Mode de travail choisi : j'implémente, l'utilisateur relit à chaque étape.

## Constats de l'exploration (dataset réel)

| Fichier | Clé | Type d'export | Changements | Nature | Stratégie silver |
|---|---|---|---|---|---|
| airports | `airport_id` | snapshot complet | 1 modif + 1 ajout sur le mois | dimension | upsert + désactivation (déjà fourni) |
| flights | `flight_id` | snapshot complet | 0–5 événements/jour (ajout/modif/suppression) | dimension | upsert + désactivation |
| passengers_en/fr | `passenger_id` / `id_passager` (ids disjoints) | snapshot complet | 0–16 ajouts/jour/langue, corrections, suppressions rares | dimension | union normalisée → upsert + désactivation |
| bookings | `booking_id` | **incrémental** (seulement le jour) | aucun (immuable) | fait | **insert** idempotent (`ON CONFLICT DO NOTHING`) |

- Données propres : aucune valeur nulle ni clé en double.
- 117 vols sur 420 arrivent avant leur heure de départ : ce sont des vols de nuit, donc la durée doit ajouter +24 h quand `arrival < departure`.
- Passagers FR : renommer les colonnes, convertir `Homme/Femme` en `Male/Female` et lire les dates au format `DD/MM/YYYY` (`strptime`).
- Une désactivation ne s'applique qu'aux snapshots complets, jamais aux bookings. Pour les passagers, elle se fait sur l'union EN+FR, sinon un fichier désactiverait les passagers de l'autre.

## Étapes

### 1. Bronze — `ingestion/common.py` + 4 × `ingest_bronze`
- `fetch_csv(subdir, filename)` : s'assure que `bronze/<subdir>/` existe. Si le fichier manque, il télécharge les octets bruts (`urllib.request`, copie à l'identique, sans passer par pandas) dans un `.tmp`, puis le renomme : un téléchargement interrompu ne laisse donc jamais de fichier tronqué. Il renvoie ensuite `pd.read_csv(local, dtype=str)`, tout en chaîne de caractères, le typage étant fait en silver.
- `ingest_bronze` appelle `fetch_csv` sur `_snapshot_file(day, init)`. Passengers télécharge 2 fichiers, et `init/bookings.csv` sert de fichier init pour bookings.

### 2. Silver — modèle `ingest_airports.py`
- **`silver_flights`** : colonnes typées (`flight_date DATE`, `departure_time/arrival_time TIME`) + `is_active`, `deleted_date`, `insert_timestamp`, `update_timestamp`. Upsert, puis désactivation des clés absentes du snapshot, comme dans `ingest_airports.ingest_silver`.
  - Amélioration par rapport au modèle airports : `update_timestamp` n'est rafraîchi qu'en cas de **vrai** changement (`DO UPDATE ... WHERE` sur `IS DISTINCT FROM`), ce qui garantit l'idempotence et une vraie traçabilité. À valider en relecture, car airports, lui, rafraîchit à chaque passage.
- **`silver_passengers`** : schéma unique `passenger_id, first_name, last_name, gender, nationality, email, birth_date DATE, signup_date DATE, source_system ('EN'|'FR')` + les colonnes techniques. La normalisation se fait en SQL DuckDB sur les deux DataFrames enregistrés (`UNION ALL`), puis même upsert + désactivation.
- **`silver_bookings`** : colonnes typées (`amount DECIMAL(10,2)`, `booking_date DATE`) + `insert_timestamp/update_timestamp`. `INSERT ... ON CONFLICT (booking_id) DO NOTHING` : un insert pur, qu'on peut rejouer sans doublon.
- **Historique SCD2 (bonus)** : tables `silver_flights_history` et `silver_passengers_history` (`valid_from`, `valid_to`, `is_current`, `row_hash`), mises à jour dans le même `ingest_silver`. Elles sont nécessaires parce que l'upsert écrase l'historique, et le gold ne peut être reconstruit qu'à partir de ce que le silver conserve.
  - Si le hash change : on clôt la version courante (`valid_to = snapshot_date`) et on ouvre une nouvelle version.
  - Si la clé disparaît : on clôt la version.
  - Si la clé est nouvelle : on ouvre une version.
  - Rejouer le même jour ne change rien.

### 3. Gold — schéma en étoile
- `dim_airport` : déjà fourni.
- `dim_flight` (SCD2, à partir de l'historique) : clé substitut `flight_sk`, attributs, `flight_duration_min`, `valid_from/valid_to/is_current`, `is_active`. Durée = `(epoch(arrival) - epoch(departure)) / 60`, avec `+1440` si négatif.
- `dim_passenger` (SCD2) : même principe, `passenger_sk`, plus `age` et `age_group` calculés à la date fixe 2025-09-30, pour le profil courant.
- `dim_date` : générée sur la plage des `booking_date` et `flight_date` (jour de semaine, mois…). Elle sert les analyses par jour de la semaine.
- `dim_currency` : fournie, sans changement.
- `fact_booking` : `booking_id`, FK `passenger_sk`, `flight_sk` (version valide à `booking_date`), `airport_id`, `date_key`, `seat_class`, `booking_channel`, `amount`, `currency`, `amount_eur` (jointure `dim_currency`), `age_at_booking`, `age_group_at_booking`, + colonnes techniques.
- Tranches d'âge, à définir dans une macro ou fonction partagée : `<18`, `18-24`, `25-34`, `35-49`, `50-64`, `65+`.
- `agg_revenue_daily_airline` : construite dans `ingest_bookings.ingest_gold`, après `fact_booking`. Contenu : `booking_date`, `airline`, `nb_bookings`, `revenue_eur` + timestamps. Bookings étant la dernière entité de `run_month.py`, toutes les dimensions sont déjà à jour à ce moment-là.
- Toutes les tables gold sont reconstruites avec `CREATE OR REPLACE`. Gold = fonction de l'état silver, donc idempotent.

### 4. Bonus Dagster — `dagster_pipeline/definitions.py`
- `DailyPartitionsDefinition(start_date="2025-09-01", end_date="2025-10-01")`.
- Un asset par entité et par couche (`bronze_flights`, `silver_flights`, `gold_flights`…), qui appelle les fonctions existantes. Les `deps` reproduisent l'ordre de `run_month.py` : silver_bookings après les silver des dimensions, gold_bookings après gold_flights/gold_passengers.
- Un job `init` non partitionné, pour `dim_currency` et le jour init.
- Point d'attention : le SCD2 et la désactivation supposent que les jours sont traités **dans l'ordre**. Le backfill doit donc être séquentiel (`BackfillPolicy` en un seul run, ou concurrence limitée à 1 via la tag concurrency). À documenter.
- Ajouter `dagster` et `dagster-webserver` à `requirements.txt`, avec un commentaire indiquant que c'est pour le bonus.

### 5. `DOCUMENTATION.md`
Contenu :
- choix fait/dimensions et justification ;
- stratégie par fichier (tableau ci-dessus) ;
- gestion des désactivations et du SCD2 ;
- diagramme Mermaid `erDiagram` du modèle gold ;
- limites :
  - taux de change fixes ;
  - rejeu non ordonné non supporté ;
  - granularité de validité au jour (un snapshot = fin de journée) ;
  - âge calculé à des dates de référence.

## Ordre de livraison (relecture à chaque jalon)
1. ✅ `fetch_csv` + bronze ×4, puis vérification du contenu de `bronze/` (5 fichiers init + 150 journaliers, copie identique à la source, 2e passe sans téléchargement).
2. Silver flights + passengers + bookings (sans SCD2), puis `run_month.py` complet.
3. Gold (dims + fact + agrégat, version simple sans SCD2).
4. SCD2 (historique silver + dims gold versionnées + jointure temporelle dans le fait).
5. Dagster.
6. `DOCUMENTATION.md`, puis commits par jalon.

## Vérification
- `cd ingestion; python run_month.py` depuis un clone propre (après suppression de `bronze/` et `warehouse.duckdb`) doit tourner sans erreur.
- Relancer `run_month.py` une 2e fois : les comptes restent identiques (idempotence).
- Requêtes de contrôle via `duckdb` :
  - `count(*)` de `fact_booking` ≈ 480 + ~39 000, et égal à la somme des lignes des CSV bookings ;
  - 0 `booking_id` en double ;
  - 0 FK orpheline (`flight_sk`, `passenger_sk`, `airport_id`) ;
  - `fact_booking.airport_id = dim_flight.origin_airport_id` ;
  - durées toutes > 0 ;
  - nombre d'aéroports actifs = 81 ;
  - vols ≈ 700 ;
  - au moins un vol désactivé, et des versions SCD2 multiples sur des vols modifiés ;
  - `sum(revenue_eur)` de l'agrégat = `sum(amount_eur)` du fait.
- Quelques requêtes de `sql/analysis/queries_analyse.sql` (CA par tranche d'âge, par jour de semaine…) pour vérifier que le modèle répond aux besoins métier.
- Dagster : `dagster dev -f dagster_pipeline/definitions.py`, job init, puis backfill de septembre, puis mêmes contrôles.
