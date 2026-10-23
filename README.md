# Texas Construction Safety Pipeline

An ELT pipeline that loads federal OSHA inspection and citation records for Texas construction employers from the U.S. Department of Labor Open Data Portal API, models them with dbt, and publishes aggregate safety analytics in Metabase. It runs on a self-hosted Proxmox VM in Docker Compose, orchestrated by Airflow.

![Dashboard](docs/dashboard.png)

The warehouse holds about 5,300 inspections (January 2024 to September 2026) and the citations issued from them.

**Privacy by design:** OSHA publishes employer records, and some employers are sole proprietors named after a person. This project classifies establishments conservatively, so ambiguous names are treated as individuals and kept out of any ranking, and the dashboard database role can read only an aggregate table with no company names.

## Stack

| Layer | Tool |
|---|---|
| Orchestration | Apache Airflow 2.10 (LocalExecutor) |
| Warehouse | PostgreSQL 16 |
| Transformation and testing | dbt-postgres 1.9 |
| BI | Metabase |
| Infrastructure | Docker Compose on a Proxmox VM (Ubuntu 24.04) |

## Data flow

1. Airflow runs the loader daily. It pulls new Texas construction inspections (NAICS 23) and re-fetches citations for recent inspections, since citations post months after an inspection opens.
2. Raw JSON lands untouched in Postgres (`raw` schema), upserted on OSHA's natural keys.
3. dbt parses it into `staging`, builds the `marts`, and runs its tests after every load.
4. Metabase reads one aggregate mart through a read-only role.
