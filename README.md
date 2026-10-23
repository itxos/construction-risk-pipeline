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

## Quick start

**Requirements:** Docker with Compose, and a free API key from the [DOL Open Data Portal](https://dataportal.dol.gov/registration) (sign in with Login.gov, then create a key under API Keys).

**1. Clone and configure**

```bash
git clone git@github.com:itxos/construction-risk-pipeline.git
cd construction-risk-pipeline
cp .env.example .env
```

Edit `.env` and set your own values:

| Variable | Notes |
|---|---|
| `DOL_API_KEY` | Your DOL API key |
| `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` | Any values you like |
| `AIRFLOW_ADMIN_PASSWORD` | Password for the Airflow `admin` user |
| `AIRFLOW_FERNET_KEY` | Generate with the command below |

```bash
python3 -c "import base64,os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())"
```

**2. Start the stack**

```bash
docker compose build
docker compose up -d
```

On first start, Postgres creates the `raw`, `staging`, and `marts` schemas, the raw tables, and the Airflow and Metabase databases.

**3. Create the dashboard role** (before the first dbt run)

```bash
docker exec -it construction-postgres psql -U <POSTGRES_USER> -d <POSTGRES_DB> -c "
CREATE ROLE metabase_ro LOGIN PASSWORD 'choose_a_password';
GRANT CONNECT ON DATABASE <POSTGRES_DB> TO metabase_ro;
GRANT USAGE ON SCHEMA marts TO metabase_ro;"
```

dbt grants this role read access to the one aggregate table, `mart_inspections_monthly`, and nothing else.

**4. Run the pipeline**

1. Open Airflow at `http://localhost:8081` and log in as `admin`.
2. Un-pause `osha_construction_daily` and trigger it once.
3. The DOL API rate-limits bulk loads. When it does, the load tasks show as **skipped** (pink), not failed, and progress is saved. The next daily run resumes where the last one stopped, so the full history takes several runs.

**5. View the dashboard**

Open Metabase at `http://localhost:3001`, create an admin account, and connect PostgreSQL with host `postgres`, port `5432`, your database name, and the `metabase_ro` credentials from step 3.

| Service | URL |
|---|---|
| Airflow | http://localhost:8081 |
| Metabase | http://localhost:3001 |
| Postgres | localhost:5433 |
