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

## Design decisions

**Raw JSON first (ELT).** Each API record is stored untouched in `raw` as JSONB, keyed by OSHA's natural identifiers (`activity_nr`, and `activity_nr` + `citation_id`). Loads are upserts, so re-runs never create duplicates, and a modeling mistake is fixed by re-running dbt, not by re-calling a rate-limited API.

**Keyset pagination.** The loader pages by "activity number greater than the last one loaded" instead of by offset, so it can stop and resume at any point, and it verifies that each page really arrives sorted before writing it. Each page of inspections and its citations is written in one transaction.

**Designed around an undocumented rate limit.** The DOL API returns HTTP 429 after a burst of requests, and I could not find a documented limit. The loader waits with increasing backoff, saves progress, and exits with a distinct code. Airflow treats that code as "skipped," not "failed," so the dbt tasks still run on whatever loaded and the next daily run resumes. Request sizes were found by testing: 100 inspection IDs per citation request works, and 200 is rejected with a 403.

**Citations arrive late, so the pipeline revisits them.** Citations post months after an inspection opens. The daily DAG therefore has a second task that re-fetches citations for inspections opened in the last 270 days. Only a few citations had posted in the first day after the initial load, so the refresh is working but its benefit builds up slowly.

**A maturity rule in the model.** In the loaded data, the share of inspections with citations stays in a steady band through early 2026 and then drops toward zero for the most recent months. That is a lag effect, not safer worksites, so dbt flags inspections opened less than six months before the build as not yet mature, and every rate and score uses only mature inspections.

**Layered models.** `raw` (untouched JSON) → `staging` (typed, deleted citations removed) → `marts` (one row per inspection, a contractor dimension, a risk score, and a monthly aggregate). dbt tests run after every build.

## Data quality findings

1. **Deleted citations carry penalties that were never owed.** Records flagged as deleted often show an initial penalty with a current penalty of zero. In the first load, 375 of 7,073 citations were flagged deleted, so staging excludes them.
2. **Citation lag.** The share of inspections with citations falls from roughly half or more for older months to near zero for the last six months. Dashboard rates use mature months only.
3. **Unexplained dip in autumn 2025.** October and November 2025 contain far fewer inspections (23 and 25) than typical months (about 150 to 250). The cause has not been determined, and the dashboard says so.
4. **Messy establishment names.** Basic normalization (case, punctuation, company suffixes) merged only a handful of names among the first 1,000 inspections, so within this dataset, simple cleaning goes most of the way. Fuzzier matching would matter when joining other sources, which this project does not yet do.

## Privacy

OSHA publishes employer records, and many Texas construction employers are sole proprietors named after a person. The project handles this in layers:

- A **conservative classifier** treats a name as a business only if it carries a business or trade word. Ambiguous names count as individuals. In a spot check of 25 excluded names, 2 were real businesses (an error rate that is deliberate: excluding a business is cheaper than exposing a person). The word list was tightened once after an earlier check found 5 of 25.
- The **risk score** (serious = 1, repeat = 3, willful = 5, other = 0; businesses with at least 3 mature inspections) is computed in the warehouse but is **not published by name**. These weights are my own assumption, not an OSHA formula.
- A dbt test fails the build if a non-business ever enters the ranking.
- The **dashboard database role** can read only the monthly aggregate table, so company names cannot reach the dashboard even by mistake.

## Limitations

- **One source.** Everything comes from OSHA enforcement records. The project does not yet join permits, insurance claims, or any other dataset, so it describes safety enforcement, not project risk.
- **Finding new inspections assumes numbering follows time.** The loader looks for activity numbers above the highest one loaded. That held across the full history load, but I have not verified it as a rule. An inspection recorded late with a lower number would be missed. A periodic reconciliation (count by date range against the API) would catch this.
- **Completeness is unchecked.** I did not compare the loaded counts against OSHA's own search site, and the source data stops at late September 2026. The autumn 2025 dip is unexplained.
- **An establishment is not a contractor.** OSHA records the employer at an inspected worksite. It does not show who is the general contractor and who is a subcontractor, so the project cannot say anything about those relationships.
- **The risk score is a design choice, not a validated measure.** Its weights are my assumption, it uses citations (which can be contested and revised), and I have not tested whether it predicts anything.
- **The business classifier is a keyword heuristic.** It errs toward excluding businesses. The private warehouse still contains establishment names, including some sole proprietors, in intermediate tables. Only the dashboard role is restricted.
- **Single-node setup.** One VM, Airflow's LocalExecutor, and no high availability. Fine for this data volume, not a production design.

## Future work

- **Reconcile against the API.** A monthly check of inspection counts by date range, to test the numbering assumption above.
- **Add a second source.** The aim is project and insurance context: building permit data where a city publishes it, and FEMA flood claims and disaster declarations for Texas. This is where matching company names across datasets would become a real problem. I have not confirmed which Texas cities publish usable permit data.
- **A streaming layer.** Poll National Weather Service alerts for Texas counties, publish new and changed alerts to Kafka or Redpanda, and process them with a stream job. NWS offers a polled API, not a push feed, so this would be poll-to-stream with deduplication. It becomes useful once there is a source of active project locations to compare against.
- **CI.** A GitHub Actions workflow that builds the dbt project against a throwaway Postgres container on every pull request.
- **Alerting.** Notify on repeated load failures or when the load falls behind.
- **Incremental dbt models** as the history grows.

## Verified from a clean clone

I cloned the repository into a fresh folder with a new database and walked the Quick start. This covers what was tested and what was not.

**Tested:** the Postgres init SQL (schemas, raw tables, and the Airflow and Metabase databases) on a brand-new volume, under different user and database names; creating the dashboard role; `dbt run` and `dbt test` from an empty database (6 models, 23 tests); Airflow starting with the DAG visible in the UI; and Metabase connecting with the read-only role and seeing only the aggregate table.

**Not tested:** a first full DAG run from an empty database, which would restart the full history load and run into the API's rate limit; and the dashboard itself, since Metabase stores its saved charts in its own database and they are not part of this repository.

The clean-clone test found two setup problems, both fixed: `airflow/logs` was missing from a fresh clone and Docker created it owned by root, and the `dbt` container wrote its build folders as root.

## Dashboard queries

The three charts query the aggregate table `marts.mart_inspections_monthly`, connected as the read-only `metabase_ro` role.

Inspections by month and trade group (stacked bar):

```sql
SELECT open_month, naics_subsector, sum(inspections) AS inspections
FROM marts.mart_inspections_monthly
GROUP BY 1, 2
ORDER BY 1;
```

Citations per 100 inspections, by trade group (bar), using only months old enough for citations to have posted:

```sql
SELECT naics_subsector,
       round(100.0 * sum(serious_citations) / sum(inspections), 1) AS serious_per_100,
       round(100.0 * sum(repeat_citations)  / sum(inspections), 1) AS repeat_per_100
FROM marts.mart_inspections_monthly
WHERE is_mature_month
GROUP BY 1
ORDER BY 1;
```

Share of inspections with citations, by month (line), mature months only:

```sql
SELECT open_month,
       round(100.0 * sum(inspections_with_citations) / sum(inspections), 1) AS pct_with_citations
FROM marts.mart_inspections_monthly
WHERE is_mature_month
GROUP BY 1
ORDER BY 1;
```

The per-100 figures are the average number of citations per inspection scaled by 100, not a percentage of inspections, so they can exceed 100.
