# YT_ELT — YouTube Channel Analytics Pipeline

An end-to-end **ELT (Extract, Load, Transform)** data pipeline that pulls video
statistics for a YouTube channel from the **YouTube Data API v3**, loads the raw
data into a PostgreSQL warehouse, transforms it into an analytics-ready model,
and validates it with automated data-quality checks — all orchestrated with
**Apache Airflow** and shipped through a **Dockerised CI/CD workflow**.

> Despite the repo name, the flow is **ELT**: raw data is landed first (staging),
> then transformed in-warehouse (core).

---

## What it does

1. **Extract** — For a given channel handle, resolve its "uploads" playlist,
   page through every video ID, and fetch each video's snippet, content details
   and statistics (views, likes, comments, duration). The raw records are written
   to `data/YT_data_<YYYY-MM-DD>.json`.
2. **Load** — The daily JSON file is loaded into the `staging.yt_api` table
   as-is (an idempotent full sync: insert new videos, update changed ones,
   delete videos no longer returned by the API).
3. **Transform** — Staging rows are transformed into `core.yt_api`: the ISO‑8601
   `duration` string is parsed into a `TIME` value and each video is classified
   as `Shorts` (≤ 60 s) or `Normal`.
4. **Data quality** — [Soda Core](https://www.soda.io/) scans both schemas for
   missing/duplicate video IDs and logical anomalies (likes or comments greater
   than view count).

---

## Architecture

```
                     ┌──────────────────────────────────────────────┐
                     │              Apache Airflow                  │
                     │        (CeleryExecutor + Redis)              |
                     └──────────────────────────────────────────────┘

  DAG 1: produce_json            DAG 2: update_db           DAG 3: data_quality
  (daily @ 14:00 Asia/Kolkata)   (triggered)                (triggered)
  ┌───────────────────┐          ┌────────────────┐         ┌──────────────────┐
  │ get_playlist_id   │          │ staging_table  │         │ soda scan        │
  │ get_video_ids     │  JSON    │   → staging.*  │         │   staging.yt_api │
  │ extract_video_data├────────► │ core_table     ├───────► │ soda scan        │
  │ save_to_json      │  file    │   → core.*     │         │   core.yt_api    │
  │ trigger_update_db │          │ trigger_dq     │         └──────────────────┘
  └───────────────────┘          └────────────────┘
          │                              │                          │
          ▼                              ▼                          ▼
   YouTube Data API v3          PostgreSQL  (elt_db)         Soda Core checks

  PostgreSQL container hosts 3 logical databases:
    • airflow metadata db      • celery result backend db      • elt_db (warehouse)
```

DAGs are chained with `TriggerDagRunOperator`, so a single scheduled run of
`produce_json` walks the whole pipeline end to end.

### Data model (`elt_db`)

| Column           | `staging.yt_api` | `core.yt_api`            |
|------------------|------------------|-------------------------|
| `Video_ID`       | `VARCHAR(11)` PK | `VARCHAR(11)` PK        |
| `Video_Title`    | `TEXT`           | `TEXT`                  |
| `Upload_Date`    | `TIMESTAMP`      | `TIMESTAMP`             |
| `Duration`       | `VARCHAR(20)` (ISO‑8601) | `TIME` (parsed) |
| `Video_Type`     | —                | `VARCHAR(10)` (`Shorts` / `Normal`) |
| `Video_Views`    | `INT`            | `INT`                   |
| `Likes_Count`    | `INT`            | `INT`                   |
| `Comments_Count` | `INT`            | `INT`                   |

---

## Tech stack

| Concern            | Tool |
|--------------------|------|
| Orchestration      | Apache Airflow 2.9.2 (Python 3.10), CeleryExecutor |
| Message broker     | Redis 7.2 |
| Storage / warehouse| PostgreSQL 13 |
| Data quality       | Soda Core (`soda-core-postgres`) |
| Source API         | YouTube Data API v3 |
| Packaging          | Docker / Docker Compose |
| Testing            | pytest (unit + integration), `airflow dags test` (E2E) |
| CI/CD              | GitHub Actions → Docker Hub |

---

## Repository layout

```
.
├── dags/
│   ├── main.py                       # DAG definitions & dependencies
│   ├── api/
│   │   └── video_stats.py            # YouTube API extraction tasks
│   ├── datawarehouse/
│   │   ├── dwh.py                    # staging_table / core_table tasks
│   │   ├── data_utils.py             # connections, schema & table DDL
│   │   ├── data_loading.py           # read daily JSON file
│   │   ├── data_modification.py      # insert / update / delete rows
│   │   └── data_transformation.py    # duration parsing, Shorts vs Normal
│   └── dataquality/
│       └── soda.py                   # BashOperator wrapping `soda scan`
├── include/soda/
│   ├── checks.yml                    # data-quality checks
│   └── configuration.yml             # Soda datasource (env-substituted)
├── tests/
│   ├── conftest.py                   # fixtures (mocked + real connections)
│   ├── unit_test.py                  # variables, connections, DAG integrity
│   └── integration_test.py           # live YouTube API + Postgres checks
├── docker/postgres/
│   └── init-multiple-databases.sh    # creates the 3 databases + users
├── data/                             # generated daily JSON extracts
├── Dockerfile                        # custom Airflow image (+ soda, pytest)
├── docker-compose.yaml               # full local stack
├── requirements.txt
└── .github/workflows/ci-cd_yt-elt.yaml
```

---

## Getting started (local)

### Prerequisites

- Docker & Docker Compose
- A [YouTube Data API v3 key](https://developers.google.com/youtube/v3/getting-started)
- At least 4 GB of memory available to Docker

### 1. Configure environment

The stack is fully driven by environment variables (no values are committed).
Create a `.env` file in the project root — it is git-ignored — and re-enable the
`env_file` blocks in `docker-compose.yaml` (they are commented out so CI can
inject variables directly).

```dotenv
# --- YouTube source ---
API_KEY=your_youtube_api_key
CHANNEL_HANDLE=MrBeast                 # channel handle without the leading @

# --- Airflow ---
AIRFLOW_UID=50000
FERNET_KEY=generate_a_fernet_key
AIRFLOW_WWW_USER_USERNAME=airflow
AIRFLOW_WWW_USER_PASSWORD=airflow

# --- PostgreSQL (shared container) ---
POSTGRES_CONN_HOST=postgres
POSTGRES_CONN_PORT=5432
POSTGRES_CONN_USERNAME=postgres
POSTGRES_CONN_PASSWORD=postgres

# --- Airflow metadata db ---
METADATA_DATABASE_NAME=airflow
METADATA_DATABASE_USERNAME=airflow
METADATA_DATABASE_PASSWORD=airflow

# --- Celery result backend db ---
CELERY_BACKEND_NAME=celery_backend
CELERY_BACKEND_USERNAME=celery
CELERY_BACKEND_PASSWORD=celery

# --- ELT warehouse db ---
ELT_DATABASE_NAME=elt_db
ELT_DATABASE_USERNAME=elt_user
ELT_DATABASE_PASSWORD=elt_pass

# --- Docker image (used by docker-compose `image:`) ---
DOCKERHUB_NAMESPACE=your_dockerhub_user
DOCKERHUB_REPOSITORY=yt-elt
```

Generate a Fernet key:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### 2. Build & start

```bash
docker compose build          # build the custom Airflow image
docker compose up -d          # start postgres, redis, airflow-*
```

The Postgres init script creates the three databases and users on first boot.
Airflow is available at **http://localhost:8080** (log in with the
`AIRFLOW_WWW_USER_*` credentials).

### 3. Run the pipeline

In the Airflow UI, unpause and trigger **`produce_json`**. It will chain into
`update_db` and then `data_quality` automatically. On its own schedule it runs
daily at **14:00 Asia/Kolkata**.

Inspect the results:

```bash
docker exec -it postgres psql -U elt_user -d elt_db -c 'SELECT * FROM core.yt_api LIMIT 10;'
```

---

## Testing

```bash
# unit + integration tests
docker exec -t airflow-worker sh -c "pytest tests/ -v"

# end-to-end DAG tests
docker exec -t airflow-worker sh -c "airflow dags test produce_json"
docker exec -t airflow-worker sh -c "airflow dags test update_db"
docker exec -t airflow-worker sh -c "airflow dags test data_quality"
```

- **Unit tests** — Airflow Variables/Connections resolution and DAG integrity
  (expected DAG IDs, task counts, no import errors).
- **Integration tests** — a live call to the YouTube API and a real connection
  to the Postgres warehouse.

---

## CI/CD

`.github/workflows/ci-cd_yt-elt.yaml` runs on pushes to `main` / `feature/*`,
on PRs to `main`, and on manual dispatch:

1. **build-and-push-image** — when `Dockerfile` or `requirements.txt` changed,
   build the image and push it to Docker Hub tagged `latest` and with the commit
   SHA.
2. **unit-and-integration-and-e2e-tests** — when `dags/**`, `include/**` or
   `docker-compose.yaml` changed, spin up the full stack with Docker Compose,
   run pytest, then run `airflow dags test` for every DAG, and tear down.

Secrets/config are provided through GitHub Actions **repository variables**
(`vars.*`) and **secrets** (`secrets.DOCKERHUB_PASSWORD`) — which is why the
`env_file` entries in `docker-compose.yaml` are commented out.

---

## Notes & limitations

- Local development only — the Compose file is not production-hardened.
- `save_to_json` overwrites the file for the current date; `load_data` reads the
  file for *today*, so `update_db` must run the same day as `produce_json`.
- The warehouse sync is a full replace against the latest API response: videos
  removed from the channel are deleted from both schemas.
