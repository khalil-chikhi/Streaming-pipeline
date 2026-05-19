# Wikimedia Edit Stream Analytics Pipeline

A production-grade real-time data pipeline that ingests live Wikipedia edit events, processes them through a modern cloud data stack, and serves analytics via a live dashboard.

## Architecture

```
Wikimedia SSE Stream (live API)
          │
          ▼
  Python Producer (auto-reconnect)
          │
          ▼
  Aiven Kafka (managed cloud, SSL/TLS)
  Topic: wikimedia-recentchange
          │
          ▼
  Databricks Batch Job (every 30min)
  ├── Consumes from Kafka
  ├── Flattens + enriches events
  ├── Deduplicates by event_id
  └── Writes to Delta Lake (Unity Catalog)
          │
          ▼
  dbt Transformations (on Databricks)
  ├── stg_wiki_events (view)
  │     └── Cleans, filters, adds derived fields
  ├── mart_edit_activity_hourly (table)
  │     └── Hourly aggregates: volume, bot%, unique editors
  └── mart_top_editors (table)
        └── Top human editors by edit count
          │
          ▼
  dbt Tests (data quality)
  ├── not_null on event_id
  ├── accepted_values on event_type
  └── accepted_values on editor_type
          │
          ▼
  Prefect Orchestration (every 30min)
  ├── Triggers Databricks ingestion job
  ├── Waits for completion
  ├── Runs dbt run (staging + marts)
  └── Runs dbt test (quality checks)
          │
          ▼
  Databricks AI/BI Dashboard (live)
  ├── Top wikis by edit volume
  ├── Bot vs human edits by wiki
  ├── Bot % by wiki (filtered)
  ├── Edit activity over time
  └── Top human editors leaderboard
```

## Tech Stack

| Layer | Tool | Notes |
|---|---|---|
| Data Source | Wikimedia SSE API | Free, no auth, ~500 events/min |
| Message Bus | Aiven Kafka (free tier) | Managed cloud Kafka, SSL/TLS |
| Stream Processing | Databricks (Free Edition) | PySpark batch, every 30min |
| Storage | Delta Lake (Unity Catalog) | ACID transactions, time travel |
| Transformation | dbt-databricks 1.11 | Staging views + mart tables |
| Quality Checks | dbt tests | not_null, accepted_values |
| Orchestration | Prefect 2 | Flow: Databricks job → dbt run → dbt test |
| Dashboard | Databricks AI/BI | Live queries against Delta Lake |

## Project Structure

```
wikimedia-streaming-pipeline/
├── producer.py          # Wikimedia SSE → Aiven Kafka
├──  spark_streaming.py   # Local Spark (dev/testing)
├── databricks_ingestion.ipynb  # Databricks batch job notebook
├── wikimedia_dbt/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_wiki_events.sql
│   │   │   └── schema.yml
│   │   └── marts/
│   │       ├── mart_edit_activity_hourly.sql
│   │       └── mart_top_editors.sql
│   └── dbt_project.yml
├── pipeline_flow.py          # Prefect orchestration flow
└── docker-compose.yml        # Local Kafka (dev)
```

## Key Engineering Decisions

**Why Aiven Kafka over self-hosted?**
Managed Kafka eliminates broker management, replication config, and Zookeeper overhead. SSL/TLS encryption is handled automatically. Mirrors how production teams use Confluent Cloud or AWS MSK.

**Why near-real-time batch (30min) over micro-batch streaming?**
Most analytical use cases don't require sub-second latency. A 30-minute batch window reduces compute costs, simplifies fault tolerance, and is easier to monitor.

**Why Delta Lake + dbt over a traditional warehouse?**
Delta Lake gives ACID transactions and time travel on object storage. dbt adds version-controlled, tested SQL transformations on top. This is the modern lakehouse pattern replacing traditional ETL pipelines.

**Why dbt views for staging, tables for marts?**
Staging views always reflect the latest raw data without storing duplicates. Mart tables are materialized because dashboards query them repeatedly — materializing avoids re-scanning Delta files on every dashboard load.

**Why Prefect over Airflow?**
Prefect requires zero infrastructure for local orchestration — no separate metadata database or scheduler process. For a single-developer pipeline, Prefect's simplicity is a better trade-off than Airflow's operational overhead.

## Setup

### Prerequisites
- Python 3.10+
- Docker Desktop
- Aiven account 
- Databricks Free Edition account 

### 1. Clone and install

```bash
git clone https://github.com/yourusername/wikimedia-streaming-pipeline
cd wikimedia-streaming-pipeline
pip install -r requirements.txt
```

### 2. Configure Aiven Kafka

1. Create a free Kafka service at [aiven.io/free-kafka](https://aiven.io/free-kafka)
2. Download SSL certificates (ca.pem, service.cert, service.key)
3. Create topic `wikimedia-recentchange` (2 partitions, 2 replication)
4. Update `KAFKA_BOOTSTRAP_SERVERS` in `ingestion/producer.py`

### 3. Configure Databricks

1. Sign up at [databricks.com](https://signup.databricks.com) (Free Edition)
2. Create a notebook from `databricks_ingestion.py`
3. Upload SSL certificates to `/tmp/`
4. Schedule as a job every 30 minutes

### 4. Configure dbt

```bash
cd wikimedia_dbt
dbt debug   # verify Databricks connection
dbt run     # run transformations
dbt test    # run quality checks
```

### 5. Run Prefect orchestration

```bash
python pipeline_flow.py
```

## Data Quality

dbt tests run automatically after every ingestion:

| Test | Column | Rule |
|---|---|---|
| not_null | event_id | Every event must have an ID |
| accepted_values | event_type | Only: edit, new, categorize, log |
| accepted_values | editor_type | Only: bot, human |

## Sample Insights

From a 60,000+ event sample:
- **commonswiki** receives 57% bot edits — highest bot ratio among major wikis
- **enwiki** has the most diverse human editors per hour
- Small wikis (cewiki, plwiki) operate almost entirely via automated bots
- Top human editor **Epìdosis** made 3,450 edits across 68 unique Wikidata pages in one session

## License

MIT