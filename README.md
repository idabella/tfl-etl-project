# 🚇 TFL ETL Pipeline Project
<br>
<div align="center">
   <img src="docs/images/diagrame.png" width="900">
</div>
<br><br>

> A production-grade, cloud-native ETL pipeline ingesting real-time data from the **Transport for London (TFL) Unified API**, processing it through a **Bronze → Silver → Gold** medallion architecture, and delivering analytics to **Power BI** and **Grafana** dashboards.

---

## 📐 Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        TFL Unified API                              │
│    (Arrivals · Line Status · Bikepoints · Stoppoints · Air Quality) │
└───────────────────────────┬─────────────────────────────────────────┘
                            │  REST / WebSocket
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   ingestion/  (Kafka Producers)                     │
│   base_producer · arrivals · line_status · bikepoint · stoppoint   │
│                  accidents · airquality                              │
└───────────────────────────┬─────────────────────────────────────────┘
                            │  Apache Kafka Topics
                    ┌───────┴────────┐
                    │                │
                    ▼                ▼
        ┌──────────────────┐  ┌──────────────────┐
        │  streaming/      │  │  kafka/           │
        │  Spark Streaming │  │  Kafka Connect    │
        │  (real-time)     │  │  S3 Sink          │
        └────────┬─────────┘  └────────┬──────────┘
                 │                     │
                 ▼                     ▼
        ┌──────────────────────────────────────────┐
        │            AWS S3  (Bronze Layer)         │
        │         Raw JSON — partitioned by date    │
        └──────────────────┬───────────────────────┘
                           │  jobs/  (Databricks / Spark)
                           ▼
        ┌──────────────────────────────────────────┐
        │  transformation/silver  (Silver Layer)   │
        │  Cleaned · Typed · Deduplicated Parquet  │
        └──────────────────┬───────────────────────┘
                           │
                           ▼
        ┌──────────────────────────────────────────┐
        │  transformation/gold   (Gold Layer)      │
        │  Star Schema: Dims + Fact tables (Delta) │
        └──────────────────┬───────────────────────┘
                           │
                           ▼
        ┌──────────────────────────────────────────┐
        │       warehouse/  (Amazon Redshift)      │
        │   DDLs · Views · Stored Procedures       │
        └───────────┬──────────────────────────────┘
                    │
          ┌─────────┴──────────┐
          ▼                    ▼
  ┌──────────────┐    ┌────────────────┐
  │  reporting/  │    │  reporting/    │
  │  Power BI    │    │  Grafana       │
  └──────────────┘    └────────────────┘
```

**Orchestration**: Apache Airflow (`orchestration/`)
**Infrastructure**: Terraform on AWS (`infrastructure/`)
**Data Quality**: Great Expectations (`quality/`)
**CI/CD**: GitHub Actions (`.github/workflows/`)

---

## 📁 Project Structure

```
tfl-etl-pipeline-project/
│
├── ingestion/           # Kafka producers — TFL API → Kafka topics
├── streaming/           # Spark Structured Streaming jobs
├── transformation/      # Bronze → Silver → Gold PySpark jobs
│   ├── schemas/         # Avro/Spark schema definitions
│   ├── silver/          # Cleaning & typing transformations
│   ├── gold/            # Dimension & fact table builders
│   └── utils/           # Shared Spark utilities
├── warehouse/           # Redshift DDLs, views, stored procedures, seeds
│   ├── schemas/
│   ├── dimensions/
│   ├── facts/
│   ├── views/
│   ├── stored_procedures/
│   └── seeds/
├── orchestration/       # Apache Airflow DAGs and custom operators
│   ├── dags/
│   └── plugins/
├── quality/             # Great Expectations suites and checkpoints
│   ├── suites/
│   └── checkpoints/
├── reporting/           # Power BI consumer & Grafana dashboards
│   └── dashboards/
├── infrastructure/      # Terraform IaC (AWS)
├── kafka/               # Kafka Connect configs & Grafana datasources
│   └── connectors/
├── jobs/                # Databricks job definitions (JSON)
├── config/              # Environment variable files (.env) & Airflow vars
├── tests/               # Unit and integration tests (pytest)
├── notebooks/           # Jupyter notebooks for exploration & validation
├── data/                # Sample JSON payloads for local testing
├── docs/                # Architecture docs, API reference, runbooks
└── .github/workflows/   # CI/CD pipelines (GitHub Actions)
```

---

## 🚀 Quick Start

### Prerequisites
| Tool | Version |
|------|---------|
| Python | ≥ 3.10 |
| Apache Kafka | ≥ 3.x |
| Apache Spark | ≥ 3.4 |
| Apache Airflow | ≥ 2.7 |
| Terraform | ≥ 1.5 |
| Docker & Docker Compose | latest |

### 1. Clone & Install Dependencies
```bash
git clone https://github.com/your-org/tfl-etl-pipeline-project.git
cd tfl-etl-pipeline-project

# Install Python dependencies
pip install -r requirements.txt

# Or use the shell script
bash install_dependencies.sh
```

### 2. Configure Environment
```bash
# Copy the dev environment template and fill in your values
cp config/dev.env .env

# Key variables to set:
# TFL_API_KEY=<your TFL app key>
# AWS_ACCESS_KEY_ID=...
# AWS_SECRET_ACCESS_KEY=...
# KAFKA_BOOTSTRAP_SERVERS=localhost:9092
# REDSHIFT_HOST=...
```

### 3. Start Local Infrastructure
```bash
docker-compose up -d   # Starts Kafka, Zookeeper, Airflow, Postgres
```

### 4. Run the Ingestion Producers
```bash
python ingestion/arrivals_producer.py
python ingestion/line_status_producer.py
python ingestion/bikepoint_producer.py
```

### 5. Deploy Kafka Connect Sink Connectors
```bash
# Register S3 sink connectors
curl -X POST http://localhost:8083/connectors \
     -H "Content-Type: application/json" \
     -d @kafka/connectors/s3_sink_arrivals.json
```

### 6. Submit Spark Streaming Job
```bash
spark-submit streaming/streaming_arrivals.py
```

### 7. Run Airflow DAGs
Access the Airflow UI at `http://localhost:8080` and trigger:
- `tfl_etl_pipeline` — main pipeline DAG
- `tfl_dimensions_refresh` — refresh dimension tables
- `tfl_data_quality` — run Great Expectations checks

### 8. Provision AWS Infrastructure
```bash
cd infrastructure/
terraform init
terraform plan -var-file=dev.tfvars
terraform apply -var-file=dev.tfvars
```

---

## 🗂️ Data Model (Star Schema)

```
                    ┌──────────────┐
                    │  dim_date    │
                    └──────┬───────┘
                           │
┌───────────┐  ┌───────────▼──────────┐  ┌──────────────┐
│ dim_line  ├──┤   fact_arrivals      ├──┤ dim_station  │
└───────────┘  └──────────┬───────────┘  └──────────────┘
                          │
              ┌───────────┴───────────┐
              │   fact_line_status    │
              └───────────────────────┘
       dim_zone  dim_disruption  dim_time
```

| Table | Description |
|-------|-------------|
| `dim_date` | Calendar dimension (day, week, month, quarter) |
| `dim_time` | Time-of-day dimension (hour, minute, period) |
| `dim_line` | TFL line master (name, mode, color) |
| `dim_station` | Station/stoppoint master (name, zone, lat/lon) |
| `dim_zone` | Fare zone dimension |
| `dim_disruption` | Disruption type & severity |
| `fact_arrivals` | Vehicle arrival events with timing metrics |
| `fact_line_status` | Line status snapshots over time |

---

## 🔬 Data Quality

Great Expectations suites are in `quality/suites/`. Checkpoints run via Airflow (`tfl_data_quality` DAG).

```bash
# Run quality checks manually
python quality/pipeline_health_check.py
```

---

## 🧪 Tests

```bash
# Run all tests
pytest tests/ -v

# Run specific test files
pytest tests/test_producers.py
pytest tests/test_spark_jobs.py
pytest tests/test_kafka_pipeline.py
```

---

## 🔄 CI/CD

| Workflow | File | Trigger |
|----------|------|---------|
| CI — Lint & Test | `.github/workflows/ci.yml` | Every PR |
| CD — Deploy Dev | `.github/workflows/cd_dev.yml` | Merge to `develop` |
| CD — Deploy Prod | `.github/workflows/cd_prod.yml` | Merge to `main` |

---

## 📚 Documentation

| Document | Description |
|----------|-------------|
| [API Reference](docs/api_reference.md) | TFL API endpoints and schemas |
| [Data Dictionary](docs/data_dictionary.md) | Field-level descriptions for all tables |
| [Onboarding Guide](docs/onboarding.md) | New developer setup guide |
| [Runbook](docs/runbook.md) | Operational runbook for incidents |
| [Architecture Diagram](docs/architecture_diagram.drawio) | Draw.io editable diagram |

---

## 🌍 Environments

| Environment | Config | Terraform vars |
|-------------|--------|----------------|
| Development | `config/dev.env` | `infrastructure/dev.tfvars` |
| Staging | `config/staging.env` | `infrastructure/staging.tfvars` |
| Production | `config/prod.env` | `infrastructure/prod.tfvars` |

---

## 🤝 Contributing

1. Fork the repository
2. Create your feature branch: `git checkout -b feature/my-feature`
3. Run tests: `pytest tests/ -v`
4. Commit your changes: `git commit -m 'feat: add my feature'`
5. Push to the branch and open a Pull Request

---

## 📄 License

This project is licensed under the MIT License.

---

*Built with ❤️ using Apache Kafka · Apache Spark · Apache Airflow · AWS · Terraform · Great Expectations*
