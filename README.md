# ByteFlow: End-to-End Modern Data Engineering Platform

[![CI/CD Pipeline](https://github.com/mauchoquenachoque/Data-Enginnering/actions/workflows/deploy.yml/badge.svg)]([https://github.com/your-username/byteflow-data-platform](https://github.com/mauchoquenachoque/Data-Enginnering)/actions/workflows/deploy.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![dbt](https://img.shields.io/badge/dbt-Core-orange.svg)](https://www.getdbt.com/)
[![Airflow](https://img.shields.io/badge/Orchestration-Apache%20Airflow-017CEE.svg)](https://airflow.apache.org/)
[![Kafka](https://img.shields.io/badge/Streaming-Apache%20Kafka-231F20.svg)](https://kafka.apache.org/)

ByteFlow is an automated, production-grade Modern Data Engineering platform designed for high-throughput event streaming, automated orchestration, resilient ELT pipelines, and dimensional modeling. Built entirely with open standards and public cloud/SaaS providers, free of cloud lock-in.

---

## 🏗️ Architecture Overview

```
 [Event Producer] (Python Stream Generator)
        │
        ▼ (Streaming Events)
 [Upstash / Confluent Kafka] (Serverless Message Broker)
        │
        ▼
 [Kafka Ingestion Service] ──► [Staging Lakehouse Storage / MotherDuck & Supabase]
                                             │
                                             ▼
 [Apache Airflow] ───────────────► [dbt Core Transformation Engine]
 (Automated Orchestration)                   │
                                             ├── Staging Models (Clean & Cast)
                                             ├── Intermediate (Sessionization & Windowing)
                                             └── Marts (Star Schema: Dims & Facts)
                                                     │
                                                     ▼
                                        [Analytical Data Products / BI]
```

### Core Technologies
- **Message Streaming**: Apache Kafka (Upstash Serverless Kafka / Confluent Cloud)
- **Workflow Orchestration**: Apache Airflow 2.8+
- **Transformation & Modeling**: dbt-core 1.8+ (Dimensional Modeling, Star Schema, Data Quality Tests)
- **Analytical Data Warehouse**: MotherDuck (Serverless DuckDB Cloud) / PostgreSQL (Supabase)
- **CI/CD & Automated Deployment**: GitHub Actions & Docker / Render / Fly.io

---

## 🚀 Quickstart & Local Reproduction

### 1. Prerequisites
- Docker Engine 24+ and Docker Compose v2+
- Python 3.11+
- Git

### 2. Clone and Setup Environment
```bash
git clone https://github.com/your-username/byteflow-data-platform.git
cd byteflow-data-platform
cp .env.example .env
```

Edit `.env` to configure your Upstash Kafka or local broker credentials and MotherDuck/Postgres tokens.

### 3. Spin Up Local Airflow & Kafka Stack
```bash
docker compose up -d
```
Access the services:
- **Airflow Web UI**: [http://localhost:8080](http://localhost:8080) (Default user: `admin` / `admin`)
- **Producer Monitor**: Streaming logs visible via `docker compose logs -f producer`

### 4. Run dbt Transformations Manually
```bash
cd dbt_project
pip install -r ../requirements.txt
dbt deps
dbt seed
dbt run
dbt test
```

---

## 🔄 Automated Deployment (CI/CD)

The project includes complete GitHub Actions automation located in `.github/workflows/deploy.yml`:
1. **Linting & Code Quality**: Flake8, Black, and SQLFluff.
2. **Integration Testing**: Spin up isolated test environment, run producer/consumer test suite.
3. **dbt Verification**: Runs `dbt build --target test` to validate syntax, schema assertions, and tests.
4. **Deploy Phase**: Automatically builds Docker images, pushes to GitHub Packages (GHCR), and triggers automated rollout to public cloud PaaS (Render / Fly.io / GCP Cloud Run).

To enable automated CD:
Set the following secrets in your GitHub repository:
- `KAFKA_BOOTSTRAP_SERVERS`
- `KAFKA_SASL_USERNAME`
- `KAFKA_SASL_PASSWORD`
- `MOTHERDUCK_TOKEN`
- `RENDER_DEPLOY_HOOK_URL` (or cloud deploy token)

---

## 📊 Data Models & Schema Design

- **`stg_ecommerce__orders`**: Bronze level. Cleans raw JSON payloads, normalizes timestamps, handles null handling and deduplication.
- **`stg_ecommerce__customers`**: Standardized customer profiles.
- **`fct_daily_revenue`**: Gold fact table partitioned by date with aggregated metrics.
- **`dim_customers`**: Type 1 dimension containing customer lifetime value and segmentation.

---

## 📜 License
MIT License. Open source and ready for enterprise customization.
