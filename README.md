# 🚀 Modern Data Platform | Railway MySQL → Snowflake → dbt → Apache Airflow (Astronomer) → Power BI

## Enterprise-Grade End-to-End Data Engineering Pipeline

This project demonstrates the design and implementation of a modern cloud-based data platform that automates data ingestion, transformation, orchestration, testing, and deployment using production-grade tools and best practices.

The platform extracts incremental sales data from a Railway-hosted MySQL database, loads it into Snowflake using a custom Python ETL process, transforms it using dbt following the Medallion Architecture (Bronze → Silver → Gold), orchestrates the workflow using Apache Airflow on Astronomer, validates data quality using dbt tests, and deploys automatically through GitHub Actions CI/CD.

---

# Business Problem

Organizations often rely on manual ETL jobs and Excel reports, resulting in:

- Delayed reporting
- Full table reloads
- Poor data quality
- Difficult maintenance
- Lack of monitoring
- No deployment automation

This project demonstrates how to modernize that process using cloud-native technologies.

---

# Solution Architecture

```
                   GitHub
                      │
                      ▼
              GitHub Actions
                 (CI/CD)
                      │
                      ▼
            Astronomer Cloud
              Apache Airflow
                      │
                      ▼
      Incremental Python ETL
                      │
                      ▼
        Railway Cloud MySQL
                      │
                      ▼
         Snowflake BRONZE Layer
                      │
                      ▼
        dbt STAGING (Silver)
                      │
                      ▼
          dbt MARTS (Gold)
                      │
                      ▼
          dbt Data Quality Tests
                      │
                      ▼
        Power BI / AI Analytics
```

---

# Technology Stack

| Layer | Technology |
|--------|------------|
| Source Database | Railway MySQL |
| Programming | Python |
| Data Warehouse | Snowflake |
| Transformation | dbt |
| Orchestration | Apache Airflow (Astronomer) |
| CI/CD | GitHub Actions |
| Version Control | Git |
| AI | Streamlit + LLM |

---

# Key Features

## Incremental ETL

Instead of performing full table reloads, the ETL uses watermark-based incremental loading to extract only newly created or modified records.

Benefits:

- Faster execution
- Lower database load
- Reduced cloud compute cost

---

## Medallion Architecture

### Bronze

Raw data loaded directly from Railway MySQL.

Tables

- CUSTOMER
- INVOICE1
- INVOICE2

---

### Silver

dbt staging models

- Data cleaning
- Standardization
- Business rule transformations

---

### Gold

Analytics-ready dimensional model

Dimensions

- DIM_CUSTOMER
- DIM_ITEM

Fact

- FACT_SALES

---

## Data Quality Validation

Automated dbt tests validate

- Null values
- Primary keys
- Relationships
- Uniqueness

ensuring only trusted data reaches reporting.

---

## Apache Airflow Orchestration

Workflow

```
Load Bronze
      ↓
dbt Staging
      ↓
dbt Gold
      ↓
dbt Tests
```

Entire workflow is automated and scheduled through Astronomer.

---

## GitHub Actions CI/CD

Every push to GitHub automatically

- Builds the Airflow image
- Deploys to Astronomer
- Updates DAGs
- Runs the latest pipeline

No manual deployment required.

---

# Engineering Challenges Solved

During development I solved several production-like engineering challenges including

- Incremental loading using watermark strategy
- Packaging Python modules for Airflow
- Configuring dbt profiles inside containers
- Deploying Airflow through Astronomer
- GitHub Actions CI/CD integration
- Snowflake authentication
- Dependency management
- Production debugging
- Airflow DAG troubleshooting

---

# Sample Pipeline Execution

✅ Load Railway MySQL → Snowflake Bronze

✅ dbt Silver Transformation

✅ dbt Gold Transformation

✅ dbt Data Quality Tests

✅ Pipeline Success

Execution Time:

~2 minutes

---

# Business Value

This platform enables organizations to

- Automate data pipelines
- Improve reporting latency
- Reduce manual ETL work
- Increase data quality
- Enable cloud analytics
- Support scalable reporting

---

# Future Enhancements

- Real-time CDC
- Kafka Streaming
- AI Pipeline Failure Assistant
- Pipeline Health Dashboard
- Email & Slack Alerts
- Automatic Data Lineage
- Data Catalog Integration

---

# About Me

I designed and built this project end-to-end as a practical implementation of a production-style modern data platform.

Responsibilities included

- Solution Architecture
- Python ETL Development
- Incremental Loading
- Snowflake Data Warehouse
- dbt Modeling
- Data Quality Testing
- Apache Airflow DAG Development
- Astronomer Deployment
- GitHub Actions CI/CD
- Pipeline Troubleshooting
- Performance Optimization

---

# Skills Demonstrated

- Python
- SQL
- Snowflake
- dbt
- Apache Airflow
- Astronomer
- GitHub Actions
- CI/CD
- Data Warehousing
- ETL
- ELT
- Incremental Loading
- Medallion Architecture
- Data Quality
- Cloud Data Engineering
