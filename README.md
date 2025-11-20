# E-Commerce Real-Time Data Pipeline

## Project Overview

An end-to-end data engineering project that processes real-time e-commerce order data through a complete ELT pipeline, from streaming ingestion to analytics-ready dashboards.

## Architecture

```
Olist Dataset → Kafka Producer → Kafka Topic → Python Consumer → PostgreSQL
                                                                      ↓
                                                              dbt Transformations
                                                                      ↓
                                                           Analytics Tables
                                                                      ↓
                                                          Streamlit Dashboard
```

## Technologies Used

- **Apache Kafka**: Real-time event streaming
- **Python**: Data ingestion and processing
- **Apache Airflow**: Workflow orchestration
- **dbt**: Data transformation and testing
- **PostgreSQL**: Data warehouse
- **Streamlit**: Real-time analytics dashboard
- **Docker**: Containerization

## Key Features

- **Real-time streaming**: Kafka-based event processing
- **Automated orchestration**: Daily Airflow DAG execution
- **Data quality**: Comprehensive dbt tests (unique, not_null, relationships)
- **Dimensional modeling**: Star schema with fact and dimension tables
- **Live dashboard**: Interactive Streamlit visualizations

## Data Models

### Staging Layer

- `stg_orders`: Clean view of raw order events

### Marts Layer

- `fact_orders`: Order-level aggregated metrics
- `dim_customers`: Customer dimension with lifetime metrics
- `agg_daily_revenue`: Pre-aggregated daily metrics by state

## Setup Instructions

### Prerequisites

- Docker Desktop
- Python 3.9+
- 8GB RAM minimum

### Installation

1. Clone the repository

```bash
git clone [your-repo-url]
cd ecommerce-data-pipeline
```

2. Start infrastructure

```bash
docker-compose up -d
```

3. Install Python dependencies

```bash
python -m venv venv
source venv/bin/activate  # or venv\Scripts\activate on Windows
pip install -r requirements.txt
```

4. Access services

- Airflow UI: http://localhost:8080 (admin/admin)
- Streamlit Dashboard: http://localhost:8501
- PostgreSQL: localhost:5432

### Running the Pipeline

1. Trigger Airflow DAG manually or wait for daily schedule
2. Monitor execution in Airflow UI
3. View results in Streamlit dashboard

## Key Metrics

- Daily revenue by state
- Average order value
- Customer lifetime orders
- Product volume tracking
- Order fulfillment status

## Data Quality

All models include comprehensive tests:

- Primary key uniqueness
- Not null constraints
- Referential integrity checks

## Documentation

Full data catalog available in dbt docs:

```bash
cd dbt/ecommerce_analytics
dbt docs generate
dbt docs serve
```

## What I Learned

- Building scalable event-driven architectures
- Implementing dimensional data modeling (Kimball methodology)
- Orchestrating complex data workflows with Airflow
- Writing maintainable, tested data transformations with dbt
- Creating real-time analytics dashboards with Streamlit
- Containerizing data infrastructure with Docker

## Future Enhancements

- Migrate to cloud (GCP BigQuery / AWS Redshift)
- Add data quality monitoring with Great Expectations
- Implement CI/CD pipeline for dbt models
- Add machine learning models (churn prediction, recommendations)
- Set up monitoring and alerting (Prometheus + Grafana)

## License

This project uses the Olist Brazilian E-Commerce dataset from Kaggle.

## Author

Jon Harris

- Portfolio: https://jonharris.dev
- LinkedIn: https://www.linkedin.com/in/jonra-harris
- GitHub: https://www.github.com/jraharris89
