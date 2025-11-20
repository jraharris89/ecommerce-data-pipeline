{% docs __overview__ %}

# E-Commerce Real-Time Analytics - Data Documentation

## Project Overview

This dbt project transforms raw e-commerce order data from a Kafka stream into analytics-ready dimensional models. The data originates from the Olist Brazilian E-Commerce dataset and flows through a real-time pipeline.

---

## Architecture

**Data Flow:**

```
Kafka Producer → Kafka Topic → Python Consumer → PostgreSQL (raw_orders)
                                                        ↓
                                                    dbt Models
                                                        ↓
                                        Analytics Schema (dimensional models)
```

---

## Data Models

### Staging Layer

- **`stg_orders`**: Clean, typed view of raw order events with minimal transformations

### Marts Layer

#### Fact Tables

- **`fact_orders`**: Order-level aggregated metrics
  - Grain: One row per order
  - Metrics: Total price, freight, payment amounts, product count

#### Dimension Tables

- **`dim_customers`**: Customer master data with lifetime metrics
  - Grain: One row per unique customer
  - Includes: Location and total order count

#### Aggregate Tables

- **`agg_daily_revenue`**: Pre-aggregated daily metrics by state
  - Grain: One row per date per state
  - Purpose: Optimized for dashboard queries

---

## Data Quality

All models include comprehensive data quality tests:

- **Uniqueness tests** on primary keys
- **Not null tests** on critical fields
- **Referential integrity tests** between fact and dimension tables

---

## Key Metrics Calculated

- Daily revenue by geographic region
- Average order value
- Customer lifetime order counts
- Product volume metrics
- Shipping cost analysis

---

## Technologies Used

- **dbt Core**: Data transformation and testing
- **PostgreSQL**: Data warehouse
- **Apache Kafka**: Real-time event streaming
- **Apache Airflow**: Workflow orchestration
- **Python**: Data ingestion and processing

---

## Navigation

Use the **Project** tab (left sidebar) to explore models by their logical structure, or use the **Database** tab to browse by schema.

Click the **graph icon** (bottom right) to view the complete data lineage.

---

## Getting Started

To run this project locally:

```bash
# Run all models
dbt run

# Run tests
dbt test

# Build everything (run + test)
dbt build

# Generate and serve documentation
dbt docs generate
dbt docs serve
```

---

## Data Source

**Olist Brazilian E-Commerce Dataset**

- Source: [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
- Contains: 100k+ orders from 2016-2018
- Includes: Order details, customer data, product information, payments, and reviews

---

## Contact

For questions about this data model, please refer to the project README or check individual model documentation.

{% enddocs %}
