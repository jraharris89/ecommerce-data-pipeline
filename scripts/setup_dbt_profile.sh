#!/bin/bash
# Ensure dbt profile directory and configuration exists

mkdir -p /home/airflow/.dbt

cat > /home/airflow/.dbt/profiles.yml << 'EOF'
ecommerce_analytics:
  target: dev
  outputs:
    dev:
      type: postgres
      host: postgres
      user: airflow
      password: airflow
      port: 5432
      dbname: ecommerce
      schema: analytics
      threads: 4
      keepalives_idle: 0
EOF

echo "dbt profile configured successfully"
