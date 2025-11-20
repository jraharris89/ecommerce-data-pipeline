FROM apache/airflow:2.8.0-python3.11

RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    pandas==2.1.4 \
    psycopg2-binary==2.9.9 \
    sqlalchemy==1.4.51 \
    dbt-core==1.7.0 \
    dbt-postgres==1.7.0

# Set up dbt profile during build
USER root
RUN mkdir -p /home/airflow/.dbt && \
    printf "ecommerce_analytics:\n  target: dev\n  outputs:\n    dev:\n      type: postgres\n      host: postgres\n      user: airflow\n      password: airflow\n      port: 5432\n      dbname: ecommerce\n      schema: analytics\n      threads: 4\n      keepalives_idle: 0\n" > /home/airflow/.dbt/profiles.yml && \
    chown -R airflow:root /home/airflow/.dbt
USER airflow