#!/bin/bash
# Script to initialize dbt project inside Docker container

echo "Initializing dbt project inside Docker container..."

# Run dbt init inside the airflow container
docker-compose exec airflow bash -c "cd /opt/airflow/dbt && dbt init ecommerce_analytics --skip-profile-setup"

echo "dbt project initialized!"
echo "Next steps:"
echo "1. Configure dbt/ecommerce_analytics/profiles.yml"
echo "2. Run 'docker-compose exec airflow bash' to enter container"
echo "3. Run dbt commands inside the container"
