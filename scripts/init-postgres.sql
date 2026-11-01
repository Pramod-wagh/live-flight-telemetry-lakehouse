-- Initialize required databases for modern data stack components
CREATE DATABASE airflow;
CREATE DATABASE iceberg_catalog;
CREATE DATABASE flight_serving;

-- Grant permissions to default postgres user
GRANT ALL PRIVILEGES ON DATABASE airflow TO postgres;
GRANT ALL PRIVILEGES ON DATABASE iceberg_catalog TO postgres;
GRANT ALL PRIVILEGES ON DATABASE flight_serving TO postgres;
