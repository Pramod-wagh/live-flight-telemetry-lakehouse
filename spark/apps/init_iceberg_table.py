"""
PySpark Script for Ingesting Raw Flight Telemetry into Apache Iceberg Table.
Catalog: lakehouse (backed by PostgreSQL JDBC Catalog)
Storage: MinIO (s3a://flight-lakehouse/iceberg/)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, to_date, hour, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, BooleanType, IntegerType
)
import os

def get_spark_session() -> SparkSession:
    """Build SparkSession configured for Iceberg, MinIO S3, and PostgreSQL Catalog."""
    os.environ["AWS_REGION"] = "us-east-1"
    os.environ["AWS_ACCESS_KEY_ID"] = "admin"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "adminpassword123"

    return SparkSession.builder \
        .appName("FlightDataIcebergIngestion") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.lakehouse.type", "jdbc") \
        .config("spark.sql.catalog.lakehouse.uri", "jdbc:postgresql://postgres:5432/iceberg_catalog") \
        .config("spark.sql.catalog.lakehouse.jdbc.user", "postgres") \
        .config("spark.sql.catalog.lakehouse.jdbc.password", "postgres") \
        .config("spark.sql.catalog.lakehouse.warehouse", "s3://flight-lakehouse/") \
        .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.lakehouse.s3.endpoint", "http://minio:9000") \
        .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true") \
        .config("spark.sql.catalog.lakehouse.s3.access-key-id", "admin") \
        .config("spark.sql.catalog.lakehouse.s3.secret-access-key", "adminpassword123") \
        .config("spark.sql.catalog.lakehouse.client.region", "us-east-1") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.iceberg:iceberg-aws-bundle:1.5.2,org.postgresql:postgresql:42.6.0") \
        .getOrCreate()

def init_iceberg_table(spark: SparkSession):
    """Create namespace and Iceberg table partitioned by origin_country and flight_date."""
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.flights")
    
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.flights.live_telemetry (
            icao24 STRING,
            callsign STRING,
            origin_country STRING,
            time_position LONG,
            last_contact LONG,
            longitude DOUBLE,
            latitude DOUBLE,
            baro_altitude DOUBLE,
            on_ground BOOLEAN,
            velocity DOUBLE,
            true_track DOUBLE,
            vertical_rate DOUBLE,
            geo_altitude DOUBLE,
            squawk STRING,
            spi BOOLEAN,
            position_source INT,
            event_timestamp TIMESTAMP,
            flight_date DATE
        )
        USING iceberg
        PARTITIONED BY (origin_country, days(flight_date))
        TBLPROPERTIES (
            'write.format.default'='parquet',
            'write.parquet.compression-codec'='zstd',
            'history.expire.max-snapshot-age-ms'='604800000'
        )
    """)
    print("Iceberg table lakehouse.flights.live_telemetry verified/created.")

if __name__ == "__main__":
    spark = get_spark_session()
    init_iceberg_table(spark)
    spark.stop()
