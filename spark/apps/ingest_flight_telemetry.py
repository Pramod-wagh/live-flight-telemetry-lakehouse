"""
PySpark Script: Flight Telemetry Processing & Iceberg Ingestion
Reads raw flight JSON from MinIO, parses flight telemetry, appends to Iceberg Lakehouse table,
and updates PostgreSQL serving table for real-time dashboard queries.
"""

import sys
import os
import json
from minio import Minio
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, from_unixtime, to_date, to_timestamp, coalesce
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, BooleanType, IntegerType, Row
)

def get_spark() -> SparkSession:
    """Build SparkSession configured for Iceberg (MinIO + Postgres JDBC Catalog)."""
    os.environ["AWS_REGION"] = "us-east-1"
    os.environ["AWS_ACCESS_KEY_ID"] = "admin"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "adminpassword123"

    return SparkSession.builder \
        .appName("OpenSkyFlightIngestion") \
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

def read_raw_from_minio(raw_path: str):
    """Retrieve raw JSON data from MinIO using Python MinIO client."""
    # Handle s3a://flight-raw/filename.json or plain filename
    clean_path = raw_path.replace("s3a://", "").replace("s3://", "")
    parts = clean_path.split("/", 1)
    if len(parts) == 2:
        bucket, key = parts[0], parts[1]
    else:
        bucket, key = os.getenv("MINIO_RAW_BUCKET", "flight-raw"), parts[0]

    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000").replace("http://", "").replace("https://", "")
    client = Minio(
        minio_endpoint,
        access_key=os.getenv("MINIO_ACCESS_KEY", "admin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "adminpassword123"),
        secure=False
    )
    
    print(f"Fetching raw data from MinIO: bucket={bucket}, object={key}")
    response = client.get_object(bucket, key)
    try:
        data = json.loads(response.read().decode('utf-8'))
    finally:
        response.close()
        response.release_conn()
    return data

def process_batch(raw_s3_path: str):
    raw_data = read_raw_from_minio(raw_s3_path)
    states = raw_data.get("states", [])
    batch_time = raw_data.get("time", 0)

    if not states:
        print("No flight states found in batch. Exiting.")
        return

    print(f"Extracted {len(states)} raw states from batch at time {batch_time}.")

    spark = get_spark()

    # 1. Ensure Iceberg Namespace and Table Exist
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
            'write.parquet.compression-codec'='zstd'
        )
    """)

    # 2. Convert states list to Spark Rows
    rows = []
    for s in states:
        if len(s) >= 17 and s[0] and s[5] is not None and s[6] is not None:
            rows.append(Row(
                icao24=str(s[0]),
                callsign=str(s[1]).strip() if s[1] else "",
                origin_country=str(s[2]) if s[2] else "Unknown",
                time_position=int(s[3]) if s[3] is not None else None,
                last_contact=int(s[4]) if s[4] is not None else None,
                longitude=float(s[5]),
                latitude=float(s[6]),
                baro_altitude=float(s[7]) if s[7] is not None else None,
                on_ground=bool(s[8]),
                velocity=float(s[9]) if s[9] is not None else None,
                true_track=float(s[10]) if s[10] is not None else None,
                vertical_rate=float(s[11]) if s[11] is not None else None,
                geo_altitude=float(s[13]) if s[13] is not None else None,
                squawk=str(s[14]) if s[14] else None,
                spi=bool(s[15]) if s[15] is not None else False,
                position_source=int(s[16]) if s[16] is not None else 0
            ))

    if not rows:
        print("No valid rows after coordinate validation. Exiting.")
        spark.stop()
        return

    raw_df = spark.createDataFrame(rows)
    flights_df = raw_df.withColumn(
        "event_timestamp",
        to_timestamp(from_unixtime(coalesce(col("time_position"), col("last_contact"))))
    ).withColumn(
        "flight_date",
        to_date(from_unixtime(coalesce(col("time_position"), col("last_contact"))))
    )

    print(f"Prepared {flights_df.count()} structured flight records.")

    # 3. Append into Apache Iceberg Table
    print("Writing records to Iceberg table 'lakehouse.flights.live_telemetry'...")
    flights_df.writeTo("lakehouse.flights.live_telemetry").append()
    print("Successfully committed batch to Apache Iceberg!")

    # 4. Sync Latest State to PostgreSQL Serving Layer (flight_serving.live_flights)
    print("Writing latest state snapshot to PostgreSQL 'flight_serving' database...")
    flights_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/flight_serving") \
        .option("dbtable", "live_flights") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()
    print("Successfully updated PostgreSQL serving table!")

    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1].strip():
        s3_path = sys.argv[1]
    else:
        s3_path = "flight-raw/latest.json"
    process_batch(s3_path)
