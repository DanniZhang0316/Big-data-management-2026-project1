import os
from pyspark.sql import SparkSession, functions as F


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("taxi-bronze")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "rest")
        .config("spark.sql.catalog.lakehouse.uri", "http://iceberg-rest:8181")
        .config("spark.sql.catalog.lakehouse.warehouse", "s3://warehouse/")
        .config(
            "spark.sql.catalog.lakehouse.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        .config("spark.sql.catalog.lakehouse.s3.endpoint", "http://minio:9000")
        .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
        .config(
            "spark.sql.catalog.lakehouse.s3.access-key-id",
            os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
        )
        .config(
            "spark.sql.catalog.lakehouse.s3.secret-access-key",
            os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        )
        .config("spark.sql.catalog.lakehouse.s3.region", "us-east-1")
        .config("spark.sql.defaultCatalog", "lakehouse")
        .getOrCreate()
    )


def run() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    # Ensure namespace exists
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.taxi")
    print("Namespace created")

    # ═══════════════════════════════════════════════════════════
    # READ FROM PARQUET FILES (not Kafka!)
    # ═══════════════════════════════════════════════════════════
    base_path = os.environ.get("PROJECT_HOME", "/home/jovyan/project")
    parquet_path = os.path.join(base_path, "data", "yellow_tripdata_2025-01.parquet")
    
    print(f"Reading parquet from: {parquet_path}")
    
    raw_df = spark.read.parquet(parquet_path)
    print(f"Read {raw_df.count()} rows from parquet")

    # ═══════════════════════════════════════════════════════════
    # ADD trip_id COLUMN (required for Task 3 joins)
    # ═══════════════════════════════════════════════════════════
    bronze_df = raw_df.withColumn("trip_id", F.monotonically_increasing_id())

    # ═══════════════════════════════════════════════════════════
    # CREATE BRONZE TABLE (note the name: bronze_trips, not bronze)
    # ═══════════════════════════════════════════════════════════
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.taxi.bronze_trips (
            VendorID INT,
            tpep_pickup_datetime STRING,
            tpep_dropoff_datetime STRING,
            passenger_count DOUBLE,
            trip_distance DOUBLE,
            RatecodeID DOUBLE,
            store_and_fwd_flag STRING,
            PULocationID INT,
            DOLocationID INT,
            payment_type INT,
            fare_amount DOUBLE,
            extra DOUBLE,
            mta_tax DOUBLE,
            tip_amount DOUBLE,
            tolls_amount DOUBLE,
            improvement_surcharge DOUBLE,
            total_amount DOUBLE,
            congestion_surcharge DOUBLE,
            Airport_fee DOUBLE,
            cbd_congestion_fee DOUBLE,
            trip_id BIGINT
        ) USING iceberg
    """)
    print("Bronze table schema created")

    # Write to Bronze (append-only)
    bronze_df.writeTo("lakehouse.taxi.bronze_trips").append()
    print("Data written to Bronze")

    # Validation
    count = spark.sql("SELECT count(*) AS n FROM lakehouse.taxi.bronze_trips").collect()[0][0]
    print(f"Bronze table now has {count} rows")


if __name__ == "__main__":
    run()