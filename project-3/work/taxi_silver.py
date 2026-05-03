import os
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("taxi-silver")
        .master("local[2]")
        .config("spark.driver.memory", "3g")
        .config("spark.sql.shuffle.partitions", "50")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.autoBroadcastJoinThreshold", "52428800")
        .config("spark.driver.maxResultSize", "1g")
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

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.taxi")

    # READ FROM bronze_trips
    print("📥 Reading from Bronze...")
    bronze_df = spark.read.table("lakehouse.taxi.bronze_trips")

    # Parse and clean data with proper types
    parsed = bronze_df.select(
        F.col("trip_id"),
        F.col("VendorID").alias("vendor_id"),
        F.to_timestamp("tpep_pickup_datetime").alias("pickup_datetime"),
        F.to_timestamp("tpep_dropoff_datetime").alias("dropoff_datetime"),
        F.col("passenger_count").cast(IntegerType()).alias("passenger_count"),
        F.col("trip_distance"),
        F.col("RatecodeID").cast(IntegerType()).alias("rate_code_id"),
        F.col("store_and_fwd_flag"),
        F.col("PULocationID").alias("pu_location_id"),
        F.col("DOLocationID").alias("do_location_id"),
        F.col("payment_type"),
        F.col("fare_amount"),
        F.col("extra"),
        F.col("mta_tax"),
        F.col("tip_amount"),
        F.col("tolls_amount"),
        F.col("improvement_surcharge"),
        F.col("total_amount"),
        F.col("congestion_surcharge"),
        F.col("Airport_fee").alias("airport_fee"),
        F.col("cbd_congestion_fee"),
    )

    # FILTER INVALID ROWS
    print("🧹 Filtering invalid rows...")
    cleaned = parsed.filter(
        F.col("vendor_id").isNotNull()
        & F.col("pickup_datetime").isNotNull()
        & F.col("dropoff_datetime").isNotNull()
        & F.col("pu_location_id").isNotNull()
        & F.col("do_location_id").isNotNull()
        & F.col("passenger_count").between(1, 6)
        & (F.col("trip_distance") > 0)
        & (F.col("fare_amount") > 0)
        & (F.col("total_amount") > 0)
        & (F.col("dropoff_datetime") > F.col("pickup_datetime"))
    )

    # Deduplicate
    print("🔄 Deduplicating...")
    deduped = cleaned.dropDuplicates(
        [
            "vendor_id",
            "pickup_datetime",
            "pu_location_id",
            "do_location_id",
            "fare_amount",
        ]
    )

    # Enrich with zone names
    print("🌍 Enriching with zone data...")
    base_path = os.environ.get("PROJECT_HOME", "/home/jovyan/project")
    zones_path = os.path.join(base_path, "data", "taxi_zone_lookup.parquet")
    zones = spark.read.parquet(zones_path)

    pu_zones = zones.select(
        F.col("LocationID").alias("_pu_id"),
        F.col("Zone").alias("pickup_zone"),
        F.col("Borough").alias("pickup_borough"),
    )
    do_zones = zones.select(
        F.col("LocationID").alias("_do_id"),
        F.col("Zone").alias("dropoff_zone"),
        F.col("Borough").alias("dropoff_borough"),
    )

    silver_df = (
        deduped.join(F.broadcast(pu_zones), deduped.pu_location_id == F.col("_pu_id"), "left")
        .drop("_pu_id")
        .join(F.broadcast(do_zones), deduped.do_location_id == F.col("_do_id"), "left")
        .drop("_do_id")
    )

    # WRITE TO silver_trips (without counting - saves memory!)
    print("💾 Writing to Silver table...")
    silver_df.writeTo("lakehouse.taxi.silver_trips").createOrReplace()

    print("✅ Taxi Silver transformation complete!")


if __name__ == "__main__":
    run()