import os
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("taxi-silver")
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

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.taxi")

    trip_schema = StructType(
        [
            StructField("VendorID", IntegerType()),
            StructField("tpep_pickup_datetime", StringType()),
            StructField("tpep_dropoff_datetime", StringType()),
            StructField("passenger_count", DoubleType()),
            StructField("trip_distance", DoubleType()),
            StructField("RatecodeID", DoubleType()),
            StructField("store_and_fwd_flag", StringType()),
            StructField("PULocationID", IntegerType()),
            StructField("DOLocationID", IntegerType()),
            StructField("payment_type", IntegerType()),
            StructField("fare_amount", DoubleType()),
            StructField("extra", DoubleType()),
            StructField("mta_tax", DoubleType()),
            StructField("tip_amount", DoubleType()),
            StructField("tolls_amount", DoubleType()),
            StructField("improvement_surcharge", DoubleType()),
            StructField("total_amount", DoubleType()),
            StructField("congestion_surcharge", DoubleType()),
            StructField("Airport_fee", DoubleType()),
            StructField("cbd_congestion_fee", DoubleType()),
        ]
    )

    bronze_df = spark.read.table("lakehouse.taxi.bronze")

    parsed = (
        bronze_df.select(
            F.from_json(F.col("value"), trip_schema).alias("t"),
            F.col("kafka_timestamp"),
        )
        .select(
            F.col("t.VendorID").alias("vendor_id"),
            F.to_timestamp("t.tpep_pickup_datetime").alias("pickup_datetime"),
            F.to_timestamp("t.tpep_dropoff_datetime").alias("dropoff_datetime"),
            F.col("t.passenger_count").cast(IntegerType()).alias("passenger_count"),
            F.col("t.trip_distance").alias("trip_distance"),
            F.col("t.RatecodeID").cast(IntegerType()).alias("rate_code_id"),
            F.col("t.store_and_fwd_flag").alias("store_and_fwd_flag"),
            F.col("t.PULocationID").alias("pu_location_id"),
            F.col("t.DOLocationID").alias("do_location_id"),
            F.col("t.payment_type").alias("payment_type"),
            F.col("t.fare_amount").alias("fare_amount"),
            F.col("t.extra").alias("extra"),
            F.col("t.mta_tax").alias("mta_tax"),
            F.col("t.tip_amount").alias("tip_amount"),
            F.col("t.tolls_amount").alias("tolls_amount"),
            F.col("t.improvement_surcharge").alias("improvement_surcharge"),
            F.col("t.total_amount").alias("total_amount"),
            F.col("t.congestion_surcharge").alias("congestion_surcharge"),
            F.col("t.Airport_fee").alias("airport_fee"),
            F.col("t.cbd_congestion_fee").alias("cbd_congestion_fee"),
            F.col("kafka_timestamp"),
        )
    )

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

    deduped = cleaned.dropDuplicates(
        [
            "vendor_id",
            "pickup_datetime",
            "pu_location_id",
            "do_location_id",
            "fare_amount",
        ]
    )

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
        deduped.join(pu_zones, deduped.pu_location_id == F.col("_pu_id"), "left")
        .drop("_pu_id")
        .join(do_zones, deduped.do_location_id == F.col("_do_id"), "left")
        .drop("_do_id")
    )

    silver_df.writeTo("lakehouse.taxi.silver").createOrReplace()

    # Basic validation for quick checks in logs.
    spark.sql("SELECT count(*) AS n FROM lakehouse.taxi.silver").show()


if __name__ == "__main__":
    run()
