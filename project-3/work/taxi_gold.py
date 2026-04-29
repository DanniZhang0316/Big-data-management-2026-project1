import os
from pyspark.sql import SparkSession, functions as F


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("taxi-gold")
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

    spark.sql(
        """
        CREATE OR REPLACE TABLE lakehouse.taxi.gold_hourly_trips (
            pickup_zone STRING,
            hour TIMESTAMP,
            trip_count BIGINT,
            avg_fare DOUBLE,
            avg_distance DOUBLE
        ) USING iceberg
        PARTITIONED BY (days(hour))
        """
    )

    gold_df = (
        spark.table("lakehouse.taxi.silver")
        .withColumn("hour", F.date_trunc("hour", "pickup_datetime"))
        .groupBy("pickup_zone", "hour")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
            F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
        )
    )

    gold_df.writeTo("lakehouse.taxi.gold_hourly_trips").createOrReplace()

    # Basic validation for quick checks in logs.
    spark.sql("SELECT count(*) AS n FROM lakehouse.taxi.gold_hourly_trips").show()


if __name__ == "__main__":
    run()
