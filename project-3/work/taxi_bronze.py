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

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.taxi")

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS lakehouse.taxi.bronze (
            key STRING,
            value STRING,
            topic STRING,
            partition INT,
            offset BIGINT,
            kafka_timestamp TIMESTAMP
        ) USING iceberg
        """
    )

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
    topic = os.environ.get("KAFKA_TOPIC", "taxi-trips")

    raw = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    bronze_df = raw.select(
        F.col("key").cast("string"),
        F.col("value").cast("string"),
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    )

    bronze_df.writeTo("lakehouse.taxi.bronze").append()

    # Basic validation for quick checks in logs.
    spark.sql("SELECT count(*) AS n FROM lakehouse.taxi.bronze").show()


if __name__ == "__main__":
    run()
