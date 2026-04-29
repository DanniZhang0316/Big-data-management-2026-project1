import os
from pyspark.sql import SparkSession, functions as F


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("CDC-Bronze")
        .config("spark.sql.shuffle.partitions", "4")

        # Iceberg config
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )

        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "rest")
        .config("spark.sql.catalog.lakehouse.uri", "http://iceberg-rest:8181")
        .config(
            "spark.sql.catalog.lakehouse.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        .config("spark.sql.catalog.lakehouse.s3.endpoint", "http://minio:9000")
        .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")

        # Credentials



        .config("spark.sql.defaultCatalog", "lakehouse")
        .getOrCreate()
    )

def run_bronze():
    print("🚀 Starting Bronze CDC job...")

    spark = build_spark()
    print("✅ Spark session created")

    # Create namespace
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.cdc")
    print("✅ Namespace ensured")

    # Read Kafka (BATCH for testing)
    raw = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "dbserver1.public.customers,dbserver1.public.drivers")
        .option("startingOffsets", "earliest")
        .load()
    )

    print(f"📥 Read {raw.count()} Kafka records")

    # Remove tombstones
    raw_filtered = raw.filter(F.col("value").isNotNull())

    value_str = F.col("value").cast("string")

    # Parse Debezium envelope
    bronze_df = raw_filtered.select(
        "topic",
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),

        F.get_json_object(value_str, "$.payload.op").alias("op"),
        F.get_json_object(value_str, "$.payload.ts_ms").cast("long").alias("ts_ms"),
        F.get_json_object(value_str, "$.payload.source.lsn").cast("long").alias("lsn"),
        F.get_json_object(value_str, "$.payload.before").alias("before"),
        F.get_json_object(value_str, "$.payload.after").alias("after")
    )

    print("🧱 Bronze DataFrame schema:")
    bronze_df.printSchema()

    print("🔍 Sample Bronze records:")
    bronze_df.show(5, truncate=False)

    # Create Iceberg table
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.cdc.bronze_cdc (
            topic STRING,
            kafka_partition INT,
            kafka_offset BIGINT,
            kafka_timestamp TIMESTAMP,
            op STRING,
            ts_ms BIGINT,
            lsn BIGINT,
            before STRING,
            after STRING
        )
        USING iceberg
    """)

    print("✅ Iceberg table ready")

    # Append-only write (Bronze = immutable log)
    bronze_df.writeTo("lakehouse.cdc.bronze_cdc").append()

    print("💾 Data written to Bronze table")

    # Debug preview
    spark.sql("""
        SELECT topic, op, ts_ms, kafka_offset
        FROM lakehouse.cdc.bronze_cdc
        ORDER BY kafka_offset DESC
        LIMIT 10
    """).show(truncate=False)

    print("🎉 Bronze job completed successfully")

if __name__ == "__main__":
    run_bronze()