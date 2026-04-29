import os
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window


def build_spark() -> SparkSession:
    return (
        SparkSession.builder \
        .appName("CDC-Gold-CustomerActivity") \
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.lakehouse.type", "rest") \
        .config("spark.sql.catalog.lakehouse.uri", "http://iceberg-rest:8181") \
        .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.lakehouse.s3.endpoint", "http://minio:9000") \
        .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true") \
        .config("spark.sql.defaultCatalog", "lakehouse") \
        .getOrCreate()
    )


def run() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.cdc")

    # Read Bronze and Silver source tables
    bronze_df = spark.sql("SELECT * FROM lakehouse.cdc.bronze_cdc")
    silver_customers = spark.sql("SELECT id FROM lakehouse.cdc.silver_customers")

    customers_bronze = (
        bronze_df
        .filter(F.col("topic") == "dbserver1.public.customers")
        .withColumn(
            "entity_id",
            F.coalesce(
                F.get_json_object("after", "$.id"),
                F.get_json_object("before", "$.id"),
            ).cast("int"),
        )
        .filter(F.col("entity_id").isNotNull())
        .withColumn("event_ts", (F.col("ts_ms") / 1000).cast("timestamp"))
    )

    # Detect email and country changes
    w_asc = Window.partitionBy("entity_id").orderBy(F.col("ts_ms").asc())

    customers_with_prev = (
        customers_bronze
        .withColumn("prev_after", F.lag("after").over(w_asc))
        .withColumn("prev_email", F.get_json_object("prev_after", "$.email"))
        .withColumn("prev_country", F.get_json_object("prev_after", "$.country"))
        .withColumn("cur_email", F.get_json_object("after", "$.email"))
        .withColumn("cur_country", F.get_json_object("after", "$.country"))
        .withColumn(
            "email_changed",
            F.when(
                F.col("prev_after").isNotNull()
                & F.col("cur_email").isNotNull()
                & F.col("prev_email").isNotNull()
                & (F.col("cur_email") != F.col("prev_email")),
                1,
            ).otherwise(0),
        )
        .withColumn(
            "country_changed",
            F.when(
                F.col("prev_after").isNotNull()
                & F.col("cur_country").isNotNull()
                & F.col("prev_country").isNotNull()
                & (F.col("cur_country") != F.col("prev_country")),
                1,
            ).otherwise(0),
        )
    )

    # Build gold_customer_activity DataFrame
    agg = customers_with_prev.groupBy("entity_id").agg(
        F.min(
            (F.get_json_object(F.col("after"), "$.created_at") / 1_000_000).cast("timestamp")
        ).alias("first_seen_ts"),
        F.count("*").alias("total_events"),
        F.sum("email_changed").alias("email_changes"),
        F.sum("country_changed").alias("country_changes"),
        F.max("event_ts").alias("last_change_ts"),
        F.max(F.when(F.col("op") == "d", 1).otherwise(0)).alias("ever_deleted"),
        F.max(F.when(F.col("op") == "d", F.col("event_ts"))).alias("deleted_at_ts"),
    )

    silver_ids = silver_customers.select(F.col("id").alias("silver_id"))

    gold_df = (
        agg
        .join(silver_ids, agg["entity_id"] == silver_ids["silver_id"], "left")
        .withColumn(
            "current_status",
            F.when(F.col("ever_deleted") == 1, "deleted")
            .when(F.col("silver_id").isNotNull(), "active")
            .otherwise("deleted"),
        )
        .withColumn(
            "days_since_last_change",
            F.datediff(
                F.current_timestamp().cast("date"),
                F.col("last_change_ts").cast("date"),
            ),
        )
        .withColumn(
            "fields_changed_most_often",
            F.when(F.col("email_changes") > F.col("country_changes"), "email")
            .when(F.col("country_changes") > F.col("email_changes"), "country")
            .when(
                (F.col("email_changes") == F.col("country_changes"))
                & (F.col("email_changes") > 0),
                "email,country",
            )
            .otherwise("none"),
        )
        .select(
            F.col("entity_id").alias("customer_id"),
            F.col("first_seen_ts"),
            F.col("total_events"),
            F.col("email_changes"),
            F.col("country_changes"),
            F.col("fields_changed_most_often"),
            F.col("current_status"),
            F.col("days_since_last_change"),
            F.col("last_change_ts"),
            F.col("ever_deleted").cast("boolean").alias("ever_deleted"),
            F.col("deleted_at_ts"),
        )
    )

    # Write gold_customer_activity Iceberg table
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.cdc.gold_customer_activity (
            customer_id                   INT,
            first_seen_ts                 TIMESTAMP,
            total_events                  BIGINT,
            email_changes                 BIGINT,
            country_changes               BIGINT,
            fields_changed_most_often     STRING,
            current_status                STRING,
            days_since_last_change        INT,
            last_change_ts                TIMESTAMP,
            ever_deleted                  BOOLEAN,
            deleted_at_ts                 TIMESTAMP
        ) USING iceberg
    """)

    gold_df.writeTo("lakehouse.cdc.gold_customer_activity").createOrReplace()

    # Iceberg REST catalog does not support views;
    # gold_customer_churn is materialized as a table instead.
    spark.sql("""
        CREATE OR REPLACE TABLE lakehouse.cdc.gold_customer_churn AS
        SELECT
            customer_id,
            first_seen_ts,
            deleted_at_ts,
            last_change_ts,
            total_events,
            days_since_last_change,
            CASE
                WHEN current_status = 'deleted'
                     AND deleted_at_ts >= (current_timestamp() - INTERVAL 24 HOURS)
                     OR (current_status = 'active'AND days_since_last_change >= 7)
                THEN 'deleted_last_24h'
                WHEN days_since_last_change >= 7
                THEN 'stale_7_days'
            END AS churn_flag
        FROM lakehouse.cdc.gold_customer_activity
        WHERE
            (current_status = 'deleted'
             AND deleted_at_ts >= (current_timestamp() - INTERVAL 24 HOURS))
            OR days_since_last_change >= 7
    """)

    # Basic validation for quick checks in logs.
    spark.sql("SELECT count(*) AS n FROM lakehouse.cdc.gold_customer_activity").show()
    spark.sql("SELECT count(*) AS n FROM lakehouse.cdc.gold_customer_churn").show()


if __name__ == "__main__":
    run()