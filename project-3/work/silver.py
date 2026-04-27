def run_silver():
    from pyspark.sql import SparkSession, functions as F
    from pyspark.sql.window import Window


    print("🚀 Starting Silver CDC job...")

    spark = SparkSession.builder \
        .appName("CDC-Silver") \
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.lakehouse.type", "rest") \
        .config("spark.sql.catalog.lakehouse.uri", "http://iceberg-rest:8181") \
        .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.lakehouse.s3.endpoint", "http://minio:9000") \
        .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true") \
        .config("spark.sql.defaultCatalog", "lakehouse") \
        .getOrCreate()

    print("✅ Spark session created")

    # Ensure namespace
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.cdc")

    # Create Silver tables
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.cdc.silver_customers (
            id INT,
            name STRING,
            email STRING,
            country STRING,
            last_updated_ms BIGINT
        ) USING iceberg
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.cdc.silver_drivers (
            id INT,
            name STRING,
            license_number STRING,
            rating DOUBLE,
            city STRING,
            active BOOLEAN,
            created_at STRING,
            last_updated_ms BIGINT
        ) USING iceberg
    """)

    print("✅ Silver tables ready")

    # Read Bronze
    bronze_df = spark.sql("SELECT * FROM lakehouse.cdc.bronze_cdc")

    print(f"📥 Bronze count: {bronze_df.count()}")

    # Split topics
    customers = bronze_df.filter(F.col("topic") == "dbserver1.public.customers")
    drivers = bronze_df.filter(F.col("topic") == "dbserver1.public.drivers")

    # Extract entity_id
    customers = customers.withColumn("entity_id", F.get_json_object("after", "$.id"))
    drivers = drivers.withColumn("entity_id", F.get_json_object("after", "$.id"))

    # Deduplicate (latest event per key)
    w = Window.partitionBy("entity_id").orderBy(F.col("ts_ms").desc())

    latest_customers = customers \
        .filter(F.col("op").isNotNull()) \
        .withColumn("rn", F.row_number().over(w)) \
        .filter("rn = 1") \
        .drop("rn")

    latest_drivers = drivers \
        .filter(F.col("op").isNotNull()) \
        .withColumn("rn", F.row_number().over(w)) \
        .filter("rn = 1") \
        .drop("rn")

    print("✅ Deduplication complete")

    # -------- CUSTOMERS --------
    customers_clean = latest_customers.select(
        F.col("entity_id").cast("int").alias("id"),
        F.get_json_object("after", "$.name").alias("name"),
        F.get_json_object("after", "$.email").alias("email"),
        F.get_json_object("after", "$.country").alias("country"),
        F.col("ts_ms").alias("last_updated_ms"),
        F.col("op")
    )

    customers_clean.writeTo("lakehouse.cdc.tmp_updates_customers").createOrReplace()

    spark.sql("""
        MERGE INTO lakehouse.cdc.silver_customers t
        USING lakehouse.cdc.tmp_updates_customers s
        ON t.id = s.id

        WHEN MATCHED AND s.op = 'd' THEN DELETE

        WHEN MATCHED AND s.op IN ('c','u','r') THEN UPDATE SET
            t.name = s.name,
            t.email = s.email,
            t.country = s.country,
            t.last_updated_ms = s.last_updated_ms

        WHEN NOT MATCHED AND s.op != 'd' THEN INSERT
            (id, name, email, country, last_updated_ms)
            VALUES (s.id, s.name, s.email, s.country, s.last_updated_ms)
    """)

    print("✅ Customers MERGE complete")

    # -------- DRIVERS --------
    drivers_clean = latest_drivers.select(
        F.col("entity_id").cast("int").alias("id"),
        F.get_json_object("after", "$.name").alias("name"),
        F.get_json_object("after", "$.license_number").alias("license_number"),
        F.expr("try_cast(get_json_object(after, '$.rating') as double)").alias("rating"),
        F.get_json_object("after", "$.city").alias("city"),
        F.expr("try_cast(get_json_object(after, '$.active') as boolean)").alias("active"),
        F.get_json_object("after", "$.created_at").alias("created_at"),
        F.col("ts_ms").alias("last_updated_ms"),
        F.col("op")
    )

    drivers_clean.writeTo("lakehouse.cdc.tmp_updates_drivers").createOrReplace()

    spark.sql("""
        MERGE INTO lakehouse.cdc.silver_drivers t
        USING lakehouse.cdc.tmp_updates_drivers s
        ON t.id = s.id

        WHEN MATCHED AND s.op = 'd' THEN DELETE

        WHEN MATCHED AND s.op IN ('c','u','r') THEN UPDATE SET
            t.name = s.name,
            t.license_number = s.license_number,
            t.rating = s.rating,
            t.city = s.city,
            t.active = s.active,
            t.created_at = s.created_at,
            t.last_updated_ms = s.last_updated_ms

        WHEN NOT MATCHED AND s.op != 'd' THEN INSERT
            (id, name, license_number, rating, city, active, created_at, last_updated_ms)
            VALUES (
                s.id, s.name, s.license_number, s.rating,
                s.city, s.active, s.created_at, s.last_updated_ms
            )
    """)

    print("✅ Drivers MERGE complete")

    # Final check
    print("📊 Silver customers preview:")
    spark.sql("SELECT * FROM lakehouse.cdc.silver_customers ORDER BY id LIMIT 5").show(truncate=False)

    print("📊 Silver drivers preview:")
    spark.sql("SELECT * FROM lakehouse.cdc.silver_drivers ORDER BY id LIMIT 5").show(truncate=False)

    print("🎉 Silver job completed successfully!")


if __name__ == "__main__":
    run_silver()