# Project 3 — CDC + Orchestrated Lakehouse Pipeline

## 1. CDC Correctness

### Merge logic documentation

The Silver layer applies CDC events using a MERGE operation based on the primary key (id):

If op = 'd', the corresponding row is deleted.
If op ∈ ('c','u','r'):
The row is updated if it already exists.
The row is inserted if it does not exist.

Before applying the MERGE, the pipeline deduplicates records using a window function to retain only the latest event per entity (ORDER BY ts_ms DESC).

### Idempotency

- Deduplication ensures only the latest event per key is processed, eliminating duplicate or outdated events.
- MERGE operates deterministically on primary keys, producing the same result for the same input.
- DELETE operations are safe to repeat, as deleting an already deleted row has no effect.
- UPDATE operations overwrite with the same values, resulting in no changes on re-execution.
- INSERT operations only occur when a row does not exist, preventing duplicate records.

Re-running the pipeline produces the same final state without duplications or inconsistencies.

### Silver matches PostgreSQL source (compare row counts; spot-check 3+ rows).

**Row count and spot-check lakehouse.cdc.silver_customers:**
```
spark.sql("SELECT COUNT(*) FROM lakehouse.cdc.silver_customers").show()
+--------+
|count(1)|
+--------+
|      10|
+--------+

spark.sql("SELECT COUNT(*) FROM lakehouse.cdc.silver_drivers").show()
+--------+
|count(1)|
+--------+
|       8|
+--------+
```
```
spark.sql("""SELECT * FROM lakehouse.cdc.silver_customers ORDER BY name ASC LIMIT 3 """).show(truncate=False)

+---+------------+-----------------+-------+---------------+
|id |name        |email            |country|last_updated_ms|
+---+------------+-----------------+-------+---------------+
|1  |Alice Mets  |alice@example.com|Estonia|1777484354013  |
|2  |Bob Virtanen|bob@example.com  |Finland|1777484354018  |
|3  |Carol Ozols |carol@example.com|Latvia |1777484354018  |
+---+------------+-----------------+-------+---------------+
```
Works also after running simulate.py some time and working hard to kill it:
```
spark.sql("SELECT COUNT(*) FROM lakehouse.cdc.silver_customers").show()
+--------+
|count(1)|
+--------+
|     120|
+--------+

spark.sql("SELECT COUNT(*) FROM lakehouse.cdc.silver_drivers").show()
+--------+
|count(1)|
+--------+
|      37|
+--------+

```


**Row count and spot-check PostgreSQL source:**
```
sourcedb=# SELECT COUNT(*) FROM customers;
 count 
-------
    10

sourcedb=# SELECT COUNT(*) FROM drivers;
 count 
-------
     8
```
```
sourcedb=# SELECT * FROM customers LIMIT 3;

 id |     name     |       email       | country |         created_at         
----+--------------+-------------------+---------+----------------------------
  1 | Alice Mets   | alice@example.com | Estonia | 2026-04-29 17:39:13.893333
  2 | Bob Virtanen | bob@example.com   | Finland | 2026-04-29 17:39:13.893333
  3 | Carol Ozols  | carol@example.com | Latvia  | 2026-04-29 17:39:13.893333
```
Works also after running simulate.py
```
sourcedb=# SELECT COUNT(*) FROM customers;
 count 
-------
   120
(1 row)

sourcedb=# SELECT COUNT(*) FROM drivers;
 count 
-------
    37
(1 row)

```


### DELETEs in PostgreSQL are reflected as absent rows in Silver

Delete a row in PostgreSQL: DELETE FROM customers WHERE id = 1; (User Alice Mets)

Silver table: spark.sql("SELECT * FROM lakehouse.cdc.silver_customers WHERE id = 1").show()
```
+---+----+-----+-------+---------------+
| id|name|email|country|last_updated_ms|
+---+----+-----+-------+---------------+
+---+----+-----+-------+---------------+
```
And previous query (SELECT * FROM lakehouse.cdc.silver_customers ORDER BY name ASC LIMIT 3). Alice Mets is no more there.
```
+---+--------------+-----------------+---------+---------------+
|id |name          |email            |country  |last_updated_ms|
+---+--------------+-----------------+---------+---------------+
|2  |Bob Virtanen  |bob@example.com  |Finland  |1777484354018  |
|3  |Carol Ozols   |carol@example.com|Latvia   |1777484354018  |
|4  |David Jonaitis|david@example.com|Lithuania|1777484354019  |
+---+--------------+-----------------+---------+---------------+
```

### Idempotency: running the DAG twice with no new changes leaves Silver unchanged (show row counts).

After running CDC bronze and silver layer 5 times and quering more than 1 time existing ID-s:
```
spark.sql("""SELECT id, COUNT(*) FROM lakehouse.cdc.silver_customers GROUP BY id HAVING COUNT(*) > 1""").show()
```

```
+---+--------+
| id|count(1)|
+---+--------+
+---+--------+
```

## 2. Lakehouse Design

### Schema of each table: Bronze CDC, Silver CDC, Bronze taxi, Silver taxi, Gold — and why each differs from the previous layer.

#### Bronze CDC, Silver CDC
Tabels differ, because bronze layer has all raw rows Debezium. Silver layer has table for customers and drivers and the data is cleaned. Silver layer stores latest state per entity. 

```
lakehouse.cdc.bronze_cdc
root
 |-- topic: string (nullable = true)
 |-- kafka_partition: integer (nullable = true)
 |-- kafka_offset: long (nullable = true)
 |-- kafka_timestamp: timestamp (nullable = true)
 |-- op: string (nullable = true)
 |-- ts_ms: long (nullable = true)
 |-- lsn: long (nullable = true)
 |-- before: string (nullable = true)
 |-- after: string (nullable = true)
```

```
lakehouse.cdc.silver_customers
 |-- id: integer (nullable = true)
 |-- name: string (nullable = true)
 |-- email: string (nullable = true)
 |-- country: string (nullable = true)
 |-- last_updated_ms: long (nullable = true)
```

```
lakehouse.cdc.silver_drivers
root
 |-- id: integer (nullable = true)
 |-- name: string (nullable = true)
 |-- license_number: string (nullable = true)
 |-- rating: double (nullable = true)
 |-- city: string (nullable = true)
 |-- active: boolean (nullable = true)
 |-- created_at: string (nullable = true)
 |-- last_updated_ms: long (nullable = true)
```

#### Iceberg snapshot history for Silver CDC (query showing multiple MERGE snapshots).
```
spark.sql("SELECT * FROM lakehouse.cdc.silver_customers.history").show()
+--------------------+-------------------+-------------------+-------------------+
|     made_current_at|        snapshot_id|          parent_id|is_current_ancestor|
+--------------------+-------------------+-------------------+-------------------+
|2026-05-01 08:48:...|6007516179617277881|               NULL|               true|
|2026-05-01 11:22:...|7939841747190356258|6007516179617277881|               true|
|2026-05-01 11:29:...|1475400092160512281|7939841747190356258|               true|
+--------------------+-------------------+-------------------+-------------------+
```

#### Time-travel: Silver CDC at a snapshot before a first MERGE.

Getting current snapshot ID's:
```
spark.sql("SELECT snapshot_id, made_current_at FROM lakehouse.cdc.silver_customers.history").show()
+-------------------+--------------------+
|        snapshot_id|     made_current_at|
+-------------------+--------------------+
|6007516179617277881|2026-05-01 08:48:...|
|7939841747190356258|2026-05-01 11:22:...|
|1475400092160512281|2026-05-01 11:29:...|
+-------------------+--------------------+
```
Quering snapshot whit ID 6007516179617277881 (first snapshot before starting simulate.py).
```
spark.sql("SELECT * FROM lakehouse.cdc.silver_customers VERSION AS OF 7939841747190356258").show()
+---+--------------+------------------+-----------+---------------+
| id|          name|             email|    country|last_updated_ms|
+---+--------------+------------------+-----------+---------------+
|  6|  Frank Muller| frank@example.com|    Germany|  1777624988039|
|  9| Ingrid Larsen|ingrid@example.com|     Norway|  1777624988040|
| 10| Javier Garcia|javier@example.com|      Spain|  1777624988040|
|  2|  Bob Virtanen|   bob@example.com|    Finland|  1777624988036|
|  4|David Jonaitis| david@example.com|  Lithuania|  1777624988038|
|  5|  Eva Svensson|   eva@example.com|     Sweden|  1777624988038|
|  1|    Alice Mets| alice@example.com|    Estonia|  1777624988021|
|  3|   Carol Ozols| carol@example.com|     Latvia|  1777624988037|
|  7|     Grace Kim| grace@example.com|South Korea|  1777624988039|
|  8|   Hiro Tanaka|  hiro@example.com|      Japan|  1777624988039|
+---+--------------+------------------+-----------+---------------+
```


## 3. Orchestration Design

## 4. Taxi Pipeline

## 5. Custom Scenario

This pipeline processes raw CDC events from lakehouse.cdc.bronze_cdc and builds customer activity and churn tables.

It first filters the raw CDC stream to include only customer topic events. For each event, the pipeline extracts entity_id from either the after or before JSON payload, depending on the operation type.

Field-level changes are detected using a window function ordered by ts_ms. Each row’s after JSON is compared with the previous row for the same customer, allowing the pipeline to flag changes to individual fields such as:

- email
- country

The aggregation layer groups records by entity_id and computes lifecycle-level customer metrics.

The first_seen_ts value is derived from the real database created_at timestamp, stored in microseconds, rather than from the Debezium ingestion timestamp.

Customer status is determined by joining against silver_customers. However, deletion history takes priority: if a customer was ever deleted, they are marked as deleted even if they still appear in silver_customers.

The pipeline produces two gold-layer tables: gold_customer_activity and gold_customer_churn

