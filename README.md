# Week 01 Practice: Big Data Fundamentals (pandas vs DuckDB vs PySpark, CSV vs Parquet)

The goal of this practice session is to compare a few well-known analytical frameworks and file types.  

What to do: Start Jupyter with Docker Compose, run one notebook, answer a short questionnaire.

This week uses the **payments** dataset:

- `data/payments.csv`
- `data/payments.parquet`

Same data, different storage format.

## Learning goals

Compare the same dataset across:

- File formats: CSV vs Parquet
- Engines / APIs:
  - pandas (Python DataFrame, single-node)
  - DuckDB (single-node analytical SQL)
  - PySpark (Spark DataFrame API, local mode inside the notebook)
- Query shapes:
  - Single-row fetch (point-query shape)
  - Aggregation (group-by over many rows)

Then reflect on:

- which NFR dominates (latency, throughput, scalability, etc.)
- when single-node stops being enough
- what you pay when moving to distributed systems

## Documentation and AI use

You are welcome to use AI chatbots as learning assistants.  

It is, however, strongly encouraged to personally review the actual documentation. 

- Pandas
  - https://pandas.pydata.org/docs/reference/index.html#api
- DuckDB
  - https://duckdb.org/docs/stable/clients/python/overview
- PySpark
  - https://spark.apache.org/docs/latest/api/python/reference/index.html

## Requirements

Must have:

- Docker Desktop installed and running (Windows / Mac / Linux)
- Git (or download the course repo as ZIP)
- A browser

Recommended:

- 8 GB RAM minimum. 16 GB helps if running PySpark locally
- 5+ GB free disk space

## Files expected

These two files must exist (same data, different format):

- `data/payments.csv`
- `data/payments.parquet`

You can find link to the datasets folder in Moodle.  
Note: the full path for `data` folder from project root is `week01/work/data`

## Step 1: Start Jupyter with Docker Compose

In the folder that contains `compose.yml`, run:

    docker compose up -d

Then open:

- http://localhost:8888

Token:

- bdm

Optional:

- Spark UI: http://localhost:4040  
  _Note: this will only work once you have started Spark (cell 7)_

To stop:

    docker compose down

## Step 2: Open and run the notebook

In JupyterLab:

1. Open the `work/` folder
2. Open `week_01_practice.ipynb`
3. Run → Run All Cells

If `week_01_practice.ipynb` is missing, create a new Python notebook with that name and copy-paste the cells below.

If PySpark fails due to RAM, complete the practice using pandas and DuckDB, and answer Spark questions conceptually.

## Notebook cells (copy into a new notebook, in order)

Cell 1 (config):

    # Dataset paths (payments)
    CSV_PATH = "data/payments.csv"
    PARQUET_PATH = "data/payments.parquet"

    # Columns in the payments dataset (default)
    # Examples: step, type, amount, nameOrig, oldbalanceOrg, nameDest, isFraud, ...
    ID_COL = "nameOrig"     # used for "single-row fetch" query shape (not necessarily unique)
    GROUP_COL = "type"      # aggregation grouping
    MEASURE_COL = "amount"  # numeric measure for AVG / SUM

    WARMUPS = 2
    RUNS = 10

Cell 2 (check files):

    from pathlib import Path

    csv_path = Path(CSV_PATH)
    pq_path = Path(PARQUET_PATH)

    if not csv_path.exists() or not pq_path.exists():
        raise FileNotFoundError(
            "Missing dataset files. Expected:\n"
            f"  {csv_path}\n  {pq_path}\n"
            "Ask the instructor or place them into the data/ folder."
        )

    print("OK: dataset files found")

Cell 3 (install DuckDB if missing):

    import importlib.util, sys, subprocess

    if importlib.util.find_spec("duckdb") is None:
        print("Installing duckdb...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "duckdb==1.1.3"])
    else:
        print("duckdb already installed")

Cell 4 (helpers):

    import time, statistics
    import pandas as pd

    def percentiles(samples_ms, ps=(0.50, 0.95, 0.99)):
        xs = sorted(samples_ms)
        out = {}
        for p in ps:
            k = int((len(xs) - 1) * p)
            out[f"p{int(p*100)}"] = xs[k]
        out["avg_ms"] = statistics.mean(xs)
        return out

    def timeit_ms(fn, warmups=WARMUPS, runs=RUNS):
        for _ in range(warmups):
            fn()
        times = []
        for _ in range(runs):
            t0 = time.perf_counter()
            fn()
            t1 = time.perf_counter()
            times.append((t1 - t0) * 1000.0)
        return percentiles(times)

    results = []

Cell 5 (pandas):

    def bench_pandas(fmt, path):
        df = pd.read_csv(path) if fmt == "csv" else pd.read_parquet(path)

        # pick one existing key value for the single-row fetch query shape
        key_value = df[ID_COL].iloc[0]

        def single_row():
            _ = df.loc[df[ID_COL] == key_value].head(1)

        def aggregate():
            _ = df.groupby(GROUP_COL)[MEASURE_COL].mean()

        s1 = timeit_ms(single_row)
        s2 = timeit_ms(aggregate)

        results.append({"engine": "pandas", "format": fmt, "query": "single_row", **s1})
        results.append({"engine": "pandas", "format": fmt, "query": "aggregate", **s2})

    bench_pandas("csv", csv_path)
    bench_pandas("parquet", pq_path)
    print("pandas done")

Cell 6 (DuckDB):

    import duckdb

    def bench_duckdb(fmt, path):
        con = duckdb.connect(database=":memory:")

        if fmt == "csv":
            con.execute(
                f"CREATE TABLE t AS SELECT * FROM read_csv_auto('{path.as_posix()}', header=true)"
            )
        else:
            con.execute(f"CREATE TABLE t AS SELECT * FROM read_parquet('{path.as_posix()}')")

        key_value = con.execute(f"SELECT {ID_COL} FROM t LIMIT 1").fetchone()[0]

        q_single = f"SELECT * FROM t WHERE {ID_COL} = ? LIMIT 1"
        q_agg = f"SELECT {GROUP_COL}, AVG({MEASURE_COL}) AS avg_amount, COUNT(*) AS n FROM t GROUP BY {GROUP_COL}"

        def single_row():
            con.execute(q_single, [key_value]).fetchall()

        def aggregate():
            con.execute(q_agg).fetchall()

        s1 = timeit_ms(single_row)
        s2 = timeit_ms(aggregate)

        results.append({"engine": "duckdb", "format": fmt, "query": "single_row", **s1})
        results.append({"engine": "duckdb", "format": fmt, "query": "aggregate", **s2})

    bench_duckdb("csv", csv_path)
    bench_duckdb("parquet", pq_path)
    print("duckdb done")

Cell 7 (PySpark):

    spark_ok = True
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import avg
    except Exception as e:
        spark_ok = False
        print("PySpark not available:", e)

    if spark_ok:
        spark = (
            SparkSession.builder
            .appName("bdm_week01")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "8")
            .getOrCreate()
        )

        def bench_spark(fmt, path):
            if fmt == "csv":
                df = (
                    spark.read.option("header", "true")
                    .option("inferSchema", "true")
                    .csv(str(path))
                )
            else:
                df = spark.read.parquet(str(path))

            key_value = df.select(ID_COL).limit(1).collect()[0][0]

            def single_row():
                df.filter(df[ID_COL] == key_value).limit(1).collect()

            def aggregate():
                df.groupBy(GROUP_COL).agg(avg(MEASURE_COL).alias("avg_amount")).collect()

            s1 = timeit_ms(single_row)
            s2 = timeit_ms(aggregate)

            results.append({"engine": "spark", "format": fmt, "query": "single_row", **s1})
            results.append({"engine": "spark", "format": fmt, "query": "aggregate", **s2})

        bench_spark("csv", csv_path)
        bench_spark("parquet", pq_path)
        print("spark done")

Cell 8 (results table):

    out = pd.DataFrame(results)
    out = out[["engine", "format", "query", "avg_ms", "p50", "p95", "p99"]].sort_values(
        ["query", "engine", "format"]
    )
    out

## Step 3: Reflect on the results

### What to observe (write down for yourself)

- Which engine was fastest for aggregation on Parquet and by roughly how much.
- How much Spark changes from CSV → Parquet (ratio is enough).
- Whether pandas shows any CSV vs Parquet difference in this notebook, and why.
- How far apart p50 vs p95/p99 are. If they’re close, what does that suggest about stability.

### What to try next (optional)

- Increase dataset size or run on a slower machine and see which engine degrades first.
- Change GROUP_COL (e.g. group by nameDest instead of type) and see how cardinality affects runtime.
- For Spark: remove inferSchema (or define schema) and compare CSV performance.

### Mini-test (next session)

- Be ready to answer:
  - Your DuckDB Parquet aggregate p50 (ms)
  - Your Spark CSV aggregate p50 (ms) 
  - Which engine benefited most from Parquet (based on your results)
  - One sentence: why average latency is not enough for user-facing systems

## Clarification

DuckDB is an analytical engine. We are not building an OLTP database here.  
When we say “OLTP-like”, we only mean the query shape (selective point lookup pattern), not true transactional behavior.

Also note: `nameOrig` is not guaranteed to be unique. This is fine for a “point-query shape” demonstration because we use `LIMIT 1`.
