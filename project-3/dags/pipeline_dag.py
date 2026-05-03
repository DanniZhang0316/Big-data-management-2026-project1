from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default arguments with retry configuration
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id="pipeline_dag",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['cdc', 'taxi', 'iceberg', 'project3'],
) as dag:

    # HEALTH CHECK - Ensure Debezium connector is running
    connector_health = HttpSensor(
        task_id="connector_health",
        http_conn_id="connect_api",
        endpoint="connectors/cdc-connector/status",
        response_check=lambda r: (
            r.status_code == 200 and
            r.json().get("connector", {}).get("state") == "RUNNING"
        ),
        poke_interval=10,
        timeout=300
    )

    # CDC PIPELINE (Driver data from PostgreSQL)
    run_bronze_cdc = BashOperator(
        task_id="run_bronze_cdc",
        bash_command="""
        docker exec jupyter spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0,org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0,org.apache.iceberg:iceberg-aws-bundle:1.10.0 \
        /home/jovyan/project/work/bronze.py
        """
    )

    run_silver_cdc = BashOperator(
        task_id="run_silver_cdc",
        bash_command="""
        docker exec jupyter spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0,org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0,org.apache.iceberg:iceberg-aws-bundle:1.10.0 \
        /home/jovyan/project/work/silver.py
        """
    )

    run_bronze_taxi = BashOperator(
        task_id="run_bronze_taxi",
        bash_command="""
        docker exec jupyter spark-submit \
        --packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0,org.apache.iceberg:iceberg-aws-bundle:1.10.0 \
        /home/jovyan/project/work/taxi_bronze.py
        """
    )

    run_silver_taxi = BashOperator(
        task_id="run_silver_taxi",
        bash_command="""
        docker exec jupyter spark-submit \
        --driver-memory 3g \
        --conf spark.sql.shuffle.partitions=50 \
        --conf spark.sql.autoBroadcastJoinThreshold=52428800 \
        --conf spark.driver.maxResultSize=1g \
        --packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0,org.apache.iceberg:iceberg-aws-bundle:1.10.0 \
        /home/jovyan/project/work/taxi_silver.py
        """
    )

    run_gold_taxi = BashOperator(
        task_id="run_gold_taxi",
        bash_command="""
        docker exec jupyter spark-submit \
        --driver-memory 2g \
        --conf spark.sql.shuffle.partitions=50 \
        --conf spark.driver.maxResultSize=1g \
        --packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0,org.apache.iceberg:iceberg-aws-bundle:1.10.0 \
        /home/jovyan/project/work/taxi_gold.py
        """
    )
    
    # CDC Pipeline: health check → bronze → silver
    connector_health >> run_bronze_cdc >> run_silver_cdc

    # Taxi Pipeline: bronze → silver → gold
    run_bronze_taxi >> run_silver_taxi >> run_gold_taxi