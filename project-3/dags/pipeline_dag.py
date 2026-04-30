from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="pipeline_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

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
        --packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0,org.apache.iceberg:iceberg-aws-bundle:1.10.0 \
        /home/jovyan/project/work/taxi_silver.py
        """
    )

    connector_health >> run_bronze_cdc >> run_silver_cdc

    run_bronze_taxi >> run_silver_taxi