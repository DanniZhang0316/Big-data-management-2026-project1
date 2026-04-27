from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

with DAG(
    dag_id="cdc_pipeline",
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
