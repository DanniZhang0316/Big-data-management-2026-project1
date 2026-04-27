import os
import time
import requests

CONNECT_URL = "http://connect:8083/connectors"

payload = {
    "name": "cdc-connector",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "postgres",
        "database.port": "5432",
        "database.user": os.getenv("PG_USER"),
        "database.password": os.getenv("PG_PASSWORD"),
        "database.dbname": "sourcedb",
        "topic.prefix": "dbserver1",
        "table.include.list": "public.customers,public.drivers",
        "plugin.name": "pgoutput",
        "slot.name": "debezium_slot",
        "publication.name": "dbz_publication",
        "decimal.handling.mode": "double"

    }
}

# Wait a bit to ensure Connect is fully ready
time.sleep(10)

try:
    response = requests.post(CONNECT_URL, json=payload)

    if response.status_code == 201:
        print("✅ Connector created")
    elif response.status_code == 409:
        print("ℹ️ Connector already exists")
    else:
        print(f"⚠️ Unexpected response: {response.status_code}")
        print(response.text)

except Exception as e:
    print(f"❌ Failed to create connector: {e}")