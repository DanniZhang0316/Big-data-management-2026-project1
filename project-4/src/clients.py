import os

import boto3
import psycopg


POSTGRES_DSN = (
    f"postgresql://"
    f"{os.environ['POSTGRES_USER']}:"
    f"{os.environ['POSTGRES_PASSWORD']}@"
    f"{os.environ.get('POSTGRES_HOST', 'postgres')}:5432/"
    f"{os.environ['POSTGRES_DB']}"
)

MINIO_ENDPOINT = os.environ["MINIO_ENDPOINT"]
MINIO_ACCESS_KEY = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY = os.environ["MINIO_SECRET_KEY"]


def get_postgres():
    return psycopg.connect(POSTGRES_DSN)


def get_s3():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )