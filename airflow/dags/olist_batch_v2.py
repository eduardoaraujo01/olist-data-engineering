from __future__ import annotations

import os
from datetime import datetime

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG
from docker.types import Mount


IMAGE = "olist-python-jobs:local"
NETWORK = os.environ.get("OLIST_DOCKER_NETWORK", "olist-local_pipeline")
SOURCE_DIR = os.environ.get(
    "OLIST_SOURCE_DIR",
    "C:/Users/Duh/Documents/olist-data-engeneering/data/source",
)

MINIO_ENV = {
    "MINIO_ENDPOINT_URL": "http://minio:9000",
    "MINIO_BUCKET": os.environ.get("MINIO_BUCKET", "olist-data-lake"),
    "MINIO_ROOT_USER": os.environ["MINIO_ROOT_USER"],
    "MINIO_ROOT_PASSWORD": os.environ["MINIO_ROOT_PASSWORD"],
}

WAREHOUSE_ENV = {
    **MINIO_ENV,
    "POSTGRES_HOST": "postgres",
    "POSTGRES_PORT": "5432",
    "POSTGRES_DB": os.environ.get("POSTGRES_DB", "olist_analytics"),
    "POSTGRES_USER": os.environ.get("POSTGRES_USER", "olist"),
    "POSTGRES_PASSWORD": os.environ["POSTGRES_PASSWORD"],
}

COMMON_DOCKER = {
    "image": IMAGE,
    "docker_url": "unix://var/run/docker.sock",
    "network_mode": NETWORK,
    "mount_tmp_dir": False,
    "auto_remove": "force",
    "do_xcom_push": False,
}


with DAG(
    dag_id="olist_batch_v2",
    description="Orquestra o lote V2 da ingestão à carga no analytics_v2.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["olist", "batch", "v2"],
) as dag:
    ingestion = DockerOperator(
        task_id="ingestion",
        environment=MINIO_ENV,
        command=["python", "ingestion/ingest_raw.py"],
        mounts=[
            Mount(
                target="/workspace/data/source",
                source=SOURCE_DIR,
                type="bind",
                read_only=True,
            )
        ],
        xcom_all=False,
        **(COMMON_DOCKER | {"do_xcom_push": True}),
    )

    processing = DockerOperator(
        task_id="processing",
        command=["python", "processing/build_processed.py"],
        environment={
            **MINIO_ENV,
            "BATCH_ID": "{{ ti.xcom_pull(task_ids='ingestion') }}",
        },
        **COMMON_DOCKER,
    )

    curated = DockerOperator(
        task_id="curated",
        command=["python", "processing/build_curated.py"],
        environment={
            **MINIO_ENV,
            "BATCH_ID": "{{ ti.xcom_pull(task_ids='ingestion') }}",
        },
        **COMMON_DOCKER,
    )

    warehouse_load = DockerOperator(
        task_id="warehouse_load",
        command=["python", "warehouse/load_postgres_v2.py"],
        environment={
            **WAREHOUSE_ENV,
            "BATCH_ID": "{{ ti.xcom_pull(task_ids='ingestion') }}",
        },
        **COMMON_DOCKER,
    )

    ingestion >> processing >> curated >> warehouse_load