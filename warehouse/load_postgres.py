from __future__ import annotations

import json
import os
import sys
import tempfile
from time import perf_counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import boto3
import pandas as pd
import psycopg
from botocore.client import Config
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]
BUCKET_DEFAULT = "olist-data-lake"
PARQUET_TABLES = {
    "dim_customer": {
        "object": "curated/dim_customer.parquet",
        "columns": [
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
        ],
        "key": ["customer_id"],
    },
    "dim_date": {
        "object": "curated/dim_date.parquet",
        "columns": [
            "date_key",
            "full_date",
            "calendar_year",
            "calendar_quarter",
            "calendar_month",
            "day_of_month",
            "iso_day_of_week",
            "is_weekend",
        ],
        "key": ["date_key"],
    },
    "dim_order_status": {
        "object": "curated/dim_order_status.parquet",
        "columns": ["order_status"],
        "key": ["order_status"],
    },
    "fact_orders": {
        "object": "curated/fact_orders.parquet",
        "columns": [
            "order_id",
            "customer_id",
            "order_status",
            "purchase_date_key",
            "approved_date_key",
            "carrier_date_key",
            "delivered_date_key",
            "estimated_delivery_date_key",
            "order_purchase_at",
            "order_approved_at",
            "order_carrier_at",
            "order_delivered_at",
            "order_estimated_delivery_at",
            "delivery_days",
            "delivered_on_time",
        ],
        "key": ["order_id"],
    },
}

DIM_CUSTOMER_INSERT = """
INSERT INTO analytics.dim_customer (
    customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state
) VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (customer_id) DO UPDATE SET
    customer_unique_id = EXCLUDED.customer_unique_id,
    customer_zip_code_prefix = EXCLUDED.customer_zip_code_prefix,
    customer_city = EXCLUDED.customer_city,
    customer_state = EXCLUDED.customer_state
"""

DIM_DATE_INSERT = """
INSERT INTO analytics.dim_date (
    date_key, full_date, calendar_year, calendar_quarter, calendar_month,
    day_of_month, iso_day_of_week, is_weekend
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (date_key) DO UPDATE SET
    full_date = EXCLUDED.full_date,
    calendar_year = EXCLUDED.calendar_year,
    calendar_quarter = EXCLUDED.calendar_quarter,
    calendar_month = EXCLUDED.calendar_month,
    day_of_month = EXCLUDED.day_of_month,
    iso_day_of_week = EXCLUDED.iso_day_of_week,
    is_weekend = EXCLUDED.is_weekend
"""

DIM_STATUS_INSERT = """
INSERT INTO analytics.dim_order_status (order_status)
VALUES (%s)
ON CONFLICT (order_status) DO NOTHING
"""

FACT_ORDERS_INSERT = """
INSERT INTO analytics.fact_orders (
    order_id, customer_key, order_status_key,
    purchase_date_key, approved_date_key, carrier_date_key,
    delivered_date_key, estimated_delivery_date_key,
    order_purchase_at, order_approved_at, order_carrier_at,
    order_delivered_at, order_estimated_delivery_at,
    delivery_days, delivered_on_time
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
)
ON CONFLICT (order_id) DO UPDATE SET
    customer_key = EXCLUDED.customer_key,
    order_status_key = EXCLUDED.order_status_key,
    purchase_date_key = EXCLUDED.purchase_date_key,
    approved_date_key = EXCLUDED.approved_date_key,
    carrier_date_key = EXCLUDED.carrier_date_key,
    delivered_date_key = EXCLUDED.delivered_date_key,
    estimated_delivery_date_key = EXCLUDED.estimated_delivery_date_key,
    order_purchase_at = EXCLUDED.order_purchase_at,
    order_approved_at = EXCLUDED.order_approved_at,
    order_carrier_at = EXCLUDED.order_carrier_at,
    order_delivered_at = EXCLUDED.order_delivered_at,
    order_estimated_delivery_at = EXCLUDED.order_estimated_delivery_at,
    delivery_days = EXCLUDED.delivery_days,
    delivered_on_time = EXCLUDED.delivered_on_time
"""


def make_s3_client():
    endpoint = os.getenv("MINIO_ENDPOINT_URL", "http://localhost:9000")
    access_key = os.getenv("MINIO_ROOT_USER")
    secret_key = os.getenv("MINIO_ROOT_PASSWORD")
    if not access_key or not secret_key:
        raise ValueError(
            "Configure MINIO_ROOT_USER e MINIO_ROOT_PASSWORD no ambiente do serviço."
        )

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}, signature_version="s3v4"),
    )


def read_curated_table(client, bucket: str, name: str, temporary_dir: Path) -> pd.DataFrame:
    config = PARQUET_TABLES[name]
    local_path = temporary_dir / f"{name}.parquet"
    client.download_file(bucket, config["object"], str(local_path))
    frame = pd.read_parquet(local_path, engine="pyarrow")
    missing = sorted(set(config["columns"]) - set(frame.columns))
    if missing:
        raise ValueError(f"{config['object']} não contém as colunas: {missing}")
    return frame.loc[:, config["columns"]].copy()


def validate_unique_key(frame: pd.DataFrame, table_name: str, columns: list[str]) -> None:
    missing_mask = frame[columns].isna().any(axis=1)
    duplicate_mask = frame.loc[~missing_mask].duplicated(subset=columns, keep=False)
    duplicate_count = int(duplicate_mask.sum())
    missing_count = int(missing_mask.sum())
    if missing_count or duplicate_count:
        examples = frame.loc[duplicate_mask, columns].head(5).to_dict(orient="records")
        raise ValueError(
            f"Chave inválida em {table_name}: {missing_count} linha(s) com chave ausente, "
            f"{duplicate_count} linha(s) duplicada(s); exemplos={examples}"
        )


def validate_date_dimension(dim_date: pd.DataFrame) -> None:
    validate_unique_key(dim_date, "dim_date", ["date_key"])
    validate_unique_key(dim_date, "dim_date", ["full_date"])
    dates = pd.to_datetime(dim_date["full_date"], errors="coerce")
    expected_keys = dates.dt.strftime("%Y%m%d").astype("string")
    actual_keys = dim_date["date_key"].astype("string").str.zfill(8)
    mismatch = dates.isna() | expected_keys.ne(actual_keys)
    if mismatch.any():
        examples = dim_date.loc[mismatch, ["date_key", "full_date"]].head(5).to_dict(orient="records")
        raise ValueError(f"date_key não corresponde a full_date em dim_date: {examples}")


def validate_curated_frames(frames: dict[str, pd.DataFrame]) -> None:
    for table_name, config in PARQUET_TABLES.items():
        validate_unique_key(frames[table_name], table_name, config["key"])

    validate_date_dimension(frames["dim_date"])
    customers = set(frames["dim_customer"]["customer_id"].dropna().astype(str))
    statuses = set(frames["dim_order_status"]["order_status"].dropna().astype(str))
    date_keys = set(frames["dim_date"]["date_key"].dropna().astype(int))
    fact = frames["fact_orders"]

    orphan_customers = fact["customer_id"].notna() & ~fact["customer_id"].astype("string").isin(customers)
    orphan_statuses = fact["order_status"].notna() & ~fact["order_status"].astype("string").isin(statuses)
    if orphan_customers.any():
        examples = fact.loc[orphan_customers, "customer_id"].drop_duplicates().head(5).astype(str).tolist()
        raise ValueError(f"fact_orders referencia customer_id ausente em dim_customer: {examples}")
    if orphan_statuses.any():
        examples = fact.loc[orphan_statuses, "order_status"].drop_duplicates().head(5).astype(str).tolist()
        raise ValueError(f"fact_orders referencia order_status ausente em dim_order_status: {examples}")

    for column in [
        "purchase_date_key",
        "approved_date_key",
        "carrier_date_key",
        "delivered_date_key",
        "estimated_delivery_date_key",
    ]:
        values = fact[column].dropna().astype(int)
        missing_dates = ~values.isin(date_keys)
        if missing_dates.any():
            raise ValueError(
                f"fact_orders.{column} tem {int(missing_dates.sum())} chave(s) sem dim_date."
            )


def db_value(value: Any) -> Any:
    if value is None or value is pd.NA:
        return None
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    if hasattr(value, "item") and not isinstance(value, (str, bytes)):
        return value.item()
    return value


def frame_rows(frame: pd.DataFrame):
    for row in frame.itertuples(index=False, name=None):
        yield tuple(db_value(value) for value in row)


def ensure_schema_exists(cursor) -> None:
    cursor.execute(
        """
        SELECT
            to_regclass('analytics.dim_customer'),
            to_regclass('analytics.dim_date'),
            to_regclass('analytics.dim_order_status'),
            to_regclass('analytics.fact_orders')
        """
    )
    relations = cursor.fetchone()
    if any(relation is None for relation in relations):
        raise RuntimeError(
            "O schema analytics está incompleto. Aplique primeiro "
            "sql/curated/01_create_orders_model.sql."
        )


def dimension_key_maps(cursor) -> tuple[dict[str, int], dict[str, int]]:
    cursor.execute("SELECT customer_id, customer_key FROM analytics.dim_customer")
    customer_map = {customer_id: customer_key for customer_id, customer_key in cursor.fetchall()}
    cursor.execute("SELECT order_status, order_status_key FROM analytics.dim_order_status")
    status_map = {status: status_key for status, status_key in cursor.fetchall()}
    return customer_map, status_map


def build_fact_rows(
    fact: pd.DataFrame,
    customer_map: dict[str, int],
    status_map: dict[str, int],
) -> list[tuple[Any, ...]]:
    column_index = {column: index for index, column in enumerate(fact.columns)}
    rows = []
    for row in fact.itertuples(index=False, name=None):
        customer_id = db_value(row[column_index["customer_id"]])
        order_status = db_value(row[column_index["order_status"]])
        customer_key = customer_map.get(customer_id) if customer_id is not None else None
        status_key = status_map.get(order_status) if order_status is not None else None
        if customer_id is not None and customer_key is None:
            raise ValueError(f"Não foi encontrada chave substituta para customer_id={customer_id}")
        if order_status is not None and status_key is None:
            raise ValueError(f"Não foi encontrada chave substituta para order_status={order_status}")

        rows.append((
            db_value(row[column_index["order_id"]]),
            customer_key,
            status_key,
            db_value(row[column_index["purchase_date_key"]]),
            db_value(row[column_index["approved_date_key"]]),
            db_value(row[column_index["carrier_date_key"]]),
            db_value(row[column_index["delivered_date_key"]]),
            db_value(row[column_index["estimated_delivery_date_key"]]),
            db_value(row[column_index["order_purchase_at"]]),
            db_value(row[column_index["order_approved_at"]]),
            db_value(row[column_index["order_carrier_at"]]),
            db_value(row[column_index["order_delivered_at"]]),
            db_value(row[column_index["order_estimated_delivery_at"]]),
            db_value(row[column_index["delivery_days"]]),
            db_value(row[column_index["delivered_on_time"]]),
        ))
    return rows


def database_counts(cursor) -> dict[str, int]:
    counts = {}
    for table_name in PARQUET_TABLES:
        cursor.execute(f"SELECT count(*) FROM analytics.{table_name}")
        counts[table_name] = int(cursor.fetchone()[0])
    return counts


def validate_database(cursor, expected_counts: dict[str, int]) -> dict[str, int]:
    actual_counts = database_counts(cursor)
    mismatches = {
        table: {"expected": expected_counts[table], "actual": actual_counts[table]}
        for table in expected_counts
        if expected_counts[table] != actual_counts[table]
    }
    if mismatches:
        raise RuntimeError(f"Contagens no PostgreSQL não correspondem aos Parquets: {mismatches}")

    cursor.execute(
        """
        SELECT count(*)
        FROM analytics.fact_orders f
        LEFT JOIN analytics.dim_customer d ON d.customer_key = f.customer_key
        WHERE f.customer_key IS NOT NULL AND d.customer_key IS NULL
        """
    )
    customer_orphans = int(cursor.fetchone()[0])
    cursor.execute(
        """
        SELECT count(*)
        FROM analytics.fact_orders f
        LEFT JOIN analytics.dim_order_status d ON d.order_status_key = f.order_status_key
        WHERE f.order_status_key IS NOT NULL AND d.order_status_key IS NULL
        """
    )
    status_orphans = int(cursor.fetchone()[0])
    if customer_orphans or status_orphans:
        raise RuntimeError(
            f"Foram encontradas referências órfãs: clientes={customer_orphans}, "
            f"status={status_orphans}."
        )

    date_orphans = {}
    for date_column in [
        "purchase_date_key",
        "approved_date_key",
        "carrier_date_key",
        "delivered_date_key",
        "estimated_delivery_date_key",
    ]:
        cursor.execute(
            f"""
            SELECT count(*)
            FROM analytics.fact_orders f
            LEFT JOIN analytics.dim_date d ON d.date_key = f.{date_column}
            WHERE f.{date_column} IS NOT NULL AND d.date_key IS NULL
            """
        )
        date_orphans[date_column] = int(cursor.fetchone()[0])
    if any(date_orphans.values()):
        raise RuntimeError(f"Foram encontradas chaves de data órfãs: {date_orphans}")

    cursor.execute(
        "SELECT count(*) FROM analytics.fact_orders "
        "WHERE order_id IS NULL"
    )
    missing_order_ids = int(cursor.fetchone()[0])
    if missing_order_ids:
        raise RuntimeError(f"fact_orders contém {missing_order_ids} order_id ausente(s).")
    return actual_counts


def load_frames(frames: dict[str, pd.DataFrame]) -> dict[str, int]:
    postgres_password = os.getenv("POSTGRES_PASSWORD")
    if not postgres_password:
        raise ValueError("Configure POSTGRES_PASSWORD no ambiente do serviço.")

    connection_config = {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
        "dbname": os.getenv("POSTGRES_DB", "olist_analytics"),
        "user": os.getenv("POSTGRES_USER", "olist"),
        "password": postgres_password,
        "connect_timeout": 15,
    }
    expected_counts = {name: len(frame) for name, frame in frames.items()}

    with psycopg.connect(**connection_config) as connection:
        with connection.transaction():
            with connection.cursor() as cursor:
                ensure_schema_exists(cursor)
                cursor.executemany(DIM_CUSTOMER_INSERT, frame_rows(frames["dim_customer"]))
                cursor.executemany(DIM_DATE_INSERT, frame_rows(frames["dim_date"]))
                cursor.executemany(
                    DIM_STATUS_INSERT,
                    frame_rows(frames["dim_order_status"]),
                )
                customer_map, status_map = dimension_key_maps(cursor)
                fact_rows = build_fact_rows(
                    frames["fact_orders"], customer_map, status_map
                )
                cursor.executemany(FACT_ORDERS_INSERT, fact_rows)
                actual_counts = validate_database(cursor, expected_counts)
    return actual_counts


def manifest_object_key(run_id: str) -> str:
    return f"warehouse/_manifests/{run_id}.json"


def write_run_manifest(client, bucket: str, object_key: str, report: dict[str, Any]) -> None:
    body = json.dumps(report, ensure_ascii=False, indent=2, default=str).encode("utf-8")
    client.put_object(
        Bucket=bucket,
        Key=object_key,
        Body=body,
        ContentType="application/json; charset=utf-8",
    )


def main() -> int:
    load_dotenv(PROJECT_ROOT / ".env")
    bucket = os.getenv("MINIO_BUCKET", BUCKET_DEFAULT)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    object_key = manifest_object_key(run_id)
    started_at = datetime.now(timezone.utc)
    timer_started = perf_counter()
    phase = "initialize"
    client = None
    database_committed = False
    report: dict[str, Any] = {
        "run_id": run_id,
        "pipeline": "curated_to_postgres",
        "stage": "warehouse_load",
        "status": "running",
        "started_at_utc": started_at.isoformat(),
        "finished_at_utc": None,
        "duration_seconds": None,
        "source": {
            "system": "minio",
            "bucket": bucket,
            "layer": "curated",
            "objects": [config["object"] for config in PARQUET_TABLES.values()],
        },
        "target": "PostgreSQL analytics schema",
        "strategy": "transactional upsert by natural/primary key",
        "rows": {"input": {}, "destination": {}},
        "validations": {
            "curated_data": "pending",
            "postgres_integrity": "pending",
        },
        "database_status": "pending",
        "manifest_status": "pending",
        "error": None,
    }

    try:
        phase = "connect_to_minio"
        client = make_s3_client()
        client.head_bucket(Bucket=bucket)
        phase = "write_running_manifest"
        write_run_manifest(client, bucket, object_key, report)

        phase = "read_curated_objects"
        with tempfile.TemporaryDirectory(prefix="olist_warehouse_load_") as temp_name:
            temporary_dir = Path(temp_name)
            frames = {
                name: read_curated_table(client, bucket, name, temporary_dir)
                for name in PARQUET_TABLES
            }

        report["rows"]["input"] = {name: len(frame) for name, frame in frames.items()}
        phase = "validate_curated_data"
        validate_curated_frames(frames)
        report["validations"]["curated_data"] = "passed"

        phase = "load_and_validate_postgres"
        loaded_counts = load_frames(frames)
        database_committed = True
        report["rows"]["destination"] = loaded_counts
        report["validations"]["postgres_integrity"] = "passed"
        report["database_status"] = "succeeded"

        report["status"] = "succeeded"
        report["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
        report["duration_seconds"] = round(perf_counter() - timer_started, 3)
        report["manifest_status"] = "written"
        phase = "write_success_manifest"
        write_run_manifest(client, bucket, object_key, report)
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0
    except Exception as error:
        report["status"] = "succeeded_with_observability_error" if database_committed else "failed"
        report["database_status"] = "succeeded" if database_committed else "rolled_back_or_not_started"
        report["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
        report["duration_seconds"] = round(perf_counter() - timer_started, 3)
        report["error"] = {
            "phase": phase,
            "type": type(error).__name__,
            "message": str(error),
        }
        report["manifest_status"] = "write_failed" if phase == "write_success_manifest" else "pending"
        if phase == "validate_curated_data":
            report["validations"]["curated_data"] = "failed"
        if phase == "load_and_validate_postgres":
            report["validations"]["postgres_integrity"] = "failed"

        if client is not None and phase != "write_success_manifest":
            report["manifest_status"] = "written"
            try:
                write_run_manifest(client, bucket, object_key, report)
            except Exception as manifest_error:
                report["manifest_status"] = "write_failed"
                report["manifest_error"] = str(manifest_error)

        print(json.dumps(report, ensure_ascii=False, indent=2, default=str), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
