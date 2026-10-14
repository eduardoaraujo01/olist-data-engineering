from __future__ import annotations

import json
import os
import sys
import tempfile
import uuid
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

TABLES = {
    "dim_customer": {
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
        "columns": ["order_status"],
        "key": ["order_status"],
    },
    "fact_orders": {
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

FACT_DATE_COLUMNS = [
    "purchase_date_key",
    "approved_date_key",
    "carrier_date_key",
    "delivered_date_key",
    "estimated_delivery_date_key",
]


def make_s3_client():
    endpoint = os.getenv("MINIO_ENDPOINT_URL", "http://localhost:9000")
    access_key = os.getenv("MINIO_ROOT_USER")
    secret_key = os.getenv("MINIO_ROOT_PASSWORD")

    if not access_key or not secret_key:
        raise ValueError(
            "Configure MINIO_ROOT_USER e MINIO_ROOT_PASSWORD no arquivo .env."
        )

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
        config=Config(
            s3={"addressing_style": "path"},
            signature_version="s3v4",
        ),
    )


def read_curated_frames(
        client,
        bucket: str,
        batch_id: str,
        temporary_dir: Path,
) -> dict[str, pd.DataFrame]:
    frames = {}

    for table_name, config in TABLES.items():
        object_key = (
            f"curated/batches/{batch_id}/{table_name}.parquet"
        )
        local_path = temporary_dir / f"{table_name}.parquet"

        client.download_file(bucket, object_key, str(local_path))
        frame = pd.read_parquet(local_path, engine="pyarrow")

        missing = sorted(set(config["columns"]) - set(frame.columns))
        if missing:
            raise ValueError(
                f"{object_key} não contém as colunas esperadas: {missing}"
            )

        frames[table_name] = frame.loc[:, config["columns"]].copy()

    return frames


def validate_unique_key(
        frame: pd.DataFrame,
        table_name: str,
        columns: list[str],
) -> None:
    missing_mask = frame[columns].isna().any(axis=1)
    duplicate_mask = frame.loc[~missing_mask].duplicated(
        subset=columns,
        keep=False,
    )

    missing_count = int(missing_mask.sum())
    duplicate_count = int(duplicate_mask.sum())

    if missing_count or duplicate_count:
        examples = (
            frame.loc[duplicate_mask, columns]
            .head(5)
            .to_dict(orient="records")
        )
        raise ValueError(
            f"Chave inválida em {table_name}: "
            f"{missing_count} linha(s) com chave ausente, "
            f"{duplicate_count} linha(s) duplicada(s); "
            f"exemplos={examples}"
        )


def validate_frames(frames: dict[str, pd.DataFrame]) -> None:
    for table_name, config in TABLES.items():
        validate_unique_key(
            frames[table_name],
            table_name,
            config["key"],
        )

    dim_date = frames["dim_date"]
    validate_unique_key(dim_date, "dim_date", ["full_date"])

    dates = pd.to_datetime(dim_date["full_date"], errors="coerce")
    expected_keys = dates.dt.strftime("%Y%m%d").astype("string")
    actual_keys = dim_date["date_key"].astype("string").str.zfill(8)
    mismatch = dates.isna() | expected_keys.ne(actual_keys)

    if mismatch.any():
        examples = (
            dim_date.loc[mismatch, ["date_key", "full_date"]]
            .head(5)
            .to_dict(orient="records")
        )
        raise ValueError(
            "date_key não corresponde a full_date em dim_date: "
            f"{examples}"
        )

    customers = set(
        frames["dim_customer"]["customer_id"].dropna().astype(str)
    )
    statuses = set(
        frames["dim_order_status"]["order_status"].dropna().astype(str)
    )
    date_keys = set(
        frames["dim_date"]["date_key"].dropna().astype(int)
    )
    fact = frames["fact_orders"]

    orphan_customers = (
            fact["customer_id"].notna()
            & ~fact["customer_id"].astype("string").isin(customers)
    )
    if orphan_customers.any():
        examples = (
            fact.loc[orphan_customers, "customer_id"]
            .drop_duplicates()
            .head(5)
            .astype(str)
            .tolist()
        )
        raise ValueError(
            "fact_orders referencia customer_id ausente em dim_customer: "
            f"{examples}"
        )

    orphan_statuses = (
            fact["order_status"].notna()
            & ~fact["order_status"].astype("string").isin(statuses)
    )
    if orphan_statuses.any():
        examples = (
            fact.loc[orphan_statuses, "order_status"]
            .drop_duplicates()
            .head(5)
            .astype(str)
            .tolist()
        )
        raise ValueError(
            "fact_orders referencia order_status ausente "
            f"em dim_order_status: {examples}"
        )

    for column in FACT_DATE_COLUMNS:
        values = fact[column].dropna().astype(int)
        missing_dates = ~values.isin(date_keys)
        if missing_dates.any():
            raise ValueError(
                f"fact_orders.{column} tem "
                f"{int(missing_dates.sum())} chave(s) sem dim_date."
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


def rows_with_batch(
        batch_id: str,
        frame: pd.DataFrame,
):
    for row in frame.itertuples(index=False, name=None):
        yield (
            batch_id,
            *(db_value(value) for value in row),
        )


def build_fact_rows(
        batch_id: str,
        fact: pd.DataFrame,
        customer_map: dict[str, int],
        status_map: dict[str, int],
) -> list[tuple[Any, ...]]:
    column_index = {
        column: index
        for index, column in enumerate(fact.columns)
    }
    rows = []

    for row in fact.itertuples(index=False, name=None):
        customer_id = db_value(row[column_index["customer_id"]])
        order_status = db_value(row[column_index["order_status"]])

        customer_key = (
            customer_map.get(customer_id)
            if customer_id is not None
            else None
        )
        status_key = (
            status_map.get(order_status)
            if order_status is not None
            else None
        )

        if customer_id is not None and customer_key is None:
            raise ValueError(
                "Não foi encontrada chave substituta para "
                f"customer_id={customer_id}"
            )
        if order_status is not None and status_key is None:
            raise ValueError(
                "Não foi encontrada chave substituta para "
                f"order_status={order_status}"
            )

        rows.append((
            batch_id,
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


def write_manifest(
        client,
        bucket: str,
        object_key: str,
        report: dict[str, Any],
) -> None:
    body = json.dumps(
        report,
        ensure_ascii=False,
        indent=2,
        default=str,
    ).encode("utf-8")

    client.put_object(
        Bucket=bucket,
        Key=object_key,
        Body=body,
        ContentType="application/json; charset=utf-8",
    )


def ensure_schema_exists(cursor) -> None:
    cursor.execute(
        """
        SELECT
            to_regclass('analytics_v2.dim_customer'),
            to_regclass('analytics_v2.dim_date'),
            to_regclass('analytics_v2.dim_order_status'),
            to_regclass('analytics_v2.fact_orders')
        """
    )
    relations = cursor.fetchone()

    if any(relation is None for relation in relations):
        raise RuntimeError(
            "O schema analytics_v2 está incompleto. "
            "Aplique sql/v2/01_create_analytics_v2.sql."
        )


def load_frames(
        frames: dict[str, pd.DataFrame],
        batch_id: str,
) -> dict[str, int]:
    postgres_password = os.getenv("POSTGRES_PASSWORD")
    if not postgres_password:
        raise ValueError(
            "Configure POSTGRES_PASSWORD no ambiente do serviço."
        )

    connection_config = {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
        "dbname": os.getenv("POSTGRES_DB", "olist_analytics"),
        "user": os.getenv("POSTGRES_USER", "olist"),
        "password": postgres_password,
        "connect_timeout": 15,
    }

    expected_counts = {
        table_name: len(frame)
        for table_name, frame in frames.items()
    }

    with psycopg.connect(**connection_config) as connection:
        with connection.transaction():
            with connection.cursor() as cursor:
                ensure_schema_exists(cursor)

                # Remove apenas este lote, dentro da mesma transação.
                # Se algo falhar depois, o PostgreSQL desfaz a transação.
                for table_name in [
                    "fact_orders",
                    "dim_customer",
                    "dim_order_status",
                    "dim_date",
                ]:
                    cursor.execute(
                        f"DELETE FROM analytics_v2.{table_name} "
                        "WHERE batch_id = %s",
                        (batch_id,),
                    )

                cursor.executemany(
                    """
                    INSERT INTO analytics_v2.dim_customer (
                        batch_id,
                        customer_id,
                        customer_unique_id,
                        customer_zip_code_prefix,
                        customer_city,
                        customer_state
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    rows_with_batch(
                        batch_id,
                        frames["dim_customer"],
                    ),
                )

                cursor.executemany(
                    """
                    INSERT INTO analytics_v2.dim_date (
                        batch_id,
                        date_key,
                        full_date,
                        calendar_year,
                        calendar_quarter,
                        calendar_month,
                        day_of_month,
                        iso_day_of_week,
                        is_weekend
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    rows_with_batch(
                        batch_id,
                        frames["dim_date"],
                    ),
                )

                cursor.executemany(
                    """
                    INSERT INTO analytics_v2.dim_order_status (
                        batch_id,
                        order_status
                    ) VALUES (%s, %s)
                    """,
                    rows_with_batch(
                        batch_id,
                        frames["dim_order_status"],
                    ),
                )

                cursor.execute(
                    """
                    SELECT customer_id, customer_key
                    FROM analytics_v2.dim_customer
                    WHERE batch_id = %s
                    """,
                    (batch_id,),
                )
                customer_map = {
                    customer_id: customer_key
                    for customer_id, customer_key in cursor.fetchall()
                }

                cursor.execute(
                    """
                    SELECT order_status, order_status_key
                    FROM analytics_v2.dim_order_status
                    WHERE batch_id = %s
                    """,
                    (batch_id,),
                )
                status_map = {
                    order_status: order_status_key
                    for order_status, order_status_key in cursor.fetchall()
                }

                fact_rows = build_fact_rows(
                    batch_id,
                    frames["fact_orders"],
                    customer_map,
                    status_map,
                )

                cursor.executemany(
                    """
                    INSERT INTO analytics_v2.fact_orders (
                        batch_id,
                        order_id,
                        customer_key,
                        order_status_key,
                        purchase_date_key,
                        approved_date_key,
                        carrier_date_key,
                        delivered_date_key,
                        estimated_delivery_date_key,
                        order_purchase_at,
                        order_approved_at,
                        order_carrier_at,
                        order_delivered_at,
                        order_estimated_delivery_at,
                        delivery_days,
                        delivered_on_time
                    ) VALUES (
                                 %s, %s, %s, %s, %s, %s, %s, %s,
                                 %s, %s, %s, %s, %s, %s, %s, %s
                             )
                    """,
                    fact_rows,
                )

                destination_counts = {}
                for table_name in TABLES:
                    cursor.execute(
                        f"""
                        SELECT count(*)
                        FROM analytics_v2.{table_name}
                        WHERE batch_id = %s
                        """,
                        (batch_id,),
                    )
                    destination_counts[table_name] = int(
                        cursor.fetchone()[0]
                    )

                mismatches = {
                    table_name: {
                        "expected": expected_counts[table_name],
                        "actual": destination_counts[table_name],
                    }
                    for table_name in expected_counts
                    if expected_counts[table_name]
                       != destination_counts[table_name]
                }

                if mismatches:
                    raise RuntimeError(
                        "Contagens do PostgreSQL não correspondem "
                        f"aos Parquets: {mismatches}"
                    )

    return destination_counts


def main() -> int:
    load_dotenv(PROJECT_ROOT / ".env")
    bucket = os.getenv("MINIO_BUCKET", BUCKET_DEFAULT)
    batch_id = os.getenv("BATCH_ID", "").strip()

    if (
            not batch_id.startswith("b_")
            or len(batch_id) != 66
            or any(character not in "0123456789abcdef" for character in batch_id[2:])
    ):
        print(
            "Informe BATCH_ID válido. Use o identificador exibido "
            "pela etapa de ingestão.",
            file=sys.stderr,
        )
        return 1

    run_id = uuid.uuid4().hex
    started_at = datetime.now(timezone.utc)
    manifest_key = (
        f"control/manifests/warehouse_load/"
        f"{batch_id}/{run_id}.json"
    )
    timer_started = started_at.timestamp()
    client = None
    database_committed = False

    curated_objects = [
        f"curated/batches/{batch_id}/{table_name}.parquet"
        for table_name in TABLES
    ]

    report: dict[str, Any] = {
        "contract_version": "2.0",
        "batch_id": batch_id,
        "run_id": run_id,
        "stage": "warehouse_load",
        "status": "running",
        "started_at_utc": started_at.isoformat(),
        "finished_at_utc": None,
        "duration_seconds": None,
        "source": {
            "system": "minio",
            "bucket": bucket,
            "layer": "curated",
        },
        "input_objects": curated_objects,
        "target": "PostgreSQL analytics_v2 schema",
        "rows": {"input": {}, "destination": {}},
        "validations": {
            "curated_data": "pending",
            "postgres_counts": "pending",
        },
        "database_status": "pending",
        "error": None,
    }

    try:
        client = make_s3_client()
        client.head_bucket(Bucket=bucket)
        write_manifest(client, bucket, manifest_key, report)

        with tempfile.TemporaryDirectory(
                prefix="olist_warehouse_load_v2_"
        ) as temp_name:
            frames = read_curated_frames(
                client,
                bucket,
                batch_id,
                Path(temp_name),
            )

        report["rows"]["input"] = {
            table_name: len(frame)
            for table_name, frame in frames.items()
        }

        validate_frames(frames)
        report["validations"]["curated_data"] = "passed"

        destination_counts = load_frames(frames, batch_id)
        database_committed = True

        report["rows"]["destination"] = destination_counts
        report["validations"]["postgres_counts"] = "passed"
        report["database_status"] = "succeeded"
        report["status"] = "succeeded"
        report["finished_at_utc"] = datetime.now(
            timezone.utc
        ).isoformat()
        report["duration_seconds"] = round(
            datetime.now(timezone.utc).timestamp() - timer_started,
            3,
            )

        write_manifest(client, bucket, manifest_key, report)
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0

    except Exception as error:
        report["status"] = (
            "succeeded"
            if database_committed
            else "failed"
        )
        report["database_status"] = (
            "succeeded"
            if database_committed
            else "rolled_back_or_not_started"
        )
        report["finished_at_utc"] = datetime.now(
            timezone.utc
        ).isoformat()
        report["duration_seconds"] = round(
            datetime.now(timezone.utc).timestamp() - timer_started,
            3,
            )
        report["error"] = {
            "type": type(error).__name__,
            "message": str(error),
        }

        if client is not None:
            try:
                write_manifest(client, bucket, manifest_key, report)
            except Exception as manifest_error:
                report["manifest_error"] = str(manifest_error)

        print(
            json.dumps(report, ensure_ascii=False, indent=2, default=str),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())