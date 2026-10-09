from __future__ import annotations

import hashlib
import json
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import boto3
import pandas as pd
import pyarrow.parquet as parquet
from botocore.client import Config
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]
BUCKET_DEFAULT = "olist-data-lake"
CUSTOMER_COLUMNS = [
    "customer_id",
    "customer_unique_id",
    "customer_zip_code_prefix",
    "customer_city",
    "customer_state",
]
ORDER_COLUMNS = [
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]
ORDER_DATE_COLUMNS = {
    "order_purchase_timestamp": "purchase_date_key",
    "order_approved_at": "approved_date_key",
    "order_delivered_carrier_date": "carrier_date_key",
    "order_delivered_customer_date": "delivered_date_key",
    "order_estimated_delivery_date": "estimated_delivery_date_key",
}
ORDER_TIMESTAMP_COLUMNS = {
    "order_purchase_timestamp": "order_purchase_at",
    "order_approved_at": "order_approved_at",
    "order_delivered_carrier_date": "order_carrier_at",
    "order_delivered_customer_date": "order_delivered_at",
    "order_estimated_delivery_date": "order_estimated_delivery_at",
}


class CuratedValidationError(Exception):
    """Raised when the source cannot safely satisfy the declared curated grain."""


def make_s3_client():
    endpoint = os.getenv("MINIO_ENDPOINT_URL", "http://localhost:9000")
    access_key = os.getenv("MINIO_ROOT_USER")
    secret_key = os.getenv("MINIO_ROOT_PASSWORD")
    if not access_key or not secret_key:
        raise ValueError(
            "Configure MINIO_ROOT_USER e MINIO_ROOT_PASSWORD no arquivo .env do projeto."
        )

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}, signature_version="s3v4"),
    )


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source_file:
        for chunk in iter(lambda: source_file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def add_issue(
    issues: list[dict[str, Any]],
    check: str,
    column: str,
    count: int,
    examples: list[Any] | None = None,
) -> None:
    if count:
        issues.append({
            "check": check,
            "column": column,
            "affected_rows": int(count),
            "examples": examples or [],
        })


def read_processed_parquet(
    client,
    bucket: str,
    object_key: str,
    required_columns: list[str],
    temporary_dir: Path,
) -> tuple[pd.DataFrame, str]:
    local_path = temporary_dir / Path(object_key).name
    client.download_file(bucket, object_key, str(local_path))
    input_hash = sha256_file(local_path)
    frame = pd.read_parquet(local_path, engine="pyarrow")

    missing = sorted(set(required_columns) - set(frame.columns))
    if missing:
        raise ValueError(
            f"O objeto {object_key} não contém as colunas esperadas: {missing}"
        )
    return frame.loc[:, required_columns].copy(), input_hash


def key_issues(
    frame: pd.DataFrame,
    key_column: str,
    table_name: str,
    issues: list[dict[str, Any]],
) -> list[str]:
    missing_mask = frame[key_column].isna()
    duplicate_mask = frame[key_column].notna() & frame[key_column].duplicated(keep=False)
    add_issue(
        issues,
        "missing_natural_key",
        f"{table_name}.{key_column}",
        int(missing_mask.sum()),
    )
    add_issue(
        issues,
        "duplicate_natural_key",
        f"{table_name}.{key_column}",
        int(duplicate_mask.sum()),
        frame.loc[duplicate_mask, key_column].drop_duplicates().head(5).astype(str).tolist(),
    )

    blockers = []
    if missing_mask.any():
        blockers.append(f"{table_name}.{key_column} tem valores ausentes")
    if duplicate_mask.any():
        blockers.append(f"{table_name}.{key_column} não é único")
    return blockers


def parse_order_timestamps(orders: pd.DataFrame) -> pd.DataFrame:
    parsed = orders.copy()
    for column in ORDER_DATE_COLUMNS:
        original = parsed[column]
        values = pd.to_datetime(original, errors="coerce")
        invalid_mask = original.notna() & values.isna()
        if invalid_mask.any():
            examples = original.loc[invalid_mask].drop_duplicates().head(5).astype(str).tolist()
            raise ValueError(
                f"Datas não interpretáveis em processed.{column}: {examples}. "
                "Confira o manifesto da camada processed."
            )
        parsed[column] = values
    return parsed


def build_dim_customer(customers: pd.DataFrame) -> pd.DataFrame:
    return customers.loc[:, CUSTOMER_COLUMNS].sort_values(
        "customer_id", kind="stable", na_position="last"
    ).reset_index(drop=True)


def build_dim_order_status(orders: pd.DataFrame) -> pd.DataFrame:
    statuses = orders.loc[orders["order_status"].notna(), "order_status"]
    return (
        statuses.drop_duplicates()
        .sort_values(kind="stable")
        .rename("order_status")
        .reset_index(drop=True)
        .to_frame()
    )


def build_dim_date(orders: pd.DataFrame) -> pd.DataFrame:
    date_values = pd.concat(
        [orders[column].dropna().dt.normalize() for column in ORDER_DATE_COLUMNS],
        ignore_index=True,
    )
    if date_values.empty:
        return pd.DataFrame({
            "date_key": pd.Series(dtype="Int64"),
            "full_date": pd.Series(dtype="object"),
            "calendar_year": pd.Series(dtype="Int16"),
            "calendar_quarter": pd.Series(dtype="Int16"),
            "calendar_month": pd.Series(dtype="Int16"),
            "day_of_month": pd.Series(dtype="Int16"),
            "iso_day_of_week": pd.Series(dtype="Int16"),
            "is_weekend": pd.Series(dtype="boolean"),
        })

    calendar = pd.date_range(date_values.min(), date_values.max(), freq="D")
    frame = pd.DataFrame({"_date": calendar})
    frame["date_key"] = frame["_date"].dt.strftime("%Y%m%d").astype("Int64")
    frame["full_date"] = frame["_date"].dt.date
    frame["calendar_year"] = frame["_date"].dt.year.astype("Int16")
    frame["calendar_quarter"] = frame["_date"].dt.quarter.astype("Int16")
    frame["calendar_month"] = frame["_date"].dt.month.astype("Int16")
    frame["day_of_month"] = frame["_date"].dt.day.astype("Int16")
    frame["iso_day_of_week"] = frame["_date"].dt.isocalendar().day.astype("Int16")
    frame["is_weekend"] = frame["_date"].dt.dayofweek.ge(5).astype("boolean")
    return frame.drop(columns="_date")


def chronology_issues(orders: pd.DataFrame, issues: list[dict[str, Any]]) -> dict[str, Any]:
    actual_events = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
    ]
    any_inversion = pd.Series(False, index=orders.index)
    pair_reports = []

    for earlier_index, earlier_column in enumerate(actual_events):
        for later_column in actual_events[earlier_index + 1:]:
            both_present = orders[earlier_column].notna() & orders[later_column].notna()
            inversion_mask = both_present & orders[later_column].lt(orders[earlier_column])
            affected_rows = int(inversion_mask.sum())
            any_inversion |= inversion_mask

            examples = []
            for row in orders.loc[
                inversion_mask, ["order_id", earlier_column, later_column]
            ].head(5).to_dict(orient="records"):
                examples.append({
                    "order_id": str(row["order_id"]),
                    "earlier_event": earlier_column,
                    "earlier_timestamp": row[earlier_column].isoformat(sep=" "),
                    "later_event": later_column,
                    "later_timestamp": row[later_column].isoformat(sep=" "),
                })

            pair_reports.append({
                "expected_order": [earlier_column, later_column],
                "violation": f"{later_column} < {earlier_column}",
                "affected_rows": affected_rows,
            })
            add_issue(
                issues,
                "timestamp_order_inversion",
                f"{later_column} < {earlier_column}",
                affected_rows,
                examples,
            )

    estimated_before_purchase = (
        orders["order_estimated_delivery_date"].notna()
        & orders["order_purchase_timestamp"].notna()
        & orders["order_estimated_delivery_date"].lt(orders["order_purchase_timestamp"])
    )
    add_issue(
        issues,
        "estimated_delivery_before_purchase",
        "order_estimated_delivery_date/order_purchase_timestamp",
        int(estimated_before_purchase.sum()),
        orders.loc[estimated_before_purchase, "order_id"].head(5).astype(str).tolist(),
    )

    return {
        "orders_with_any_actual_timestamp_inversion": int(any_inversion.sum()),
        "actual_event_pairs": pair_reports,
    }


def build_fact_orders(
    orders: pd.DataFrame,
    customers: pd.DataFrame,
    issues: list[dict[str, Any]],
) -> tuple[pd.DataFrame, dict[str, int]]:
    customer_reference = customers.loc[:, ["customer_id"]].drop_duplicates()
    customer_reference["_customer_found"] = True
    before_join = len(orders)
    joined = orders.merge(
        customer_reference,
        how="left",
        on="customer_id",
        validate="many_to_one",
        sort=False,
    )
    if len(joined) != before_join:
        raise RuntimeError(
            f"O join com clientes alterou a quantidade de pedidos: "
            f"antes={before_join}, depois={len(joined)}."
        )

    missing_customer = joined["customer_id"].isna()
    orphan_customer = joined["customer_id"].notna() & joined["_customer_found"].isna()
    add_issue(
        issues,
        "order_without_customer_id",
        "customer_id",
        int(missing_customer.sum()),
        joined.loc[missing_customer, "order_id"].head(5).astype(str).tolist(),
    )
    add_issue(
        issues,
        "customer_reference_not_found",
        "customer_id",
        int(orphan_customer.sum()),
        joined.loc[orphan_customer, "customer_id"].drop_duplicates().head(5).astype(str).tolist(),
    )

    chronology_summary = chronology_issues(joined, issues)
    purchase = joined["order_purchase_timestamp"]
    delivered = joined["order_delivered_customer_date"]
    estimated = joined["order_estimated_delivery_date"]

    delivery_days = (delivered - purchase).dt.total_seconds().div(86400).round(2)
    delivery_days = delivery_days.where(purchase.notna() & delivered.notna()).astype("Float64")
    comparison_valid = delivered.notna() & estimated.notna()
    delivered_on_time = pd.Series(pd.NA, index=joined.index, dtype="boolean")
    delivered_on_time.loc[comparison_valid] = delivered.loc[comparison_valid].le(
        estimated.loc[comparison_valid]
    )

    fact = pd.DataFrame({
        "order_id": joined["order_id"],
        "customer_id": joined["customer_id"],
        "order_status": joined["order_status"],
    })
    for source_column, target_column in ORDER_DATE_COLUMNS.items():
        parsed = joined[source_column]
        fact[target_column] = parsed.dt.strftime("%Y%m%d").astype("Int64")
    for source_column, target_column in ORDER_TIMESTAMP_COLUMNS.items():
        fact[target_column] = joined[source_column]
    fact["delivery_days"] = delivery_days
    fact["delivered_on_time"] = delivered_on_time

    if len(fact) != len(orders):
        raise RuntimeError(
            f"A contagem de pedidos mudou ao montar fact_orders: "
            f"entrada={len(orders)}, saída={len(fact)}."
        )
    return fact.reset_index(drop=True), {
        "orders_before_customer_join": before_join,
        "orders_after_customer_join": len(joined),
    }, chronology_summary


def frame_summary(frame: pd.DataFrame) -> dict[str, Any]:
    return {
        "rows": int(len(frame)),
        "columns": {column: str(dtype) for column, dtype in frame.dtypes.items()},
        "nulls_by_column": {
            column: int(count) for column, count in frame.isna().sum().items()
        },
    }


def upload_manifest(client, bucket: str, manifest: dict[str, Any], run_id: str) -> str:
    manifest_key = f"curated/_manifests/{run_id}.json"
    payload = json.dumps(manifest, ensure_ascii=False, indent=2, default=str).encode("utf-8")
    client.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=payload,
        ContentType="application/json; charset=utf-8",
    )
    return manifest_key


def upload_parquet(client, bucket: str, frame: pd.DataFrame, object_key: str, path: Path) -> dict[str, Any]:
    frame.to_parquet(path, engine="pyarrow", compression="snappy", index=False)
    output_rows = int(parquet.ParquetFile(path).metadata.num_rows)
    if output_rows != len(frame):
        raise RuntimeError(
            f"A contagem de linhas mudou ao gravar {object_key}: "
            f"DataFrame={len(frame)}, Parquet={output_rows}."
        )
    output_hash = sha256_file(path)
    client.upload_file(
        str(path),
        bucket,
        object_key,
        ExtraArgs={
            "ContentType": "application/vnd.apache.parquet",
            "Metadata": {"sha256": output_hash},
        },
    )
    return {
        "object": object_key,
        "sha256": output_hash,
        "size_bytes": path.stat().st_size,
        "rows": output_rows,
    }


def main() -> int:
    load_dotenv(PROJECT_ROOT / ".env")
    bucket = os.getenv("MINIO_BUCKET", BUCKET_DEFAULT)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    issues: list[dict[str, Any]] = []
    source_objects = {
        "customers": "processed/olist_customers_dataset.parquet",
        "orders": "processed/olist_orders_dataset.parquet",
    }

    try:
        client = make_s3_client()
        client.head_bucket(Bucket=bucket)
        with tempfile.TemporaryDirectory(prefix="olist_curated_") as temp_name:
            temporary_dir = Path(temp_name)
            customers, customer_hash = read_processed_parquet(
                client, bucket, source_objects["customers"], CUSTOMER_COLUMNS, temporary_dir
            )
            orders, orders_hash = read_processed_parquet(
                client, bucket, source_objects["orders"], ORDER_COLUMNS, temporary_dir
            )
            orders = parse_order_timestamps(orders)

            blockers = []
            blockers.extend(key_issues(customers, "customer_id", "customers", issues))
            blockers.extend(key_issues(orders, "order_id", "orders", issues))
            if blockers:
                manifest = {
                    "run_id": run_id,
                    "processed_at_utc": datetime.now(timezone.utc).isoformat(),
                    "status": "blocked_by_key_validation",
                    "bucket": bucket,
                    "sources": {
                        source_objects["customers"]: {"sha256": customer_hash, "rows": len(customers)},
                        source_objects["orders"]: {"sha256": orders_hash, "rows": len(orders)},
                    },
                    "issues": issues,
                    "blockers": blockers,
                    "outputs": {},
                }
                manifest_key = upload_manifest(client, bucket, manifest, run_id)
                print(f"Validação interrompida. Manifesto: {manifest_key}", file=sys.stderr)
                for blocker in blockers:
                    print(f"- {blocker}", file=sys.stderr)
                return 1

            dim_customer = build_dim_customer(customers)
            dim_order_status = build_dim_order_status(orders)
            dim_date = build_dim_date(orders)
            fact_orders, join_counts, chronology_summary = build_fact_orders(
                orders, customers, issues
            )

            frames = {
                "dim_customer": dim_customer,
                "dim_date": dim_date,
                "dim_order_status": dim_order_status,
                "fact_orders": fact_orders,
            }
            output_reports = {}
            output_paths = {
                name: temporary_dir / f"{name}.parquet" for name in frames
            }
            for name, frame in frames.items():
                output_reports[name] = upload_parquet(
                    client,
                    bucket,
                    frame,
                    f"curated/{name}.parquet",
                    output_paths[name],
                )

            manifest = {
                "run_id": run_id,
                "processed_at_utc": datetime.now(timezone.utc).isoformat(),
                "status": "succeeded",
                "bucket": bucket,
                "sources": {
                    source_objects["customers"]: {"sha256": customer_hash, "rows": len(customers)},
                    source_objects["orders"]: {"sha256": orders_hash, "rows": len(orders)},
                },
                "rules": {
                    "source_layer": "processed",
                    "raw_and_processed_modified": False,
                    "fact_orders_grain": "one row per order_id",
                    "customer_join": "many-to-one; missing references are retained and reported",
                    "delivery_days": "elapsed days between purchase and customer delivery, rounded to 2 decimals",
                    "delivered_on_time": "comparison only when actual and estimated delivery timestamps exist",
                    "dim_date_range": "continuous calendar days from minimum to maximum valid source date",
                    "issues_remove_rows": False,
                    "parquet_compression": "snappy",
                },
                "join_counts": join_counts,
                "chronology": chronology_summary,
                "outputs": output_reports,
                "tables": {name: frame_summary(frame) for name, frame in frames.items()},
                "issues": issues,
            }
            manifest_key = upload_manifest(client, bucket, manifest, run_id)

        print("Camada curated gerada com sucesso:")
        for name, report in output_reports.items():
            print(f"  {name}: {report['rows']} linhas -> {report['object']}")
        print(f"Manifesto: {manifest_key}")
        if chronology_summary["orders_with_any_actual_timestamp_inversion"]:
            print(
                "Pedidos com alguma inversão entre eventos reais: "
                f"{chronology_summary['orders_with_any_actual_timestamp_inversion']}"
            )
            for pair in chronology_summary["actual_event_pairs"]:
                if pair["affected_rows"]:
                    print(
                        f"  {pair['violation']}: "
                        f"{pair['affected_rows']} pedidos"
                    )
        if issues:
            print(f"Verificações com ocorrências registradas: {len(issues)}")
        else:
            print("Nenhum problema adicional encontrado nesta etapa.")
        return 0
    except (BotoCoreError, ClientError, NoCredentialsError, OSError, ValueError, RuntimeError) as error:
        print(f"Falha ao gerar curated: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
