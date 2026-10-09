from __future__ import annotations

import hashlib
import json
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as parquet
import boto3
from botocore.client import Config
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]
BUCKET_DEFAULT = "olist-data-lake"

# Declarado o esquema esperado para detectar alterações na fonte.
# IDs e textos são strings; números e datas recebem tipos específicos.
TABLES = [
    {
        "name": "customers",
        "file": "olist_customers_dataset.csv",
        "columns": [
            "customer_id", "customer_unique_id", "customer_zip_code_prefix",
            "customer_city", "customer_state",
        ],
        "dtypes": {
            "customer_id": "string",
            "customer_unique_id": "string",
            "customer_zip_code_prefix": "string",
            "customer_city": "string",
            "customer_state": "string",
        },
        "primary_key": ["customer_id"],
        "foreign_keys": [],
        "dates": [],
        "postal_codes": ["customer_zip_code_prefix"],
    },
    {
        "name": "orders",
        "file": "olist_orders_dataset.csv",
        "columns": [
            "order_id", "customer_id", "order_status", "order_purchase_timestamp",
            "order_approved_at", "order_delivered_carrier_date",
            "order_delivered_customer_date", "order_estimated_delivery_date",
        ],
        "dtypes": {
            "order_id": "string",
            "customer_id": "string",
            "order_status": "string",
            "order_purchase_timestamp": "string",
            "order_approved_at": "string",
            "order_delivered_carrier_date": "string",
            "order_delivered_customer_date": "string",
            "order_estimated_delivery_date": "string",
        },
        "primary_key": ["order_id"],
        "foreign_keys": [("customer_id", "customers", "customer_id")],
        "dates": [
            "order_purchase_timestamp", "order_approved_at",
            "order_delivered_carrier_date", "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ],
        "postal_codes": [],
    },
    {
        "name": "product_category_name_translation",
        "file": "product_category_name_translation.csv",
        "columns": ["product_category_name", "product_category_name_english"],
        "dtypes": {
            "product_category_name": "string",
            "product_category_name_english": "string",
        },
        "primary_key": ["product_category_name"],
        "foreign_keys": [],
        "dates": [],
        "postal_codes": [],
    },
    {
        "name": "products",
        "file": "olist_products_dataset.csv",
        "columns": [
            "product_id", "product_category_name", "product_name_lenght",
            "product_description_lenght", "product_photos_qty", "product_weight_g",
            "product_length_cm", "product_height_cm", "product_width_cm",
        ],
        "dtypes": {
            "product_id": "string",
            "product_category_name": "string",
            "product_name_lenght": "Int64",
            "product_description_lenght": "Int64",
            "product_photos_qty": "Int64",
            "product_weight_g": "Int64",
            "product_length_cm": "Int64",
            "product_height_cm": "Int64",
            "product_width_cm": "Int64",
        },
        "primary_key": ["product_id"],
        "foreign_keys": [],
        "dates": [],
        "postal_codes": [],
    },
    {
        "name": "sellers",
        "file": "olist_sellers_dataset.csv",
        "columns": [
            "seller_id", "seller_zip_code_prefix", "seller_city", "seller_state",
        ],
        "dtypes": {
            "seller_id": "string",
            "seller_zip_code_prefix": "string",
            "seller_city": "string",
            "seller_state": "string",
        },
        "primary_key": ["seller_id"],
        "foreign_keys": [],
        "dates": [],
        "postal_codes": ["seller_zip_code_prefix"],
    },
    {
        "name": "order_items",
        "file": "olist_order_items_dataset.csv",
        "columns": [
            "order_id", "order_item_id", "product_id", "seller_id",
            "shipping_limit_date", "price", "freight_value",
        ],
        "dtypes": {
            "order_id": "string",
            "order_item_id": "Int64",
            "product_id": "string",
            "seller_id": "string",
            "shipping_limit_date": "string",
            "price": "Float64",
            "freight_value": "Float64",
        },
        "primary_key": ["order_id", "order_item_id"],
        "foreign_keys": [
            ("order_id", "orders", "order_id"),
            ("product_id", "products", "product_id"),
            ("seller_id", "sellers", "seller_id"),
        ],
        "dates": ["shipping_limit_date"],
        "postal_codes": [],
    },
    {
        "name": "order_payments",
        "file": "olist_order_payments_dataset.csv",
        "columns": [
            "order_id", "payment_sequential", "payment_type",
            "payment_installments", "payment_value",
        ],
        "dtypes": {
            "order_id": "string",
            "payment_sequential": "Int64",
            "payment_type": "string",
            "payment_installments": "Int64",
            "payment_value": "Float64",
        },
        "primary_key": ["order_id", "payment_sequential"],
        "foreign_keys": [("order_id", "orders", "order_id")],
        "dates": [],
        "postal_codes": [],
    },
    {
        "name": "order_reviews",
        "file": "olist_order_reviews_dataset.csv",
        "columns": [
            "review_id", "order_id", "review_score", "review_comment_title",
            "review_comment_message", "review_creation_date", "review_answer_timestamp",
        ],
        "dtypes": {
            "review_id": "string",
            "order_id": "string",
            "review_score": "Int64",
            "review_comment_title": "string",
            "review_comment_message": "string",
            "review_creation_date": "string",
            "review_answer_timestamp": "string",
        },
        "primary_key": ["review_id", "order_id"],
        "foreign_keys": [("order_id", "orders", "order_id")],
        "dates": ["review_creation_date", "review_answer_timestamp"],
        "postal_codes": [],
    },
    {
        "name": "geolocation",
        "file": "olist_geolocation_dataset.csv",
        "columns": [
            "geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng",
            "geolocation_city", "geolocation_state",
        ],
        "dtypes": {
            "geolocation_zip_code_prefix": "string",
            "geolocation_lat": "Float64",
            "geolocation_lng": "Float64",
            "geolocation_city": "string",
            "geolocation_state": "string",
        },
        "primary_key": [],
        "foreign_keys": [],
        "dates": [],
        "postal_codes": ["geolocation_zip_code_prefix"],
    },
]


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source_file:
        for chunk in iter(lambda: source_file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def examples_as_strings(frame: pd.DataFrame, columns: list[str], limit: int = 5) -> list[dict[str, Any]]:
    examples = frame.loc[:, columns].drop_duplicates().head(limit)
    return [
        {
            column: None if pd.isna(value) else str(value)
            for column, value in row.items()
        }
        for row in examples.to_dict(orient="records")
    ]


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
            "affected_rows": count,
            "examples": examples or [],
        })


def validate_primary_key(
    frame: pd.DataFrame,
    key_columns: list[str],
    issues: list[dict[str, Any]],
) -> dict[str, Any] | None:
    if not key_columns:
        return None

    complete_key = frame[key_columns].notna().all(axis=1)
    duplicate_mask = pd.Series(False, index=frame.index)
    duplicate_mask.loc[complete_key] = frame.loc[complete_key].duplicated(
        subset=key_columns, keep=False
    )
    duplicate_count = int(duplicate_mask.sum())
    add_issue(
        issues,
        "duplicate_candidate_key",
        "+".join(key_columns),
        duplicate_count,
        examples_as_strings(frame.loc[duplicate_mask], key_columns),
    )
    return {
        "columns": key_columns,
        "duplicate_rows": duplicate_count,
        "rows_with_missing_key": int((~complete_key).sum()),
    }


def validate_foreign_keys(
    frame: pd.DataFrame,
    table_name: str,
    foreign_keys: list[tuple[str, str, str]],
    known_keys: dict[str, set[str]],
    issues: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    results = []
    for column, parent_table, parent_column in foreign_keys:
        if parent_table not in known_keys:
            raise RuntimeError(
                f"A tabela pai '{parent_table}' precisa ser processada antes de '{table_name}'."
            )

        values = frame[column]
        orphan_mask = values.notna() & ~values.isin(known_keys[parent_table])
        orphan_count = int(orphan_mask.sum())
        add_issue(
            issues,
            "orphan_foreign_key",
            column,
            orphan_count,
            values.loc[orphan_mask].drop_duplicates().head(5).astype(str).tolist(),
        )
        results.append({
            "column": column,
            "references": f"{parent_table}.{parent_column}",
            "orphan_rows": orphan_count,
            "rows_with_missing_value": int(values.isna().sum()),
        })
    return results


def normalize_postal_codes(
    frame: pd.DataFrame,
    columns: list[str],
    issues: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    results = []
    for column in columns:
        original = frame[column].copy()
        normalized = original.str.strip().str.zfill(5)
        changed_mask = original.notna() & normalized.ne(original)
        invalid_mask = normalized.notna() & ~normalized.str.fullmatch(r"\d{5}", na=False)
        invalid_count = int(invalid_mask.sum())

        add_issue(
            issues,
            "invalid_postal_code_prefix",
            column,
            invalid_count,
            normalized.loc[invalid_mask].drop_duplicates().head(5).astype(str).tolist(),
        )
        frame[column] = normalized
        results.append({
            "column": column,
            "normalized_rows": int(changed_mask.sum()),
            "invalid_rows": invalid_count,
        })
    return results


def parse_dates(
    frame: pd.DataFrame,
    columns: list[str],
    issues: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    results = []
    for column in columns:
        original = frame[column].copy()
        parsed = pd.to_datetime(original, errors="coerce", format="mixed")
        invalid_mask = original.notna() & parsed.isna()
        invalid_count = int(invalid_mask.sum())

        add_issue(
            issues,
            "unparseable_date",
            column,
            invalid_count,
            original.loc[invalid_mask].drop_duplicates().head(5).astype(str).tolist(),
        )
        frame[column] = parsed
        results.append({
            "column": column,
            "invalid_rows": invalid_count,
            "missing_rows": int(frame[column].isna().sum()),
        })
    return results


def process_table(
    client,
    bucket: str,
    config: dict[str, Any],
    known_keys: dict[str, set[str]],
    translated_categories: set[str],
    temporary_dir: Path,
) -> tuple[dict[str, Any], set[str] | None]:
    source_name = config["file"]
    raw_key = f"raw/{source_name}"
    output_key = f"processed/{Path(source_name).stem}.parquet"
    input_path = temporary_dir / source_name
    output_path = temporary_dir / f"{config['name']}.parquet"

    print(f"\nProcessando {source_name}...")
    client.download_file(bucket, raw_key, str(input_path))
    input_hash = sha256_file(input_path)

    frame = pd.read_csv(
        input_path,
        encoding="utf-8-sig",
        dtype=config["dtypes"],
        low_memory=False,
    )
    expected_columns = set(config["columns"])
    actual_columns = set(frame.columns)
    if actual_columns != expected_columns:
        missing = sorted(expected_columns - actual_columns)
        unexpected = sorted(actual_columns - expected_columns)
        raise ValueError(
            f"Esquema inesperado em {source_name}; faltando={missing}, extras={unexpected}"
        )

    input_rows = len(frame)
    issues: list[dict[str, Any]] = []
    date_results = parse_dates(frame, config["dates"], issues)
    postal_results = normalize_postal_codes(frame, config["postal_codes"], issues)
    primary_key_result = validate_primary_key(frame, config["primary_key"], issues)
    foreign_key_results = validate_foreign_keys(
        frame, config["name"], config["foreign_keys"], known_keys, issues
    )

    category_result = None
    if config["name"] == "products":
        categories = frame["product_category_name"]
        unmatched = categories.notna() & ~categories.isin(translated_categories)
        unmatched_count = int(unmatched.sum())
        add_issue(
            issues,
            "category_without_translation",
            "product_category_name",
            unmatched_count,
            categories.loc[unmatched].drop_duplicates().head(5).astype(str).tolist(),
        )
        category_result = {
            "untranslated_rows": unmatched_count,
            "rows_without_category": int(categories.isna().sum()),
        }

    nulls_by_column = {
        column: int(count)
        for column, count in frame.isna().sum().items()
    }
    column_types = {column: str(dtype) for column, dtype in frame.dtypes.items()}

    frame.to_parquet(
        output_path,
        engine="pyarrow",
        compression="snappy",
        index=False,
    )
    parquet_rows = int(parquet.ParquetFile(output_path).metadata.num_rows)
    if parquet_rows != input_rows:
        raise RuntimeError(
            f"A contagem de linhas mudou em {source_name}: CSV={input_rows}, Parquet={parquet_rows}"
        )

    output_hash = sha256_file(output_path)
    client.upload_file(
        str(output_path),
        bucket,
        output_key,
        ExtraArgs={
            "ContentType": "application/vnd.apache.parquet",
            "Metadata": {"sha256": output_hash},
        },
    )

    report = {
        "table": config["name"],
        "source_object": raw_key,
        "source_sha256": input_hash,
        "output_object": output_key,
        "output_sha256": output_hash,
        "source_size_bytes": input_path.stat().st_size,
        "output_size_bytes": output_path.stat().st_size,
        "rows_read": input_rows,
        "rows_written": parquet_rows,
        "columns": column_types,
        "nulls_by_column": nulls_by_column,
        "candidate_primary_key": primary_key_result,
        "foreign_keys": foreign_key_results,
        "date_checks": date_results,
        "postal_code_checks": postal_results,
        "category_check": category_result,
        "issues": issues,
    }

    primary_key = config["primary_key"]
    reference_values = None
    if len(primary_key) == 1:
        reference_values = {
            str(value)
            for value in frame[primary_key[0]].dropna().tolist()
        }

    if config["name"] == "product_category_name_translation":
        reference_values = {
            str(value)
            for value in frame["product_category_name"].dropna().tolist()
        }

    print(
        f"  {input_rows} linhas → {parquet_rows} linhas; "
        f"{len(issues)} tipo(s) de problema sinalizado(s)"
    )
    del frame
    return report, reference_values


def main() -> int:
    load_dotenv(PROJECT_ROOT / ".env")
    bucket = os.getenv("MINIO_BUCKET", BUCKET_DEFAULT)

    try:
        client = make_s3_client()
        client.head_bucket(Bucket=bucket)

        all_reports = []
        known_keys: dict[str, set[str]] = {}
        translated_categories: set[str] = set()

        with tempfile.TemporaryDirectory(prefix="olist_processed_") as temp_name:
            temporary_dir = Path(temp_name)
            for config in TABLES:
                report, reference_values = process_table(
                    client,
                    bucket,
                    config,
                    known_keys,
                    translated_categories,
                    temporary_dir,
                )
                all_reports.append(report)
                if config["name"] == "product_category_name_translation":
                    translated_categories = reference_values or set()
                if reference_values is not None and config["name"] != "product_category_name_translation":
                    known_keys[config["name"]] = reference_values

        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
        issue_count = sum(len(report["issues"]) for report in all_reports)
        manifest = {
            "run_id": run_id,
            "processed_at_utc": datetime.now(timezone.utc).isoformat(),
            "bucket": bucket,
            "rules": {
                "raw_is_unchanged": True,
                "rows_are_not_removed": True,
                "joins_are_not_performed": True,
                "parquet_compression": "snappy",
            },
            "tables": all_reports,
            "tables_processed": len(all_reports),
            "issue_types_signaled": issue_count,
        }
        manifest_key = f"processed/_manifests/{run_id}.json"
        client.put_object(
            Bucket=bucket,
            Key=manifest_key,
            Body=json.dumps(manifest, ensure_ascii=False, indent=2, default=str).encode("utf-8"),
            ContentType="application/json; charset=utf-8",
        )

        print(f"\nConcluído: {len(all_reports)} tabelas gravadas em processed/.")
        print(f"Tipos de problemas sinalizados para revisão: {issue_count}")
        print(f"Manifesto de qualidade: s3://{bucket}/{manifest_key}")
        return 0
    except NoCredentialsError:
        print("Credenciais do MinIO não foram encontradas no .env.", file=sys.stderr)
    except ClientError as error:
        code = error.response.get("Error", {}).get("Code", "erro S3")
        print(
            f"Falha ao acessar o bucket '{bucket}' ({code}). Confira se o MinIO está ativo, "
            "se o bucket existe e se as credenciais estão corretas.",
            file=sys.stderr,
        )
    except BotoCoreError as error:
        print(f"Falha de comunicação com o MinIO: {error}", file=sys.stderr)
    except Exception as error:
        print(f"Falha no processamento ({type(error).__name__}): {error}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())


