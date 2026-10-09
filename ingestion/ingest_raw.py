from __future__ import annotations

import csv
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.client import Config
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_DIR = PROJECT_ROOT / "data" / "source"
EXPECTED_FILES = (
    "olist_customers_dataset.csv",
    "olist_geolocation_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_products_dataset.csv",
    "olist_sellers_dataset.csv",
    "product_category_name_translation.csv",
)


def inspect_file(path: Path) -> dict[str, object]:
    """Calculate the raw file's checksum and number of data rows (excluding header)."""
    digest = hashlib.sha256()
    with path.open("rb") as source_file:
        for chunk in iter(lambda: source_file.read(1024 * 1024), b""):
            digest.update(chunk)

    with path.open("r", encoding="utf-8-sig", newline="") as source_file:
        reader = csv.reader(source_file)
        header = next(reader, None)
        if not header:
            raise ValueError(f"Arquivo sem cabeçalho ou vazio: {path.name}")
        row_count = sum(1 for _ in reader)

    return {
        "name": path.name,
        "object_key": f"raw/{path.name}",
        "rows": row_count,
        "size_bytes": path.stat().st_size,
        "sha256": digest.hexdigest(),
    }


def source_files() -> list[Path]:
    if not SOURCE_DIR.is_dir():
        raise FileNotFoundError(f"Pasta de origem não encontrada: {SOURCE_DIR}")

    available = {path.name for path in SOURCE_DIR.glob("*.csv")}
    missing = sorted(set(EXPECTED_FILES) - available)
    if missing:
        names = ", ".join(missing)
        raise FileNotFoundError(f"Faltam CSVs esperados em data/source: {names}")

    return [SOURCE_DIR / name for name in EXPECTED_FILES]


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


def object_has_same_checksum(client, bucket: str, item: dict[str, object]) -> bool:
    try:
        response = client.head_object(Bucket=bucket, Key=str(item["object_key"]))
    except ClientError as error:
        code = str(error.response.get("Error", {}).get("Code", ""))
        status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if code in {"404", "NoSuchKey", "NotFound"} or status == 404:
            return False
        raise

    saved_hash = response.get("Metadata", {}).get("sha256")
    return saved_hash == item["sha256"]


def main() -> int:
    load_dotenv(PROJECT_ROOT / ".env")
    bucket = os.getenv("MINIO_BUCKET", "olist-data-lake")

    try:
        files = source_files()
        manifest_files = [inspect_file(path) for path in files]
        client = make_s3_client()
        client.head_bucket(Bucket=bucket)

        for path, item in zip(files, manifest_files, strict=True):
            if object_has_same_checksum(client, bucket, item):
                item["result"] = "unchanged"
                print(f"[SEM ALTERAÇÃO] {path.name} — já existe com o mesmo SHA-256")
                continue

            client.upload_file(
                str(path),
                bucket,
                str(item["object_key"]),
                ExtraArgs={
                    "ContentType": "text/csv; charset=utf-8",
                    "Metadata": {"sha256": str(item["sha256"])},
                },
            )
            item["result"] = "uploaded"
            size_mb = int(item["size_bytes"]) / (1024 * 1024)
            print(f"[ENVIADO] {path.name} — {size_mb:.1f} MiB, {item['rows']} linhas")

        batch_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
        manifest = {
            "batch_id": batch_id,
            "ingested_at_utc": datetime.now(timezone.utc).isoformat(),
            "bucket": bucket,
            "files": manifest_files,
        }
        manifest_key = f"raw/_manifests/{batch_id}.json"
        client.put_object(
            Bucket=bucket,
            Key=manifest_key,
            Body=json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8"),
            ContentType="application/json; charset=utf-8",
        )
        print(f"\nConcluído: {len(manifest_files)} arquivos registrados em s3://{bucket}/raw/")
        print(f"Manifesto: s3://{bucket}/{manifest_key}")
        return 0
    except (FileNotFoundError, ValueError) as error:
        print(f"Erro de configuração ou origem: {error}", file=sys.stderr)
    except NoCredentialsError:
        print("Credenciais do MinIO não foram encontradas no .env.", file=sys.stderr)
    except ClientError as error:
        code = error.response.get("Error", {}).get("Code", "erro S3")
        print(
            f"Falha ao acessar o bucket '{bucket}' ({code}). Confira se o MinIO está ativo, "
            "se o bucket existe e se as credenciais em .env estão corretas.",
            file=sys.stderr,
        )
    except BotoCoreError as error:
        print(f"Falha de comunicação com o MinIO: {error}", file=sys.stderr)
    except csv.Error as error:
        print(f"Erro ao ler um CSV: {error}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
