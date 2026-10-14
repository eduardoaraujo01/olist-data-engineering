from __future__ import annotations

import csv
import uuid
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

def build_batch_id(files: list[dict[str, object]]) -> str:
    """Create a stable identifier from the complete source file set."""
    ordered_files = sorted(files, key=lambda item: str(item["name"]))
    batch_description = [
        {
            "name": str(item["name"]),
            "size_bytes": int(item["size_bytes"]),
            "sha256": str(item["sha256"]),
        }
        for item in ordered_files
    ]

    canonical_content = json.dumps(
        batch_description,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")

    digest = hashlib.sha256(canonical_content).hexdigest()
    return f"b_{digest}"


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

def write_manifest(
        client,
        bucket: str,
        object_key: str,
        manifest: dict[str, object],
) -> None:
    body = json.dumps(
        manifest,
        ensure_ascii=False,
        indent=2,
    ).encode("utf-8")

    client.put_object(
        Bucket=bucket,
        Key=object_key,
        Body=body,
        ContentType="application/json; charset=utf-8",
    )

def main() -> int:
    load_dotenv(PROJECT_ROOT / ".env")
    bucket = os.getenv("MINIO_BUCKET", "olist-data-lake")

    run_id = uuid.uuid4().hex
    started_at = datetime.now(timezone.utc)
    client = None
    manifest = None
    manifest_key = None

    try:
        files = source_files()
        manifest_files = [inspect_file(path) for path in files]
        batch_id = build_batch_id(manifest_files)

        for item in manifest_files:
            item["object_key"] = (
                f"raw/batches/{batch_id}/{item['name']}"
            )

        manifest_key = (
            f"control/manifests/ingestion/"
            f"{batch_id}/{run_id}.json"
        )

        manifest = {
            "contract_version": "2.0",
            "batch_id": batch_id,
            "run_id": run_id,
            "stage": "ingestion",
            "status": "running",
            "started_at_utc": started_at.isoformat(),
            "finished_at_utc": None,
            "bucket": bucket,
            "files": manifest_files,
            "error": None,
        }

        client = make_s3_client()
        client.head_bucket(Bucket=bucket)
        write_manifest(client, bucket, manifest_key, manifest)

        for path, item in zip(files, manifest_files, strict=True):
            if object_has_same_checksum(client, bucket, item):
                item["result"] = "unchanged"
                print(
                    f"[SEM ALTERAÇÃO] {path.name} — "
                    "já existe com o mesmo SHA-256 neste lote"
                )
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
            print(
                f"[ENVIADO] {path.name} — "
                f"{size_mb:.1f} MiB, {item['rows']} linhas"
            )

        manifest["status"] = "succeeded"
        manifest["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
        write_manifest(client, bucket, manifest_key, manifest)

        print(f"\nConcluído: lote {batch_id}")
        print(
            f"Arquivos registrados: {len(manifest_files)}"
        )
        print(f"Manifesto: s3://{bucket}/{manifest_key}")
        return 0

    except (
            FileNotFoundError,
            ValueError,
            NoCredentialsError,
            ClientError,
            BotoCoreError,
            csv.Error,
    ) as error:
        if manifest is not None:
            manifest["status"] = "failed"
            manifest["finished_at_utc"] = datetime.now(
                timezone.utc
            ).isoformat()
            manifest["error"] = {
                "type": type(error).__name__,
                "message": str(error),
            }

            if client is not None and manifest_key is not None:
                try:
                    write_manifest(
                        client,
                        bucket,
                        manifest_key,
                        manifest,
                    )
                except Exception as manifest_error:
                    print(
                        "Também não foi possível atualizar o manifesto: "
                        f"{manifest_error}",
                        file=sys.stderr,
                    )

        if isinstance(error, (FileNotFoundError, ValueError)):
            print(
                f"Erro de configuração ou origem: {error}",
                file=sys.stderr,
            )
        elif isinstance(error, NoCredentialsError):
            print(
                "Credenciais do MinIO não foram encontradas no .env.",
                file=sys.stderr,
            )
        elif isinstance(error, ClientError):
            code = error.response.get("Error", {}).get("Code", "erro S3")
            print(
                f"Falha ao acessar o bucket '{bucket}' ({code}). "
                "Confira se o MinIO está ativo, se o bucket existe "
                "e se as credenciais em .env estão corretas.",
                file=sys.stderr,
            )
        elif isinstance(error, BotoCoreError):
            print(
                f"Falha de comunicação com o MinIO: {error}",
                file=sys.stderr,
            )
        else:
            print(f"Erro ao ler um CSV: {error}", file=sys.stderr)

        return 1


if __name__ == "__main__":
    raise SystemExit(main())
