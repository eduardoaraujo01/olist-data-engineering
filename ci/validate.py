"""Valida configuração e DAG sem carregar .env ou executar o pipeline."""
import os
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
# Valores públicos e fictícios, exclusivos da validação.
ENV = {**os.environ, **dict.fromkeys([
    "MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD", "POSTGRES_PASSWORD",
    "AIRFLOW_DB_PASSWORD", "AIRFLOW_FERNET_KEY", "AIRFLOW_API_SECRET_KEY",
    "AIRFLOW_JWT_SECRET",
], "ci-placeholder-only"), "POSTGRES_USER": "ci", "POSTGRES_DB": "ci",
    "OLIST_SOURCE_DIR": "/tmp/olist-ci-source"}


def run(*args):
    timeout = 900 if args[1] == "build" else 120
    subprocess.run(args, cwd=ROOT, env=ENV, check=True, timeout=timeout)


if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in {"compose", "dag"}:
        raise SystemExit("Uso: python ci/validate.py compose|dag")
    if sys.argv[1] == "compose":
        run("docker", "compose", "--env-file", os.devnull, "--profile", "jobs", "--profile", "airflow", "config", "--quiet")
    else:
        run("docker", "build", "-f", "docker/airflow/Dockerfile", "-t", "olist-airflow-ci:local", ".")
        run("docker", "run", "--rm", "--network", "none", "--entrypoint", "python",
            "-e", "MINIO_ROOT_USER=ci", "-e", "MINIO_ROOT_PASSWORD=ci-placeholder-only",
            "-e", "POSTGRES_PASSWORD=ci-placeholder-only",
            "--mount", f"type=bind,source={ROOT / 'airflow/dags'},target=/opt/airflow/dags,readonly",
            "--mount", f"type=bind,source={ROOT / 'ci/check_dag.py'},target=/tmp/check_dag.py,readonly",
            "olist-airflow-ci:local", "/tmp/check_dag.py")
