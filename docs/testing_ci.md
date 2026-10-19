# Testes e integração contínua: guia da implementação

O workflow automatiza verificações de contratos locais em push e pull request. Não executa o pipeline real.

## Como executar

Os comandos completos estão na seção “Testes automatizados e CI” do README. Use Python 3.12; `py -3.12` exige essa versão instalada no launcher do Windows. O ambiente `.venv` criado nesta implementação usa Python 3.12.14. Para repetir as verificações sem recriá-lo:

```powershell
Set-Location C:\Users\Duh\Documents\olist-data-engeneering
.\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
.\.venv\Scripts\python.exe -m pytest
.\.venv\Scripts\python.exe -m ruff check .
.\.venv\Scripts\python.exe ci/validate.py compose
.\.venv\Scripts\python.exe ci/validate.py dag
```

## Como funciona cada arquivo

- `requirements-dev.txt`: reutiliza as dependências de produção e acrescenta versões fixas de pytest e Ruff. As faixas das bibliotecas de produção são preservadas; não é um lockfile de todas as dependências transitivas.
- `pyproject.toml`: concentra descoberta/importação dos testes e regras estáticas. Ruff procura erros de sintaxe e construções inválidas com escopo restrito, sem introduzir uma reforma de estilo.
- `tests/test_batch.py`: testa identidade do lote, ordem irrelevante, metadados de execução ignorados, mudança de nome/tamanho/hash/conteúdo e exigência dos nove arquivos. `tmp_path` garante arquivos descartáveis.
- `tests/test_quality.py`: carrega a configuração real a cada teste; fixtures novas permitem alterar observe sem contaminar outro caso. Relatórios e máscaras sintéticos exercitam decisões, fronteiras e contagem distinta. A máscara de 10.000 linhas representa o limite real de 0,01% com uma única ocorrência. Horários não são fixados nem comparados.
- `tests/test_curated.py`: usa cliente e leituras/gravações substituídos, mantendo transformações e política reais. Um pedido cujo cliente inexiste deve gerar manifesto de erro e impedir qualquer upload de Parquet. `load_dotenv` é substituído para não ler configuração real.
- `ci/check_dag.py`: importa o arquivo real com Airflow e DockerOperator instalados. Confere grafo, execução manual, limite de concorrência, montagem de origem somente leitura e renderização de BATCH_ID. Não chama `execute()` de tarefa. O contexto XCom sintético substitui apenas o valor de retorno da ingestão.
- `ci/validate.py`: centraliza os comandos para uso local e CI. Sobrescreve campos necessários com valores fictícios, ignora `.env` no Compose e não imprime a configuração. A importação roda sem rede, sem socket Docker e sem CSVs de origem; somente DAG e script de validação são montados. O build pode baixar imagem e dependências, mas não executa tarefas do pipeline. Há prazo máximo de 15 minutos para build e dois minutos para os demais comandos.
- `.github/workflows/ci.yml`: separa testes/Compose da importação Airflow, usa Python 3.12 e concede somente leitura do repositório. Não precisa cadastrar secrets. Os limites de tempo impedem jobs indefinidos. O cache guarda downloads de pip, não dados do pipeline.
- `.gitignore`: exclui bytecode e caches de pytest/Ruff, além da exclusão de `.venv` já existente.
- `README.md`: documenta instalação, comandos, escopo e leitura dos checks; remove CI da lista de evoluções futuras.
- `docs/testing_ci.md`: este guia reúne o texto integral dos arquivos novos executáveis/configurações, para estudo e revisão.

## Validação observada nesta implementação

25 testes passaram no Windows com Python 3.12.14. Ruff e `git diff --check` passaram. A configuração Compose foi validada com todos os perfis e valores fictícios; também foi repetida pelo executável independente docker-compose, sem daemon.

A validação da DAG em contêiner ficou pendente: Docker Desktop falhou ao iniciar o componente local dockerInference, antes da disponibilização do mecanismo Linux. Nenhuma tarefa Airflow real foi executada. A implementação do check não deve ser confundida com evidência de que ele passou. Antes de concluir a entrega, execute `python ci/validate.py dag` com Docker funcional ou obtenha autorização explícita para enviar essa verificação ainda pendente.

## Como ler o GitHub

Abra Actions → Pipeline checks → execução correspondente ao commit. O job tests mostra instalação, pytest, Ruff e Compose; dag mostra build e importação. Verde significa que aquele job passou. Em vermelho, abra a primeira etapa com erro e procure a assertiva ou mensagem causal. Em um pull request, esses jobs aparecem em Checks. Um push só dispara o workflow depois de o arquivo chegar ao GitHub; resultado local não é resultado remoto.

## Código completo

### requirements-dev.txt

```text
-r requirements.txt
pytest==8.3.5
ruff==0.11.13

```

### pyproject.toml

```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
pythonpath = ["."]
addopts = "-ra"

[tool.ruff]
target-version = "py312"

[tool.ruff.lint]
select = ["E9", "F63", "F7", "F82"]

```

### tests/test_batch.py

```python
import hashlib

import pytest

from ingestion import ingest_raw


def test_batch_is_order_independent_and_ignores_run_metadata():
    files = [
        {"name": "a.csv", "size_bytes": 4, "sha256": "abc"},
        {"name": "b.csv", "size_bytes": 8, "sha256": "def"},
    ]
    batch = ingest_raw.build_batch_id(files)
    assert batch.startswith("b_") and len(batch) == 66
    assert set(batch[2:]) <= set("0123456789abcdef")
    assert ingest_raw.build_batch_id(list(reversed(files))) == batch
    assert ingest_raw.build_batch_id([dict(f, rows=99, run_id="new") for f in files]) == batch


@pytest.mark.parametrize("field,value", [("name", "renamed.csv"), ("size_bytes", 5), ("sha256", "changed")])
def test_batch_changes_when_source_identity_changes(field, value):
    original = {"name": "a.csv", "size_bytes": 4, "sha256": "abc"}
    assert ingest_raw.build_batch_id([original]) != ingest_raw.build_batch_id([dict(original, **{field: value})])


def test_inspect_csv_with_bom_and_multiline_field(tmp_path):
    path = tmp_path / "sample.csv"
    content = '\ufeffid,text\n1,"two\nlines"\n2,ok\n'.encode("utf-8")
    path.write_bytes(content)
    result = ingest_raw.inspect_file(path)
    assert result == {"name": "sample.csv", "rows": 2, "size_bytes": len(content), "sha256": hashlib.sha256(content).hexdigest()}


def test_empty_csv_is_rejected(tmp_path):
    path = tmp_path / "empty.csv"
    path.write_bytes(b"")
    with pytest.raises(ValueError, match="vazio"):
        ingest_raw.inspect_file(path)


def test_all_nine_sources_are_required(tmp_path, monkeypatch):
    monkeypatch.setattr(ingest_raw, "SOURCE_DIR", tmp_path)
    for name in ingest_raw.EXPECTED_FILES:
        (tmp_path / name).write_text("id\n1\n", encoding="utf-8")
    assert len(ingest_raw.source_files()) == 9
    (tmp_path / ingest_raw.EXPECTED_FILES[0]).unlink()
    with pytest.raises(FileNotFoundError, match="Faltam CSVs"):
        ingest_raw.source_files()


def test_changed_csv_content_changes_batch_even_with_same_size(tmp_path):
    path = tmp_path / "sample.csv"
    path.write_bytes(b"id\n1\n")
    before = ingest_raw.inspect_file(path)
    path.write_bytes(b"id\n2\n")
    after = ingest_raw.inspect_file(path)
    assert before["size_bytes"] == after["size_bytes"]
    assert ingest_raw.build_batch_id([before]) != ingest_raw.build_batch_id([after])

```

### tests/test_quality.py

```python
from pathlib import Path

import pandas as pd
import pytest

from processing.quality_policy import QualityPolicyError, evaluate_quality, load_policy, split_frame


@pytest.fixture
def policy():
    return load_policy(Path(__file__).resolve().parents[1] / "config/data_quality_v2.json")


def evaluate(policy, checks=(), stage="curated", rows=10000, masks=None):
    return evaluate_quality(
        stage=stage, policy=policy,
        reports=[{"table": "orders", "rows_read": rows, "issues": [
            {"check": check, "column": "id", "affected_rows": 1} for check in checks
        ]}], row_masks=masks,
    )


def test_clean_batch_passes(policy):
    assert evaluate(policy)["manifest"]["decision"] == "pass"


@pytest.mark.parametrize("check", ["missing_natural_key", "duplicate_natural_key", "order_without_customer_id", "customer_reference_not_found", "unknown_check"])
def test_invalid_keys_and_unmapped_checks_block(policy, check):
    assert evaluate(policy, [check])["manifest"]["decision"] == "blocked"


@pytest.mark.parametrize("check", ["timestamp_order_inversion", "delivery_after_estimate", "unparseable_date"])
def test_chronology_warnings_do_not_block(policy, check):
    assert evaluate(policy, [check])["manifest"]["decision"] == "pass_with_warnings"


def test_observe_records_would_block_without_quarantine(policy):
    policy["mode"] = "observe"
    result = evaluate(policy, ["missing_natural_key"])
    assert result["manifest"]["decision"] == "observe_only"
    assert result["manifest"]["would_decision"] == "blocked"
    assert result["quarantine_masks_by_table"] == {}


@pytest.mark.parametrize("rows,decision", [(10000, "pass"), (9999, "blocked")])
def test_quarantine_threshold_boundary(policy, rows, decision):
    result = evaluate(policy, ["duplicate_candidate_key"], stage="processed", rows=rows,
                      masks={("orders", "duplicate_candidate_key", "id"): [True] + [False] * (rows - 1)})
    assert result["manifest"]["decision"] == decision
    assert result["manifest"]["rows_by_table"]["orders"]["quarantined_distinct_rows"] == 1


def test_table_limit_counts_union_of_rules(policy):
    checks = ["duplicate_candidate_key", "orphan_foreign_key"]
    overlapping = {( "orders", check, "id"): [True] + [False] * 9999 for check in checks}
    result = evaluate(policy, checks, stage="processed", masks=overlapping)
    assert result["manifest"]["decision"] == "pass"
    assert result["manifest"]["rows_by_table"]["orders"]["approved_rows"] == 9999
    overlapping[("orders", checks[1], "id")] = [False, True] + [False] * 9998
    result = evaluate(policy, checks, stage="processed", masks=overlapping)
    assert result["manifest"]["decision"] == "blocked"
    assert all(rule["outcome"] != "limit_exceeded" for rule in result["manifest"]["rules"])


@pytest.mark.parametrize("mask", [None, [True]])
def test_quarantine_requires_complete_masks(policy, mask):
    masks = {} if mask is None else {("orders", "duplicate_candidate_key", "id"): mask}
    with pytest.raises(QualityPolicyError, match="máscara"):
        evaluate(policy, ["duplicate_candidate_key"], stage="processed", masks=masks)


def test_split_preserves_source_and_lineage():
    frame = pd.DataFrame({"id": ["a", "b", "c"]}, index=[10, 20, 30])
    original = frame.copy(deep=True)
    approved, rejected = split_frame(frame, masks_by_rule={"r1": [False, True, False], "r2": [False, True, True]},
                                    batch_id="b_test", run_id="run", source_table="orders")
    assert approved["id"].tolist() == ["a"]
    assert rejected["id"].tolist() == ["b", "c"]
    assert rejected["_rule_ids"].tolist() == [["r1", "r2"], ["r2"]]
    assert rejected["_batch_id"].tolist() == ["b_test", "b_test"]
    assert rejected["_run_id"].tolist() == ["run", "run"]
    pd.testing.assert_frame_equal(frame, original)

```

### tests/test_curated.py

```python
import copy
import importlib
from pathlib import Path
from unittest.mock import Mock

import pandas as pd


def test_orphan_blocks_publication_and_records_failed_manifest(monkeypatch):
    # O script usa import local porque também roda diretamente como arquivo.
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[1] / "processing"))
    curated = importlib.import_module("processing.build_curated")
    monkeypatch.setenv("BATCH_ID", "b_" + "a" * 64)
    monkeypatch.setattr(curated, "load_dotenv", lambda *args: None)
    monkeypatch.setattr(curated, "make_s3_client", lambda: Mock())
    customers = pd.DataFrame([["c1", "u1", "01000", "City", "SP"]], columns=curated.CUSTOMER_COLUMNS)
    orders = pd.DataFrame([["o1", "missing_customer", "delivered",
                           "2018-01-01", "2018-01-02", "2018-01-03", "2018-01-04", "2018-01-05"]],
                          columns=curated.ORDER_COLUMNS)

    def read(client, bucket, key, columns, directory):
        return (customers.copy() if key.endswith("customers.parquet") else orders.copy()), "synthetic-hash"

    manifests = []

    def record(client, bucket, manifest, batch_id, run_id):
        manifests.append(copy.deepcopy(manifest))
        return "synthetic-manifest"

    publish = Mock()
    monkeypatch.setattr(curated, "read_processed_parquet", read)
    monkeypatch.setattr(curated, "upload_manifest", record)
    monkeypatch.setattr(curated, "upload_parquet", publish)
    assert curated.main() == 1
    publish.assert_not_called()
    assert manifests[-1]["status"] == "failed"
    assert manifests[-1]["quality"]["decision"] == "blocked"
    assert manifests[-1]["error"]["type"] == "CuratedValidationError"

```

### ci/check_dag.py

```python
"""Importa a DAG com Airflow real; nunca executa operadores."""
import runpy


dag = runpy.run_path("/opt/airflow/dags/olist_batch_v2.py")["dag"]
assert dag.dag_id == "olist_batch_v2"
assert dag.schedule is None
assert dag.catchup is False
assert dag.max_active_runs == 1
chain = ["ingestion", "processing", "curated", "warehouse_load"]
assert set(dag.task_ids) == set(chain)
for index, name in enumerate(chain):
    task = dag.get_task(name)
    assert task.downstream_task_ids == ({chain[index + 1]} if index < 3 else set())
    assert task.upstream_task_ids == ({chain[index - 1]} if index else set())
    assert task.do_xcom_push is (index == 0)
    if index:
        task.render_template_fields({"ti": type("Context", (), {"xcom_pull": lambda self, task_ids: "b_" + "a" * 64 if task_ids == "ingestion" else None})()})
        assert task.environment["BATCH_ID"] == "b_" + "a" * 64
    else:
        assert task.xcom_all is False
        assert task.mounts[0]["ReadOnly"] is True
print("DAG importada; sequência, execução manual e contrato XCom validados.")

```

### ci/validate.py

```python
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

```

### .github/workflows/ci.yml

```yaml
name: Pipeline checks

on: [push, pull_request]

permissions:
  contents: read

jobs:
  tests:
    runs-on: ubuntu-latest
    timeout-minutes: 15
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.12'
          cache: pip
          cache-dependency-path: |
            requirements.txt
            requirements-dev.txt
      - run: python -m pip install -r requirements-dev.txt
      - run: python -m pytest
      - run: python -m ruff check .
      - run: python ci/validate.py compose
  dag:
    runs-on: ubuntu-latest
    timeout-minutes: 20
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.12'
      - run: python ci/validate.py dag

```
