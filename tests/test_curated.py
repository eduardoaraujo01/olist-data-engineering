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
