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
