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
