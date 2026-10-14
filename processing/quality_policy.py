from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence
import json

ACTIONS = {
    "fail_batch",
    "quarantine_rows",
    "keep_and_warn",
}

MODES = {
    "observe",
    "enforce",
}

MANIFEST_DECISIONS = {
    "observe_only",
    "pass",
    "pass_with_warnings",
    "blocked",
}


class QualityPolicyError(ValueError):
    """Configuração ou avaliação de qualidade inválida."""


def load_policy(path: str | Path) -> dict[str, Any]:
    """Carrega e valida a política JSON de qualidade."""
    policy_path = Path(path)

    try:
        with policy_path.open("r", encoding="utf-8") as file:
            policy = json.load(file)
    except FileNotFoundError as error:
        raise QualityPolicyError(
            f"Arquivo de política não encontrado: {policy_path}"
        ) from error
    except json.JSONDecodeError as error:
        raise QualityPolicyError(
            f"JSON inválido em {policy_path}: {error}"
        ) from error

    validate_policy(policy)
    return policy


def validate_policy(policy: Mapping[str, Any]) -> None:
    """Valida os campos e as combinações permitidas na política."""
    if not isinstance(policy, Mapping):
        raise QualityPolicyError("A política deve ser um objeto JSON.")

    if not policy.get("ruleset_version"):
        raise QualityPolicyError("Informe ruleset_version.")

    mode = policy.get("mode")
    if mode not in MODES:
        raise QualityPolicyError(
            f"mode deve ser um destes valores: {sorted(MODES)}."
        )

    unmapped_action = policy.get("unmapped_rule_action", "fail_batch")
    if unmapped_action != "fail_batch":
        raise QualityPolicyError(
            "unmapped_rule_action deve ser 'fail_batch'. "
            "Regras desconhecidas não podem passar silenciosamente."
        )

    table_limits = policy.get("table_limits", {})
    if not isinstance(table_limits, Mapping):
        raise QualityPolicyError("table_limits deve ser um objeto por tabela.")

    for table, table_limit in table_limits.items():
        if not isinstance(table, str) or not table.strip():
            raise QualityPolicyError("Toda entrada de table_limits precisa de nome.")
        if not isinstance(table_limit, Mapping):
            raise QualityPolicyError(
                f"table_limits.{table} deve ser um objeto."
            )
        maximum_rate = table_limit.get("max_quarantine_rate_pct")
        if maximum_rate is not None:
            _validate_percentage(f"table_limits.{table}", maximum_rate)
        elif mode == "enforce":
            raise QualityPolicyError(
                f"table_limits.{table} precisa de max_quarantine_rate_pct "
                "em enforce."
            )

    rules = policy.get("rules")
    if not isinstance(rules, list) or not rules:
        raise QualityPolicyError("rules deve ser uma lista não vazia.")

    rule_ids: set[str] = set()

    for rule in rules:
        if not isinstance(rule, Mapping):
            raise QualityPolicyError("Cada regra deve ser um objeto JSON.")

        rule_id = rule.get("rule_id")
        stage = rule.get("stage")
        checks = rule.get("checks")
        action = rule.get("action")
        severity = rule.get("severity")

        if not isinstance(rule_id, str) or not rule_id.strip():
            raise QualityPolicyError("Toda regra precisa de rule_id.")
        if rule_id in rule_ids:
            raise QualityPolicyError(f"rule_id duplicado: {rule_id}")
        rule_ids.add(rule_id)

        if not isinstance(stage, str) or not stage.strip():
            raise QualityPolicyError(
                f"A regra {rule_id} precisa de stage."
            )

        if (
                not isinstance(checks, list)
                or not checks
                or any(not isinstance(check, str) for check in checks)
        ):
            raise QualityPolicyError(
                f"A regra {rule_id} precisa de checks, "
                "uma lista de identificadores emitidos pelo código."
            )

        if action not in ACTIONS:
            raise QualityPolicyError(
                f"Ação inválida na regra {rule_id}: {action!r}."
            )

        if severity not in {"error", "warning", "info"}:
            raise QualityPolicyError(
                f"severity inválida na regra {rule_id}."
            )

        tables = rule.get("tables", [])
        if not isinstance(tables, list) or any(
                not isinstance(table, str) for table in tables
        ):
            raise QualityPolicyError(
                f"tables da regra {rule_id} deve ser uma lista de nomes."
            )

        if action == "fail_batch":
            maximum = rule.get("max_affected_rows")
            if (
                    isinstance(maximum, bool)
                    or not isinstance(maximum, int)
                    or maximum < 0
            ):
                raise QualityPolicyError(
                    f"A regra {rule_id} precisa de "
                    "max_affected_rows >= 0."
                )

        if action == "quarantine_rows":
            maximum_rate = rule.get("max_quarantine_rate_pct")
            if maximum_rate is not None:
                _validate_percentage(rule_id, maximum_rate)

            if mode == "enforce" and maximum_rate is None:
                raise QualityPolicyError(
                    f"A regra {rule_id} não pode ser aplicada em enforce "
                    "sem max_quarantine_rate_pct."
                )
            if mode == "enforce":
                if not tables:
                    raise QualityPolicyError(
                        f"A regra {rule_id} precisa declarar tables em enforce "
                        "para validar os limites agregados."
                    )
                for table in tables:
                    if table not in table_limits:
                        raise QualityPolicyError(
                            f"A tabela {table} usada pela regra {rule_id} "
                            "não tem limite agregado em table_limits."
                        )

        if action == "keep_and_warn" and mode == "enforce":
            # Avisos são permitidos em enforce, mas continuam sem bloquear.
            pass


def _validate_percentage(rule_id: str, value: Any) -> None:
    if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or value < 0
            or value > 100
    ):
        raise QualityPolicyError(
            f"max_quarantine_rate_pct da regra {rule_id} "
            "deve estar entre 0 e 100."
        )


def _mask_to_list(mask: Any) -> list[bool] | None:
    """Converte uma máscara interna em lista de booleanos."""
    if mask is None:
        return None

    if hasattr(mask, "tolist"):
        mask = mask.tolist()

    if not isinstance(mask, Sequence) or isinstance(mask, (str, bytes)):
        raise QualityPolicyError(
            "A máscara de quarentena deve ser uma sequência booleana."
        )

    return [bool(value) for value in mask]


def _union_masks(masks: list[list[bool]], expected_length: int) -> list[bool]:
    if not masks:
        return [False] * expected_length

    for mask in masks:
        if len(mask) != expected_length:
            raise QualityPolicyError(
                "A máscara de qualidade tem tamanho diferente "
                "da quantidade de linhas da tabela."
            )

    return [
        any(mask[position] for mask in masks)
        for position in range(expected_length)
    ]


def evaluate_quality(
        *,
        stage: str,
        reports: list[Mapping[str, Any]],
        policy: Mapping[str, Any],
        row_masks: Mapping[
                       tuple[str, str, str],
                       Sequence[bool] | Any,
                   ] | None = None,
) -> dict[str, Any]:
    """
    Avalia as ocorrências de uma etapa e monta o bloco JSON para o manifesto.

    Cada report deve ter table, rows_read e issues. Para ações de quarentena,
    row_masks contém máscaras indexadas por (tabela, check, coluna).
    """
    validate_policy(policy)

    mode = str(policy["mode"])
    row_masks = row_masks or {}
    table_limits = policy.get("table_limits", {})
    rules = [rule for rule in policy["rules"] if rule["stage"] == stage]

    normalized_reports: list[dict[str, Any]] = []
    for report in reports:
        table = report.get("table")
        rows_read = report.get("rows_read")
        issues = report.get("issues", [])

        if not isinstance(table, str) or not table:
            raise QualityPolicyError("Cada relatório precisa do campo table.")
        if (
                isinstance(rows_read, bool)
                or not isinstance(rows_read, int)
                or rows_read < 0
        ):
            raise QualityPolicyError(f"rows_read inválido para a tabela {table}.")
        if not isinstance(issues, list):
            raise QualityPolicyError(f"issues deve ser uma lista para a tabela {table}.")

        normalized_reports.append({
            "table": table,
            "rows_read": rows_read,
            "issues": issues,
        })

    rules_by_check: dict[str, list[Mapping[str, Any]]] = {}
    for rule in rules:
        for check in rule["checks"]:
            rules_by_check.setdefault(check, []).append(rule)

    unmapped_findings: list[dict[str, Any]] = []
    rule_summaries: list[dict[str, Any]] = []
    quarantine_masks_by_table: dict[str, dict[str, list[bool]]] = {}
    candidate_masks_by_table: dict[str, dict[str, list[bool]]] = {}
    tables_missing_masks: set[str] = set()
    table_totals: dict[str, dict[str, Any]] = {}
    table_limit_summaries: list[dict[str, Any]] = []

    has_blocking_failure = False
    has_warning = False
    needs_configuration = False

    for report in normalized_reports:
        table = report["table"]
        rows_read = report["rows_read"]
        issues = report["issues"]
        table_totals[table] = {
            "input_rows": rows_read,
            "approved_rows": rows_read,
            "quarantined_distinct_rows": 0,
        }

        for issue in issues:
            check = issue.get("check")
            if not isinstance(check, str):
                raise QualityPolicyError(
                    f"Ocorrência sem check na tabela {table}."
                )
            matching_rules = [
                rule for rule in rules_by_check.get(check, [])
                if not rule.get("tables") or table in rule["tables"]
            ]
            if not matching_rules:
                unmapped_findings.append({
                    "table": table,
                    "check": check,
                    "column": issue.get("column"),
                    "affected_rows": int(issue.get("affected_rows", 0)),
                })
                if mode == "enforce":
                    has_blocking_failure = True
                continue
            if len(matching_rules) > 1:
                raise QualityPolicyError(
                    f"O check {check!r} da tabela {table!r} "
                    "está associado a mais de uma regra."
                )

        for rule in rules:
            if rule.get("tables") and table not in rule["tables"]:
                continue

            matching_issues = [
                issue for issue in issues
                if issue.get("check") in rule["checks"]
            ]
            finding_count = len(matching_issues)
            affected_sum = sum(
                int(issue.get("affected_rows", 0))
                for issue in matching_issues
            )

            masks_for_rule: list[list[bool]] = []
            missing_masks = False
            if rule["action"] == "quarantine_rows":
                for issue in matching_issues:
                    mask_key = (
                        table,
                        str(issue.get("check", "")),
                        str(issue.get("column", "")),
                    )
                    mask = _mask_to_list(row_masks.get(mask_key))
                    if mask is None:
                        missing_masks = True
                    else:
                        masks_for_rule.append(mask)

            if masks_for_rule:
                affected_rows = sum(_union_masks(masks_for_rule, rows_read))
            else:
                affected_rows = affected_sum

            rate = (affected_rows / rows_read * 100) if rows_read else 0.0
            outcome = "passed"
            limit: int | float | None = None

            if rule["action"] == "fail_batch":
                limit = int(rule["max_affected_rows"])
                if affected_rows > limit:
                    outcome = "limit_exceeded"
                    has_blocking_failure = True

            elif rule["action"] == "quarantine_rows":
                limit = rule.get("max_quarantine_rate_pct")
                if affected_rows == 0:
                    outcome = "passed"
                elif limit is None:
                    outcome = "threshold_not_configured"
                    needs_configuration = True
                elif rate > float(limit):
                    outcome = "limit_exceeded"
                    has_blocking_failure = True
                else:
                    outcome = (
                        "would_quarantine" if mode == "observe"
                        else "quarantined"
                    )

                if affected_rows > 0 and missing_masks:
                    tables_missing_masks.add(table)
                if mode == "enforce" and affected_rows > 0 and missing_masks:
                    raise QualityPolicyError(
                        f"A regra {rule['rule_id']} precisa de máscaras "
                        "por linha para colocar registros em quarentena."
                    )

                if masks_for_rule:
                    union_mask = _union_masks(masks_for_rule, rows_read)
                    candidate_masks_by_table.setdefault(table, {})[
                        rule["rule_id"]
                    ] = union_mask
                    if mode == "enforce":
                        quarantine_masks_by_table.setdefault(table, {})[
                            rule["rule_id"]
                        ] = union_mask

            elif rule["action"] == "keep_and_warn" and affected_rows > 0:
                outcome = "warning"
                has_warning = True

            rule_summaries.append({
                "rule_id": rule["rule_id"],
                "stage": rule["stage"],
                "table": table,
                "severity": rule["severity"],
                "action": rule["action"],
                "finding_count": finding_count,
                "affected_rows": affected_rows,
                "denominator_rows": rows_read,
                "affected_rate_pct": round(rate, 6),
                "limit": limit,
                "outcome": outcome,
                "row_masks_available": not missing_masks,
            })

    for report in normalized_reports:
        table = report["table"]
        input_rows = report["rows_read"]
        quarantine_rules = [
            rule for rule in rules
            if rule["action"] == "quarantine_rows"
            and (not rule.get("tables") or table in rule["tables"])
        ]
        if not quarantine_rules:
            continue

        configured_limit = table_limits.get(table, {}).get(
            "max_quarantine_rate_pct"
        )
        masks_by_rule = candidate_masks_by_table.get(table, {})
        distinct_rows = (
            sum(_union_masks(list(masks_by_rule.values()), input_rows))
            if masks_by_rule else 0
        )
        rate = distinct_rows / input_rows * 100 if input_rows else 0.0

        if table in tables_missing_masks:
            outcome = "row_masks_unavailable"
            has_blocking_failure = True
        elif configured_limit is None:
            outcome = "threshold_not_configured"
            needs_configuration = True
        elif rate > float(configured_limit):
            outcome = "limit_exceeded" if mode == "enforce" else "would_exceed"
            has_blocking_failure = True
        else:
            outcome = "passed" if mode == "enforce" else "would_pass"

        if mode == "enforce":
            table_totals[table]["quarantined_distinct_rows"] = distinct_rows
            table_totals[table]["approved_rows"] = input_rows - distinct_rows

        table_limit_summaries.append({
            "table": table,
            "input_rows": input_rows,
            "quarantined_distinct_rows": distinct_rows,
            "quarantined_rate_pct": round(rate, 6),
            "max_quarantine_rate_pct": configured_limit,
            "outcome": outcome,
            "row_masks_available": table not in tables_missing_masks,
        })

    if mode == "observe":
        if needs_configuration:
            would_decision = "needs_configuration"
        elif unmapped_findings:
            would_decision = "needs_rule_mapping"
        elif has_blocking_failure:
            would_decision = "blocked"
        elif has_warning:
            would_decision = "pass_with_warnings"
        else:
            would_decision = "pass"
        decision = "observe_only"
    else:
        if unmapped_findings or has_blocking_failure:
            decision = "blocked"
        elif has_warning:
            decision = "pass_with_warnings"
        else:
            decision = "pass"
        would_decision = None

    manifest_block = {
        "ruleset_version": policy["ruleset_version"],
        "mode": mode,
        "decision": decision,
        "would_decision": would_decision,
        "evaluated_at_utc": datetime.now(timezone.utc).isoformat(),
        "rows_by_table": table_totals,
        "table_limits": table_limit_summaries,
        "rules": rule_summaries,
        "unmapped_findings": unmapped_findings,
        "quarantine_objects": [],
    }

    return {
        "manifest": manifest_block,
        "quarantine_masks_by_table": quarantine_masks_by_table,
    }


def split_frame(
        frame: Any,
        *,
        masks_by_rule: Mapping[str, Sequence[bool]],
        batch_id: str,
        run_id: str,
        source_table: str,
) -> tuple[Any, Any]:
    """
    Separa um DataFrame em linhas aprovadas e em quarentena.

    Retorna (approved_frame, quarantined_frame). A função é chamada pelo
    pipeline somente em modo enforce, após avaliar os limites.
    """
    try:
        import pandas as pd
    except ImportError as error:
        raise QualityPolicyError(
            "split_frame requer pandas, já usado pelo pipeline."
        ) from error

    if not masks_by_rule:
        return frame.copy(), frame.iloc[0:0].copy()

    row_count = len(frame)
    normalized_masks: dict[str, list[bool]] = {}

    for rule_id, raw_mask in masks_by_rule.items():
        mask = _mask_to_list(raw_mask)
        if mask is None or len(mask) != row_count:
            raise QualityPolicyError(
                f"Máscara inválida para a regra {rule_id}."
            )
        normalized_masks[rule_id] = mask

    quarantine_mask = _union_masks(
        list(normalized_masks.values()),
        row_count,
    )

    rule_ids_by_row: list[list[str]] = [[] for _ in range(row_count)]
    for rule_id, mask in normalized_masks.items():
        for position, affected in enumerate(mask):
            if affected:
                rule_ids_by_row[position].append(rule_id)

    quarantined = frame.loc[quarantine_mask].copy()
    approved = frame.loc[[not value for value in quarantine_mask]].copy()

    reserved_columns = {
        "_batch_id",
        "_run_id",
        "_source_table",
        "_rule_ids",
        "_quarantined_at_utc",
    }
    collisions = reserved_columns.intersection(frame.columns)
    if collisions:
        raise QualityPolicyError(
            "A origem já contém colunas reservadas da quarentena: "
            f"{sorted(collisions)}"
        )

    quarantined_positions = [
        position
        for position, affected in enumerate(quarantine_mask)
        if affected
    ]

    quarantined["_batch_id"] = batch_id
    quarantined["_run_id"] = run_id
    quarantined["_source_table"] = source_table
    quarantined["_rule_ids"] = [
        rule_ids_by_row[position]
        for position in quarantined_positions
    ]
    quarantined["_quarantined_at_utc"] = (
        datetime.now(timezone.utc).isoformat()
    )

    return approved, quarantined
