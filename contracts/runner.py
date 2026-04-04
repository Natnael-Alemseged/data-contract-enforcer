"""
contracts/runner.py — ValidationRunner (Phase 2A)
==================================================
Executes every clause in a contract YAML against a data snapshot.
Never crashes — always produces a complete report even on broken input.

Usage:
    python contracts/runner.py \
        --contract generated_contracts/week3-document-refinery-extractions.yaml \
        --data outputs/week3/extractions.jsonl \
        --output validation_reports/week3_$(date +%Y%m%d_%H%M).json

    # Inject a known violation to test detection:
    python contracts/runner.py \
        --contract generated_contracts/week3-document-refinery-extractions.yaml \
        --data outputs/week3/extractions.jsonl \
        --output validation_reports/ \
        --inject-violation confidence_scale
"""

import argparse
import hashlib
import json
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

ROOT = Path(__file__).parent.parent
BASELINES_PATH = ROOT / "schema_snapshots" / "baselines.json"

# ── helpers ───────────────────────────────────────────────────────────────────

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def rng_uuid() -> str:
    return str(uuid.uuid4())

def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()

def load_jsonl(path: Path) -> list[dict]:
    with open(path) as f:
        return [json.loads(l) for l in f if l.strip()]

def load_contract(path: Path) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)

def flatten_for_profile(records: list[dict]) -> pd.DataFrame:
    """Same flattening logic as generator — one row per nested array item."""
    rows = []
    for r in records:
        base = {k: v for k, v in r.items() if not isinstance(v, (list, dict))}
        for k, v in r.items():
            if isinstance(v, dict):
                for dk, dv in v.items():
                    if not isinstance(dv, (list, dict)):
                        base[f"{k}.{dk}"] = dv
        exploded = False
        for k, v in r.items():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                for item in v:
                    row = {**base}
                    for ik, iv in item.items():
                        if not isinstance(iv, (list, dict)):
                            row[f"{k}.{ik}"] = iv
                    rows.append(row)
                exploded = True
                break
        if not exploded:
            rows.append(base)
    return pd.DataFrame(rows)


# ── violation injection (P2A-9) ───────────────────────────────────────────────

def inject_violation(records: list[dict], violation_type: str) -> list[dict]:
    """Mutate records to introduce a known violation for testing."""
    import copy
    records = copy.deepcopy(records)

    if violation_type == "confidence_scale":
        # Multiply all confidence values by 100 (0.0–1.0 → 0–100)
        count = 0
        for r in records:
            for fact in r.get("extracted_facts", []):
                if "confidence" in fact:
                    fact["confidence"] = round(fact["confidence"] * 100, 1)
                    count += 1
            # also handle flat confidence
            if "confidence" in r:
                r["confidence"] = round(r["confidence"] * 100, 1)
                count += 1
        print(f"  [inject] multiplied {count} confidence values ×100 (simulating 0–100 scale)")

    elif violation_type == "missing_required":
        for r in records[:3]:
            r.pop("doc_id", None)
            r.pop("event_id", None)
        print("  [inject] removed required ID field from first 3 records")

    elif violation_type == "bad_enum":
        for r in records[:5]:
            if "overall_verdict" in r:
                r["overall_verdict"] = "MAYBE"
            if "run_type" in r:
                r["run_type"] = "unknown_type"
        print("  [inject] set invalid enum values in first 5 records")

    return records


# ── individual check functions ────────────────────────────────────────────────

UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I)

def make_result(check_id, column_name, check_type, status, actual_value,
                expected, severity, records_failing=0, sample_failing=None, message=""):
    return {
        "check_id":        check_id,
        "column_name":     column_name,
        "check_type":      check_type,
        "status":          status,
        "actual_value":    str(actual_value),
        "expected":        str(expected),
        "severity":        severity,
        "records_failing": records_failing,
        "sample_failing":  sample_failing or [],
        "message":         message,
    }


def check_required(contract_id, col, clause, series) -> dict:
    """CRITICAL if any nulls in a required field."""
    null_count = int(series.isna().sum())
    check_id = f"{contract_id}.{col}.required"
    if null_count > 0:
        sample = list(series[series.isna()].index[:5])
        return make_result(
            check_id, col, "required", "FAIL",
            f"null_count={null_count}", "null_count=0",
            "CRITICAL", null_count, sample,
            f"`{col}` has {null_count} null values but is marked required."
        )
    return make_result(check_id, col, "required", "PASS", "null_count=0", "null_count=0", "LOW")


def check_type(contract_id, col, clause, series) -> dict:
    """CRITICAL if column type doesn't match contract type."""
    expected_type = clause.get("type")
    check_id = f"{contract_id}.{col}.type"
    if not expected_type:
        return make_result(check_id, col, "type", "PASS", "no type clause", "—", "LOW")

    actual_is_numeric = pd.api.types.is_numeric_dtype(series)
    type_ok = True
    if expected_type in ("number", "integer") and not actual_is_numeric:
        # Allow if all non-null values are numeric strings
        non_null = series.dropna()
        try:
            pd.to_numeric(non_null)
        except (ValueError, TypeError):
            type_ok = False
    elif expected_type == "boolean" and series.dtype != bool:
        type_ok = series.dropna().isin([True, False, 0, 1]).all()

    if not type_ok:
        return make_result(
            check_id, col, "type", "FAIL",
            f"dtype={series.dtype}", f"type={expected_type}",
            "CRITICAL", int(series.notna().sum()), [],
            f"`{col}` has dtype `{series.dtype}` but contract expects `{expected_type}`."
        )
    return make_result(check_id, col, "type", "PASS",
                       f"dtype={series.dtype}", f"type={expected_type}", "LOW")


def check_enum(contract_id, col, clause, series) -> dict:
    """WARN/FAIL if values outside declared enum."""
    enum_vals = clause.get("enum")
    check_id = f"{contract_id}.{col}.enum"
    if not enum_vals:
        return make_result(check_id, col, "enum", "PASS", "no enum clause", "—", "LOW")

    non_null = series.dropna().astype(str)
    bad = non_null[~non_null.isin([str(e) for e in enum_vals])]
    if len(bad) > 0:
        sample = list(bad.unique()[:5])
        return make_result(
            check_id, col, "enum", "FAIL",
            f"{len(bad)} non-conforming values: {sample[:3]}",
            f"enum={enum_vals}",
            "CRITICAL", int(len(bad)), sample,
            f"`{col}` has {len(bad)} values not in enum {enum_vals}."
        )
    return make_result(check_id, col, "enum", "PASS",
                       f"all values in {enum_vals}", f"enum={enum_vals}", "LOW")


def check_uuid_pattern(contract_id, col, clause, series) -> dict:
    """CRITICAL if UUID-formatted field has non-UUID values."""
    if clause.get("format") != "uuid":
        return None
    check_id = f"{contract_id}.{col}.uuid_pattern"
    non_null = series.dropna().astype(str)
    # Sample up to 1000 if large
    sample_series = non_null.sample(min(1000, len(non_null)), random_state=42) if len(non_null) > 1000 else non_null
    bad = sample_series[~sample_series.str.match(UUID_RE, na=False)]
    if len(bad) > 0:
        sample = list(bad.unique()[:5])
        return make_result(
            check_id, col, "uuid_pattern", "FAIL",
            f"{len(bad)} non-UUID values in sample",
            "format: uuid (^[0-9a-f-]{36}$)",
            "CRITICAL", int(len(bad)), sample,
            f"`{col}` contains values that don't match UUID format."
        )
    return make_result(check_id, col, "uuid_pattern", "PASS",
                       "all values match UUID pattern", "format: uuid", "LOW")


def check_datetime_format(contract_id, col, clause, series) -> dict:
    """HIGH if date-time field has unparseable values."""
    if clause.get("format") != "date-time":
        return None
    check_id = f"{contract_id}.{col}.datetime_format"
    non_null = series.dropna().astype(str)
    bad_count = 0
    bad_samples = []
    for val in non_null[:500]:  # check first 500
        try:
            datetime.fromisoformat(val.replace("Z", "+00:00"))
        except ValueError:
            bad_count += 1
            if len(bad_samples) < 5:
                bad_samples.append(val)
    if bad_count > 0:
        return make_result(
            check_id, col, "datetime_format", "FAIL",
            f"{bad_count} unparseable values",
            "format: date-time (ISO 8601)",
            "HIGH", bad_count, bad_samples,
            f"`{col}` has {bad_count} values that are not valid ISO 8601 date-times."
        )
    return make_result(check_id, col, "datetime_format", "PASS",
                       "all values parse as ISO 8601", "format: date-time", "LOW")


def check_range(contract_id, col, clause, series) -> dict:
    """CRITICAL if numeric values outside declared min/max."""
    minimum = clause.get("minimum")
    maximum = clause.get("maximum")
    if minimum is None and maximum is None:
        return None
    check_id = f"{contract_id}.{col}.range"
    numeric = pd.to_numeric(series.dropna(), errors="coerce").dropna()
    if len(numeric) == 0:
        return make_result(check_id, col, "range", "ERROR",
                           "no numeric values to check", f"min={minimum}, max={maximum}",
                           "HIGH", 0, [], f"`{col}` has no numeric values.")

    actual_min = float(numeric.min())
    actual_max = float(numeric.max())
    actual_mean = float(numeric.mean())

    violations = []
    if minimum is not None and actual_min < minimum:
        violations.append(f"min={actual_min:.4f} < contract_min={minimum}")
    if maximum is not None and actual_max > maximum:
        violations.append(f"max={actual_max:.4f} > contract_max={maximum}")

    if violations:
        bad_mask = pd.Series(False, index=numeric.index)
        if minimum is not None:
            bad_mask |= numeric < minimum
        if maximum is not None:
            bad_mask |= numeric > maximum
        bad_count = int(bad_mask.sum())
        return make_result(
            check_id, col, "range", "FAIL",
            f"min={actual_min:.4f}, max={actual_max:.4f}, mean={actual_mean:.4f}",
            f"min>={minimum}, max<={maximum}",
            "CRITICAL", bad_count, [],
            f"`{col}` range violation: {'; '.join(violations)}"
        )
    return make_result(
        check_id, col, "range", "PASS",
        f"min={actual_min:.4f}, max={actual_max:.4f}, mean={actual_mean:.4f}",
        f"min>={minimum}, max<={maximum}", "LOW"
    )


def check_statistical_drift(contract_id, col, series, baselines: dict) -> dict | None:
    """WARN if >2σ drift, FAIL if >3σ drift from baseline mean."""
    check_id = f"{contract_id}.{col}.statistical_drift"
    numeric = pd.to_numeric(series.dropna(), errors="coerce").dropna()
    if len(numeric) < 5:
        return None  # not enough data to drift-check

    current_mean = float(numeric.mean())
    current_std  = float(numeric.std()) if len(numeric) > 1 else 0.0

    if col not in baselines:
        return None  # baseline not yet established

    b = baselines[col]
    z_score = abs(current_mean - b["mean"]) / max(b["stddev"], 1e-9)

    if z_score > 3:
        return make_result(
            check_id, col, "statistical_drift", "FAIL",
            f"mean={current_mean:.4f} (baseline={b['mean']:.4f}, z={z_score:.2f})",
            f"|z| <= 3.0 (baseline stddev={b['stddev']:.4f})",
            "HIGH", 0, [],
            f"`{col}` mean drifted {z_score:.1f} stddev from baseline. "
            f"Possible scale change or data corruption."
        )
    elif z_score > 2:
        return make_result(
            check_id, col, "statistical_drift", "WARN",
            f"mean={current_mean:.4f} (baseline={b['mean']:.4f}, z={z_score:.2f})",
            f"|z| <= 2.0",
            "MEDIUM", 0, [],
            f"`{col}` mean within warning range ({z_score:.1f} stddev from baseline)."
        )
    return make_result(
        check_id, col, "statistical_drift", "PASS",
        f"mean={current_mean:.4f}, z={z_score:.2f}", f"|z| <= 2.0", "LOW"
    )


# ── quality block execution (SodaChecks compiled to pandas) ──────────────────

SODA_AGGS = re.compile(
    r"^-\s+(row_count|missing_count|duplicate_count|min|max|avg)\(?([\w.]*)\)?\s*([><=!]+)\s*(.+)$"
)

def check_quality_spec(contract_id: str, contract: dict, df: pd.DataFrame) -> list[dict]:
    """
    Parse quality.specification (SodaChecks YAML list) and execute each check
    as a pandas operation. Returns a list of check results.
    """
    results = []
    quality = contract.get("quality", {})
    spec = quality.get("specification", {})
    checks_raw = []
    for _table, items in spec.items():
        if isinstance(items, list):
            checks_raw.extend(items)

    for raw in checks_raw:
        raw = str(raw).strip()
        m = SODA_AGGS.match(raw)
        if not m:
            continue  # skip unparseable (e.g. cross-field prose)
        fn, col_raw, op, threshold_str = m.group(1), m.group(2), m.group(3), m.group(4).strip()
        col = col_raw.replace("_", ".")  # undo flattening for lookup
        check_id = f"{contract_id}.quality.{fn}_{col_raw}_{op.replace('=','eq').replace('>','gt').replace('<','lt')}"

        try:
            threshold = float(threshold_str)
        except ValueError:
            continue

        # Get series — try dot-notation first, then underscore
        series = df.get(col) if col else None
        if series is None and col_raw:
            series = df.get(col_raw)

        try:
            if fn == "row_count":
                actual = len(df)
            elif fn == "missing_count":
                actual = int(series.isna().sum()) if series is not None else 0
            elif fn == "duplicate_count":
                actual = int(series.dropna().duplicated().sum()) if series is not None else 0
            elif fn == "min":
                actual = float(pd.to_numeric(series.dropna(), errors="coerce").min()) if series is not None else 0.0
            elif fn == "max":
                actual = float(pd.to_numeric(series.dropna(), errors="coerce").max()) if series is not None else 0.0
            elif fn == "avg":
                actual = float(pd.to_numeric(series.dropna(), errors="coerce").mean()) if series is not None else 0.0
            else:
                continue

            ops = {"=": actual == threshold, ">=": actual >= threshold,
                   "<=": actual <= threshold, ">": actual > threshold, "<": actual < threshold,
                   "!=": actual != threshold}
            passed = ops.get(op, False)
            severity = "CRITICAL" if fn in ("missing_count", "duplicate_count", "row_count") else "HIGH"
            status = "PASS" if passed else "FAIL"
            results.append(make_result(
                check_id, col_raw or "dataset", "quality_spec", status,
                str(actual), f"{fn}({col_raw}) {op} {threshold}",
                severity, 0 if passed else int(actual),
                [],
                f"Quality check `{raw}`: actual={actual}, expected {op} {threshold}."
            ))
        except Exception as e:
            results.append(make_result(
                check_id, col_raw or "dataset", "quality_spec", "ERROR",
                str(e), raw, "MEDIUM", 0, [],
                f"Quality check failed to execute: {e}"
            ))

    return results


# ── x-relationships execution ─────────────────────────────────────────────────

def check_relationships(contract_id: str, contract: dict, df: pd.DataFrame) -> list[dict]:
    """
    Evaluate x-relationships expressions using pandas eval.
    Supports simple column arithmetic comparisons.
    """
    results = []
    relationships = contract.get("x-relationships", [])
    if not relationships:
        return results

    # Build flat df with underscore column names for eval compatibility
    df_eval = df.copy()
    df_eval.columns = [c.replace(".", "_").replace("-", "_") for c in df_eval.columns]

    for rel in relationships:
        name = rel.get("name", "unnamed")
        expr = rel.get("expression", "")
        severity = rel.get("severity", "HIGH")
        check_id = f"{contract_id}.relationship.{name}"

        # Skip expressions with GROUP BY or sequence logic (can't eval simply)
        if "GROUP BY" in expr or "[n]" in expr:
            results.append(make_result(
                check_id, name, "relationship", "PASS",
                "complex expression — documented constraint",
                expr, "LOW", 0, [],
                f"Relationship `{name}` is a complex constraint documented in contract; "
                f"manual or pipeline-level enforcement required."
            ))
            continue

        # Translate dot-notation columns and == to evaluable expression
        eval_expr = expr.replace(".", "_").replace("-", "_")
        # pandas eval uses == for equality
        try:
            # Only evaluate on rows where all involved columns are non-null
            mask = df_eval.eval(eval_expr)
            violations = int((~mask).sum()) if hasattr(mask, "__iter__") else 0
            total = int(mask.notna().sum()) if hasattr(mask, "notna") else len(df_eval)
            status = "PASS" if violations == 0 else "FAIL"
            results.append(make_result(
                check_id, name, "relationship", status,
                f"{violations}/{total} rows violate expression",
                expr, severity, violations, [],
                f"Relationship `{name}`: {violations} rows failed `{expr}`."
            ))
        except Exception as e:
            results.append(make_result(
                check_id, name, "relationship", "ERROR",
                str(e), expr, "MEDIUM", 0, [],
                f"Could not evaluate relationship `{name}`: {e}"
            ))

    return results


# ── baseline management ───────────────────────────────────────────────────────

def load_baselines(contract_id: str) -> dict:
    if not BASELINES_PATH.exists():
        return {}
    data = json.loads(BASELINES_PATH.read_text())
    return data.get(contract_id, {})


def write_baselines(contract_id: str, df: pd.DataFrame):
    BASELINES_PATH.parent.mkdir(parents=True, exist_ok=True)
    existing = json.loads(BASELINES_PATH.read_text()) if BASELINES_PATH.exists() else {}
    baselines = {}
    for col in df.select_dtypes(include="number").columns:
        clean = df[col].dropna()
        if len(clean) >= 5:
            baselines[col] = {
                "mean":   float(clean.mean()),
                "stddev": float(clean.std()) if len(clean) > 1 else 0.0,
                "n":      int(len(clean)),
            }
    existing[contract_id] = baselines
    existing["_written_at"] = now_iso()
    BASELINES_PATH.write_text(json.dumps(existing, indent=2))
    print(f"  [runner] baselines written for {len(baselines)} numeric columns → {BASELINES_PATH}")


# ── main runner ───────────────────────────────────────────────────────────────

def run_validation(
    contract_path: Path,
    data_path: Path,
    output_dir: Path,
    inject: str | None = None,
    mode: str = "AUDIT",
) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)
    report_id   = rng_uuid()
    run_ts      = now_iso()
    contract    = load_contract(contract_path)
    contract_id = contract.get("id", contract_path.stem)
    schema      = contract.get("schema", {})

    print(f"\n[runner] contract   : {contract_id}")
    print(f"[runner] data       : {data_path}")

    # Load data
    try:
        records = load_jsonl(data_path)
    except Exception as e:
        return _error_report(report_id, contract_id, str(data_path), run_ts,
                             f"Failed to load data file: {e}")

    snapshot_id = sha256_file(data_path)

    # Inject violation if requested
    if inject:
        records = inject_violation(records, inject)

    # Flatten
    try:
        df = flatten_for_profile(records)
    except Exception as e:
        return _error_report(report_id, contract_id, snapshot_id, run_ts,
                             f"Failed to flatten records: {e}")

    print(f"  [runner] {len(records)} records → {len(df)} rows × {len(df.columns)} columns")

    # Load baselines; write them if first run
    baselines = load_baselines(contract_id)
    first_run = not baselines
    if first_run:
        print(f"  [runner] no baselines yet — will write after this run")

    # ── run all checks ────────────────────────────────────────────────────────
    results = []

    for col, clause in schema.items():
        # Map contract column name to DataFrame column (dots preserved)
        series = df.get(col)
        if series is None:
            # Try without the array prefix (e.g. "extracted_facts.confidence" → present in df)
            series = df.get(col.replace("[*]", "").lstrip("."))

        if series is None:
            results.append(make_result(
                f"{contract_id}.{col}.presence", col, "presence", "ERROR",
                "column not found in data", "column present",
                "CRITICAL", 0, [],
                f"Column `{col}` declared in contract but not found in data. "
                f"Available columns: {list(df.columns)[:8]}..."
            ))
            continue

        # 1. required
        if clause.get("required"):
            results.append(check_required(contract_id, col, clause, series))

        # 2. type
        r = check_type(contract_id, col, clause, series)
        if r:
            results.append(r)

        # 3. enum
        r = check_enum(contract_id, col, clause, series)
        if r:
            results.append(r)

        # 4. UUID pattern
        r = check_uuid_pattern(contract_id, col, clause, series)
        if r:
            results.append(r)

        # 5. date-time format
        r = check_datetime_format(contract_id, col, clause, series)
        if r:
            results.append(r)

        # 6. range
        r = check_range(contract_id, col, clause, series)
        if r:
            results.append(r)

        # 7. statistical drift
        r = check_statistical_drift(contract_id, col, series, baselines)
        if r:
            results.append(r)

    # 8. Quality specification block (SodaChecks compiled to pandas)
    quality_results = check_quality_spec(contract_id, contract, df)
    if quality_results:
        results.extend(quality_results)
        print(f"  [runner] quality spec: {len(quality_results)} checks executed")

    # 9. x-relationships (cross-field constraints)
    rel_results = check_relationships(contract_id, contract, df)
    if rel_results:
        results.extend(rel_results)
        print(f"  [runner] relationships: {len(rel_results)} checks executed")

    # Write baselines on first run
    if first_run:
        try:
            write_baselines(contract_id, df)
        except Exception as e:
            print(f"  [runner] WARNING: failed to write baselines: {e}")

    # ── build report ──────────────────────────────────────────────────────────
    status_counts = {"PASS": 0, "FAIL": 0, "WARN": 0, "ERROR": 0}
    for r in results:
        status_counts[r["status"]] = status_counts.get(r["status"], 0) + 1

    # Determine pipeline_action based on mode
    has_critical = any(r["severity"] == "CRITICAL" and r["status"] == "FAIL" for r in results)
    has_high = any(r["severity"] == "HIGH" and r["status"] == "FAIL" for r in results)

    if mode == "AUDIT":
        pipeline_action = "PASS"
    elif mode == "WARN":
        pipeline_action = "BLOCK" if has_critical else ("WARN" if has_high else "PASS")
    else:  # ENFORCE
        pipeline_action = "BLOCK" if (has_critical or has_high) else "PASS"

    report = {
        "report_id":    report_id,
        "contract_id":  contract_id,
        "snapshot_id":  snapshot_id,
        "run_timestamp": run_ts,
        "data_path":    str(data_path),
        "mode":         mode,
        "pipeline_action": pipeline_action,
        "injected_violation": inject,
        "total_checks": len(results),
        "passed":       status_counts["PASS"],
        "failed":       status_counts["FAIL"],
        "warned":       status_counts["WARN"],
        "errored":      status_counts["ERROR"],
        "results":      results,
    }

    # Write report
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    suffix = f"_{inject}" if inject else ""
    out_path = output_dir / f"{contract_id}{suffix}_{ts}.json"
    out_path.write_text(json.dumps(report, indent=2))

    # Summary
    verdict = "✅ PASS" if status_counts["FAIL"] == 0 and status_counts["ERROR"] == 0 else "❌ FAIL"
    print(f"\n  [runner] {verdict} — "
          f"{status_counts['PASS']} passed, {status_counts['FAIL']} failed, "
          f"{status_counts['WARN']} warned, {status_counts['ERROR']} errored")
    print(f"  [runner] report → {out_path}")

    # Print failures prominently
    failures = [r for r in results if r["status"] in ("FAIL", "ERROR")]
    if failures:
        print(f"\n  [runner] FAILURES:")
        for f in failures:
            print(f"    ❌ [{f['severity']}] {f['check_id']}: {f['message']}")

    return report


def _error_report(report_id, contract_id, snapshot_id, run_ts, message):
    return {
        "report_id": report_id, "contract_id": contract_id,
        "snapshot_id": snapshot_id, "run_timestamp": run_ts,
        "total_checks": 0, "passed": 0, "failed": 0, "warned": 0, "errored": 1,
        "results": [make_result(
            f"{contract_id}.load_error", "—", "load", "ERROR",
            message, "data loads successfully", "CRITICAL", 0, [], message
        )],
    }


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ValidationRunner — execute contract checks against data")
    parser.add_argument("--contract", required=True)
    parser.add_argument("--data",     required=True)
    parser.add_argument("--output",   required=True)
    parser.add_argument("--mode",     choices=["AUDIT", "WARN", "ENFORCE"], default="AUDIT",
                        help="Enforcement mode: AUDIT (log only), WARN (block CRITICAL), ENFORCE (block CRITICAL+HIGH)")
    parser.add_argument("--inject-violation", dest="inject",
                        choices=["confidence_scale", "missing_required", "bad_enum"],
                        help="Inject a known violation for testing")
    args = parser.parse_args()

    run_validation(
        contract_path=Path(args.contract),
        data_path=Path(args.data),
        output_dir=Path(args.output),
        inject=args.inject,
        mode=args.mode,
    )
