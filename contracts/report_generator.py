"""
contracts/report_generator.py — Enforcer Report Generator (Phase 4B)
=====================================================================
Auto-generates the stakeholder Enforcer Report from live validation data.
Must be machine-generated — not hand-written.

Outputs:
  - enforcer_report/report_data.json — structured JSON
  - enforcer_report/report_{date}.md  — Markdown with 5 required sections

Usage:
    python contracts/report_generator.py
"""

import argparse
import glob
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml

ROOT = Path(__file__).parent.parent
REPORTS_DIR = ROOT / "validation_reports"
VIOLATIONS_PATH = ROOT / "violation_log" / "violations.jsonl"
AI_EXTENSIONS_PATH = ROOT / "validation_reports" / "ai_extensions.json"
REGISTRY_PATH = ROOT / "contract_registry" / "subscriptions.yaml"
SCHEMA_EVOLUTION_DIR = ROOT / "validation_reports"
ENFORCER_DIR = ROOT / "enforcer_report"

SEVERITY_DEDUCTIONS = {"CRITICAL": 20, "HIGH": 10, "MEDIUM": 5, "LOW": 1}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def load_all_reports() -> list[dict]:
    reports = []
    for p in sorted(glob.glob(str(REPORTS_DIR / "*.json"))):
        if "ai_extensions" in p or "schema_evolution" in p:
            continue
        try:
            with open(p) as f:
                reports.append(json.load(f))
        except Exception:
            pass
    return reports


def load_violations() -> list[dict]:
    if not VIOLATIONS_PATH.exists():
        return []
    with open(VIOLATIONS_PATH) as f:
        return [json.loads(l) for l in f if l.strip()]


def load_ai_extensions() -> dict:
    if not AI_EXTENSIONS_PATH.exists():
        return {}
    with open(AI_EXTENSIONS_PATH) as f:
        return json.load(f)


def load_registry() -> dict:
    if not REGISTRY_PATH.exists():
        return {"subscriptions": []}
    with open(REGISTRY_PATH) as f:
        return yaml.safe_load(f) or {"subscriptions": []}


def load_schema_evolution() -> list[dict]:
    """Load schema evolution reports (JSONL format: *_evolution.jsonl)."""
    reports = []
    for p in sorted(glob.glob(str(SCHEMA_EVOLUTION_DIR / "*_evolution.jsonl"))):
        if not Path(p).is_file():
            continue
        try:
            with open(p) as f:
                for line in f:
                    line = line.strip()
                    if line:
                        reports.append(json.loads(line))
        except Exception:
            pass
    return reports


def compute_health_score(reports: list[dict]) -> tuple[int, list[dict]]:
    all_fails = []
    for rep in reports:
        for r in rep.get("results", []):
            if r.get("status") in ("FAIL", "ERROR"):
                all_fails.append(r)

    total_checks = sum(rep.get("total_checks", 0) for rep in reports)
    passed = sum(rep.get("passed", 0) for rep in reports)

    if total_checks == 0:
        return 100, all_fails

    base_score = round((passed / total_checks) * 100)
    # Per rubric: adjusted down by 20 points per CRITICAL violation
    # Additional graduated deductions for HIGH/MEDIUM/LOW via SEVERITY_DEDUCTIONS
    critical_count = sum(1 for f in all_fails if f.get("severity") == "CRITICAL")
    other_deduction = sum(
        SEVERITY_DEDUCTIONS.get(f.get("severity", "LOW"), 0)
        for f in all_fails if f.get("severity") != "CRITICAL"
    )
    score = max(0, min(100, base_score - (critical_count * 20) - other_deduction))

    return score, all_fails


def plain_language_violation(result: dict, registry: dict) -> str:
    """Convert a technical check result into plain language."""
    col = result.get("column_name", "unknown")
    check_type = result.get("check_type", "unknown")
    actual = result.get("actual_value", "unknown")
    expected = result.get("expected", "unknown")
    check_id = result.get("check_id", "")
    records = result.get("records_failing", "unknown")

    contract_id = check_id.split(".")[0] if "." in check_id else check_id
    subs = [s["subscriber_id"] for s in registry.get("subscriptions", [])
            if s["contract_id"] == contract_id]
    sub_str = ", ".join(subs) if subs else "no registered subscribers"

    return (
        f"The '{col}' field failed its {check_type} check. "
        f"Expected {expected}, found {actual}. "
        f"Downstream subscribers affected: {sub_str}. "
        f"Records failing: {records}."
    )


def generate_report() -> dict:
    print("\n[report_generator] Generating Enforcer Report...")

    reports = load_all_reports()
    violations = load_violations()
    ai = load_ai_extensions()
    registry = load_registry()
    schema_evo = load_schema_evolution()

    print(f"  loaded {len(reports)} validation reports")
    print(f"  loaded {len(violations)} violations")
    print(f"  loaded {len(schema_evo)} schema evolution reports")

    # Section 1: Data Health Score
    score, all_fails = compute_health_score(reports)
    critical_count = sum(1 for f in all_fails if f.get("severity") == "CRITICAL")

    if score >= 90:
        narrative = f"Score {score}/100. All systems operating within contract parameters."
    elif score >= 70:
        narrative = (f"Score {score}/100. {critical_count} critical issue(s) detected. "
                     "Review recommended before next deployment.")
    else:
        narrative = (f"Score {score}/100. {critical_count} critical issue(s) require immediate action. "
                     "Pipeline should not proceed without resolution.")

    # Section 2: Violations
    severity_order = ["CRITICAL", "HIGH", "MEDIUM", "LOW"]
    top_fails = sorted(all_fails,
                       key=lambda x: severity_order.index(x.get("severity", "LOW"))
                       if x.get("severity") in severity_order else 99)[:3]

    violations_by_severity = {}
    for sev in severity_order:
        count = sum(1 for f in all_fails if f.get("severity") == sev)
        if count > 0:
            violations_by_severity[sev] = count

    # Section 3: Schema changes
    schema_changes = []
    for evo in schema_evo:
        for change in evo.get("changes", []):
            schema_changes.append(change)
    breaking_changes = [c for c in schema_changes
                        if c.get("compatibility") == "BREAKING"
                        or c.get("severity") in ("BREAKING", "CRITICAL")]

    # Section 4: AI risk
    embedding_drift = ai.get("embedding_drift", {})
    output_viol = ai.get("output_violation_rate", {})
    prompt_val = ai.get("prompt_input_validation", {})

    # Section 5: Recommendations (derive file paths from validation data)
    # Extract actual data_path from validation reports that had failures
    failing_data_paths = set()
    for rep in reports:
        has_fails = any(r.get("status") in ("FAIL", "ERROR") for r in rep.get("results", []))
        if has_fails:
            dp = rep.get("data_path") or rep.get("metadata", {}).get("data_path")
            if dp:
                failing_data_paths.add(dp)

    recommendations = []
    if critical_count > 0:
        for f in top_fails:
            if f.get("severity") == "CRITICAL":
                col = f.get("column_name", "unknown")
                check_id = f.get("check_id", "")
                # Use actual failing data path if available
                data_ref = next(iter(failing_data_paths), "the source data file")
                recommendations.append(
                    f"Fix {col} in {data_ref}: {f.get('message', 'check failed')} "
                    f"(contract clause: {check_id}). Ensure confidence values are float 0.0-1.0."
                )
                break
    if not recommendations:
        data_ref = next(iter(failing_data_paths), "the source data file")
        recommendations.append(
            f"Review {data_ref} for contract compliance. "
            "Ensure all field values conform to their contract-specified ranges and types."
        )
    recommendations.append(
        "Add contracts/runner.py --mode ENFORCE as a required CI step before any deployment. "
        "Run: python contracts/runner.py --contract generated_contracts/<contract>.yaml "
        "--data outputs/<week>/<file>.jsonl --mode ENFORCE --output validation_reports/"
    )
    recommendations.append(
        "Schedule monthly baseline refresh: delete schema_snapshots/baselines.json "
        "and re-run python contracts/runner.py --mode AUDIT on clean data to re-establish baselines"
    )

    # Build report_data.json
    report_data = {
        "generated_at": now_iso(),
        "period": f"{(datetime.now(timezone.utc) - timedelta(days=7)).date()} to {datetime.now(timezone.utc).date()}",
        "data_health_score": score,
        "health_narrative": narrative,
        "violations_by_severity": violations_by_severity,
        "top_violations": [plain_language_violation(f, registry) for f in top_fails],
        "total_violations_attributed": len(violations),
        "schema_changes_detected": len(schema_changes),
        "breaking_changes": len(breaking_changes),
        "ai_risk": {
            "embedding_drift": {
                "score": embedding_drift.get("drift_score", "N/A"),
                "status": embedding_drift.get("status", "UNKNOWN"),
                "threshold": embedding_drift.get("threshold", 0.15),
            },
            "output_violation_rate": {
                "rate": output_viol.get("violation_rate", "N/A"),
                "trend": output_viol.get("trend", "unknown"),
                "status": output_viol.get("status", "UNKNOWN"),
            },
            "prompt_validation": {
                "valid": prompt_val.get("valid", "N/A"),
                "quarantined": prompt_val.get("quarantined", "N/A"),
                "status": prompt_val.get("status", "UNKNOWN"),
            },
        },
        "recommendations": recommendations,
    }

    # Write report_data.json
    ENFORCER_DIR.mkdir(parents=True, exist_ok=True)
    data_path = ENFORCER_DIR / "report_data.json"
    with open(data_path, "w") as f:
        json.dump(report_data, f, indent=2)
    print(f"  [report_generator] report_data.json -> {data_path}")

    # Generate Markdown report
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    md_path = ENFORCER_DIR / f"report_{date_str}.md"
    md = generate_markdown(report_data, violations, schema_changes, registry)
    with open(md_path, "w") as f:
        f.write(md)
    print(f"  [report_generator] markdown report -> {md_path}")

    return report_data


def generate_markdown(data: dict, violations: list, schema_changes: list, registry: dict) -> str:
    lines = [
        "# Data Contract Enforcer Report",
        "",
        f"**Generated:** {data['generated_at']}",
        f"**Period:** {data['period']}",
        "",
        "---",
        "",
        "## 1. Data Health Score",
        "",
        f"**Score: {data['data_health_score']}/100**",
        "",
        data["health_narrative"],
        "",
        "---",
        "",
        "## 2. Violations This Week",
        "",
    ]

    if data["violations_by_severity"]:
        lines.append("| Severity | Count |")
        lines.append("|----------|-------|")
        for sev, count in data["violations_by_severity"].items():
            lines.append(f"| {sev} | {count} |")
        lines.append("")

    if data["top_violations"]:
        lines.append("### Top Violations")
        lines.append("")
        for i, v in enumerate(data["top_violations"], 1):
            lines.append(f"{i}. {v}")
        lines.append("")
    else:
        lines.append("No violations detected in the reporting period.")
        lines.append("")

    lines.extend([
        "---",
        "",
        "## 3. Schema Changes Detected",
        "",
    ])

    if schema_changes:
        lines.append(f"{len(schema_changes)} schema change(s) detected. "
                     f"{data['breaking_changes']} classified as BREAKING.")
        lines.append("")
        for c in schema_changes[:5]:
            compat = c.get("compatibility") or c.get("severity", "UNKNOWN")
            col = c.get("field") or c.get("column", "unknown")
            desc = c.get("description") or c.get("detail", "")
            lines.append(f"- **{col}**: {c.get('change_type', 'unknown')} "
                        f"({compat}) — {desc}")
        lines.append("")
    else:
        lines.append("No schema changes detected in the reporting period.")
        lines.append("")

    lines.extend([
        "---",
        "",
        "## 4. AI System Risk Assessment",
        "",
    ])

    ai = data["ai_risk"]
    drift = ai["embedding_drift"]
    lines.append(f"- **Embedding Drift:** {drift['status']} "
                 f"(score={drift['score']}, threshold={drift['threshold']})")

    ovr = ai["output_violation_rate"]
    lines.append(f"- **LLM Output Violation Rate:** {ovr['status']} "
                 f"(rate={ovr['rate']}, trend={ovr['trend']})")

    pv = ai["prompt_validation"]
    lines.append(f"- **Prompt Input Validation:** {pv['status']} "
                 f"(valid={pv['valid']}, quarantined={pv['quarantined']})")
    lines.append("")

    lines.extend([
        "---",
        "",
        "## 5. Recommended Actions",
        "",
    ])

    for i, rec in enumerate(data["recommendations"], 1):
        lines.append(f"{i}. {rec}")
    lines.append("")

    lines.extend([
        "---",
        "",
        "*This report was auto-generated by `contracts/report_generator.py` "
        "from live validation data.*",
    ])

    return "\n".join(lines)


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Enforcer Report Generator")
    parser.parse_args()
    generate_report()
