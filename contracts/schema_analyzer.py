"""
contracts/schema_analyzer.py — SchemaEvolutionAnalyzer (Phase 3)
=================================================================
Diffs consecutive schema snapshots for a contract, classifies each change
as backward-compatible or breaking, and writes a migration impact report.

Change taxonomy
───────────────
BREAKING (would fail consumers):
  - column_removed        : column present in old, absent in new
  - type_changed          : dtype changed (e.g. float64 → str)
  - nullable_tightened    : null_fraction was >0, now 0 (non-null enforced)
  - range_narrowed        : numeric min increased or max decreased

WARN (may affect consumers):
  - cardinality_spike     : cardinality grew >5× (enum might be busted)
  - stat_drift            : mean/stddev shifted >2σ from prior snapshot

COMPATIBLE (safe):
  - column_added          : new column (consumers can ignore)
  - nullable_relaxed      : null_fraction was 0, now >0 (consumers already handle non-null)
  - cardinality_stable    : no meaningful change

Usage:
    python contracts/schema_analyzer.py \
        --contract-id week3-document-refinery-extractions \
        --snapshots schema_snapshots/ \
        --output migration_reports/
"""

import argparse
import json
import uuid
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path

import yaml

ROOT = Path(__file__).parent.parent
REGISTRY_PATH = ROOT / "contract_registry" / "subscriptions.yaml"

# Narrow-type pairs: (old_dtype_keyword, new_dtype_keyword) that lose precision
NARROW_TYPE_PAIRS = [
    ("float", "int"),
    ("double", "int"),
    ("float64", "int64"),
    ("float64", "int32"),
    ("float32", "int32"),
    ("float32", "int16"),
    ("str", "int"),
    ("str", "float"),
    ("datetime", "str"),
]


# ── helpers ───────────────────────────────────────────────────────────────────

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def rng_uuid() -> str:
    return str(uuid.uuid4())


def load_snapshot(path: Path) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


# ── diff logic ────────────────────────────────────────────────────────────────

def classify_changes(old_snap: dict, new_snap: dict) -> list[dict]:
    """
    Compare two snapshots column-by-column.
    Returns a list of change records.
    """
    old_cols: dict = old_snap.get("columns", {})
    new_cols: dict = new_snap.get("columns", {})

    changes = []

    # Columns removed (BREAKING)
    for col in old_cols:
        if col not in new_cols:
            changes.append({
                "column": col,
                "change_type": "column_removed",
                "severity": "BREAKING",
                "detail": f"Column '{col}' was present in the old snapshot but is absent in the new one.",
                "old_value": str(old_cols[col].get("dtype")),
                "new_value": None,
            })

    # Columns added (COMPATIBLE)
    for col in new_cols:
        if col not in old_cols:
            changes.append({
                "column": col,
                "change_type": "column_added",
                "severity": "COMPATIBLE",
                "detail": f"New column '{col}' added — consumers can safely ignore.",
                "old_value": None,
                "new_value": str(new_cols[col].get("dtype")),
            })

    # Columns present in both — check for mutations
    for col in old_cols:
        if col not in new_cols:
            continue  # already handled above

        old_c = old_cols[col]
        new_c = new_cols[col]

        # Type change (BREAKING or CRITICAL for narrow-type)
        old_dtype = str(old_c.get("dtype", ""))
        new_dtype = str(new_c.get("dtype", ""))
        if old_dtype != new_dtype:
            # Check for narrow-type (precision loss) — escalate to CRITICAL
            is_narrow = any(
                old_dtype.lower().startswith(old_kw) and new_dtype.lower().startswith(new_kw)
                for old_kw, new_kw in NARROW_TYPE_PAIRS
            )
            severity = "CRITICAL" if is_narrow else "BREAKING"
            detail = (
                f"NARROW-TYPE: dtype changed from '{old_dtype}' to '{new_dtype}' — "
                f"precision loss (e.g. float->int truncates decimals). Consumers expecting "
                f"fractional values will receive truncated data."
                if is_narrow else
                f"dtype changed from '{old_dtype}' to '{new_dtype}'."
            )
            changes.append({
                "column": col,
                "change_type": "type_changed",
                "severity": severity,
                "detail": detail,
                "old_value": old_dtype,
                "new_value": new_dtype,
            })

        # Nullable tightened (BREAKING) — null_fraction went from >0 to 0
        old_null = old_c.get("null_fraction", 0.0) or 0.0
        new_null = new_c.get("null_fraction", 0.0) or 0.0
        if old_null > 0.0 and new_null == 0.0:
            changes.append({
                "column": col,
                "change_type": "nullable_tightened",
                "severity": "BREAKING",
                "detail": f"null_fraction dropped from {old_null:.3f} to 0 — column is now effectively NOT NULL.",
                "old_value": old_null,
                "new_value": new_null,
            })

        # Nullable relaxed (COMPATIBLE) — null_fraction went from 0 to >0
        if old_null == 0.0 and new_null > 0.0:
            changes.append({
                "column": col,
                "change_type": "nullable_relaxed",
                "severity": "COMPATIBLE",
                "detail": f"null_fraction rose from 0 to {new_null:.3f} — column now allows NULLs.",
                "old_value": old_null,
                "new_value": new_null,
            })

        # Cardinality spike (WARN) — grew >5×
        old_card = old_c.get("cardinality", 0) or 0
        new_card = new_c.get("cardinality", 0) or 0
        if old_card > 0 and new_card > old_card * 5:
            changes.append({
                "column": col,
                "change_type": "cardinality_spike",
                "severity": "WARN",
                "detail": f"Cardinality grew {old_card} → {new_card} (×{new_card/old_card:.1f}). Enum constraint may be violated.",
                "old_value": old_card,
                "new_value": new_card,
            })

        # Numeric range narrowing (BREAKING)
        old_stats = old_c.get("stats") or {}
        new_stats = new_c.get("stats") or {}
        if old_stats and new_stats:
            old_min = old_stats.get("min")
            new_min = new_stats.get("min")
            old_max = old_stats.get("max")
            new_max = new_stats.get("max")

            if old_min is not None and new_min is not None and new_min > old_min:
                changes.append({
                    "column": col,
                    "change_type": "range_narrowed",
                    "severity": "BREAKING",
                    "detail": f"min increased {old_min} → {new_min}. Records previously valid may now fail range checks.",
                    "old_value": old_min,
                    "new_value": new_min,
                })

            if old_max is not None and new_max is not None and new_max < old_max:
                changes.append({
                    "column": col,
                    "change_type": "range_narrowed",
                    "severity": "BREAKING",
                    "detail": f"max decreased {old_max} → {new_max}. Records previously valid may now fail range checks.",
                    "old_value": old_max,
                    "new_value": new_max,
                })

            # Stat drift (WARN) — mean shifted >2σ
            old_mean = old_stats.get("mean")
            new_mean = new_stats.get("mean")
            old_std  = old_stats.get("stddev")
            if old_mean is not None and new_mean is not None and old_std:
                drift = abs(new_mean - old_mean) / (old_std + 1e-9)
                if drift > 2.0:
                    changes.append({
                        "column": col,
                        "change_type": "stat_drift",
                        "severity": "WARN",
                        "detail": f"mean shifted {old_mean:.4f} → {new_mean:.4f} ({drift:.1f}σ). Possible data distribution change.",
                        "old_value": old_mean,
                        "new_value": new_mean,
                    })

        # Enum value changes — compare sample_values or enum lists
        old_enum = set(old_c.get("enum", old_c.get("sample_values", [])) or [])
        new_enum = set(new_c.get("enum", new_c.get("sample_values", [])) or [])
        if old_enum and new_enum:
            removed_vals = old_enum - new_enum
            added_vals = new_enum - old_enum
            if removed_vals:
                changes.append({
                    "column": col,
                    "change_type": "enum_values_removed",
                    "severity": "BREAKING",
                    "detail": f"Enum values removed: {sorted(removed_vals)}. Consumers relying on these values will break.",
                    "old_value": sorted(old_enum),
                    "new_value": sorted(new_enum),
                })
            if added_vals and not removed_vals:
                changes.append({
                    "column": col,
                    "change_type": "enum_values_added",
                    "severity": "COMPATIBLE",
                    "detail": f"Enum values added: {sorted(added_vals)}. Existing consumers unaffected.",
                    "old_value": sorted(old_enum),
                    "new_value": sorted(new_enum),
                })

    # Rename detection heuristic — removed + added columns with similar dtype/stats
    removed_cols = {col: old_cols[col] for col in old_cols if col not in new_cols}
    added_cols = {col: new_cols[col] for col in new_cols if col not in old_cols}
    for rem_name, rem_info in removed_cols.items():
        for add_name, add_info in added_cols.items():
            same_dtype = str(rem_info.get("dtype", "")) == str(add_info.get("dtype", ""))
            name_sim = SequenceMatcher(None, rem_name, add_name).ratio()
            if same_dtype and name_sim > 0.5:
                changes.append({
                    "column": f"{rem_name} -> {add_name}",
                    "change_type": "potential_rename",
                    "severity": "BREAKING",
                    "detail": (
                        f"Column '{rem_name}' removed and '{add_name}' added with same dtype "
                        f"'{rem_info.get('dtype')}' (name similarity: {name_sim:.0%}). "
                        f"Likely a rename — consumers referencing '{rem_name}' will break."
                    ),
                    "old_value": rem_name,
                    "new_value": add_name,
                })

    return changes


# ── migration impact summary ──────────────────────────────────────────────────

def load_registry(registry_path: Path = REGISTRY_PATH) -> dict:
    """Load consumer registry for impact analysis."""
    if not registry_path.exists():
        return {"subscriptions": []}
    with open(registry_path) as f:
        return yaml.safe_load(f) or {"subscriptions": []}


def migration_impact(
    changes: list[dict],
    old_snap: dict | None = None,
    registry: dict | None = None,
) -> dict:
    breaking  = [c for c in changes if c["severity"] in ("BREAKING", "CRITICAL")]
    criticals = [c for c in changes if c["severity"] == "CRITICAL"]
    warns     = [c for c in changes if c["severity"] == "WARN"]
    compat    = [c for c in changes if c["severity"] == "COMPATIBLE"]

    # Rollback plan — data-driven based on changes detected
    rollback_steps = []
    if breaking or criticals:
        snap_id = old_snap.get("snapshot_id", "unknown") if old_snap else "unknown"
        rollback_steps.append(f"Revert to snapshot {snap_id}")
        rollback_steps.append("Re-validate consumers against previous schema")
        if any(c["change_type"] == "type_changed" for c in breaking + criticals):
            rollback_steps.append("Cast affected columns back to original dtypes before re-deploy")
        if any(c["change_type"] == "column_removed" for c in breaking):
            rollback_steps.append("Restore removed columns from backup or prior snapshot")
        if any(c["change_type"] == "enum_values_removed" for c in breaking):
            rollback_steps.append("Re-add removed enum values to maintain consumer compatibility")
        rollback_steps.append("Run full contract validation suite after rollback")

    # Per-consumer failure mode analysis
    consumer_impact = []
    if registry and breaking:
        for sub in registry.get("subscriptions", []):
            affected_fields = []
            for c in breaking + criticals:
                col = c["column"]
                # Check if this subscriber consumes the affected column
                consumed = sub.get("fields_consumed", [])
                breaking_flds = [bf["field"] for bf in sub.get("breaking_fields", [])]
                if col in consumed or col in breaking_flds or any(col.startswith(f) for f in consumed):
                    reason = next(
                        (bf["reason"] for bf in sub.get("breaking_fields", []) if bf["field"] == col),
                        f"Consumes field '{col}' which has a {c['change_type']} change",
                    )
                    affected_fields.append({
                        "field": col,
                        "change_type": c["change_type"],
                        "severity": c["severity"],
                        "reason": reason,
                    })
            if affected_fields:
                consumer_impact.append({
                    "subscriber_id": sub["subscriber_id"],
                    "subscriber_team": sub.get("subscriber_team", "unknown"),
                    "contact": sub.get("contact", "unknown"),
                    "validation_mode": sub.get("validation_mode", "AUDIT"),
                    "affected_fields": affected_fields,
                })

    result = {
        "total_changes":      len(changes),
        "breaking_count":     len(breaking),
        "critical_count":     len(criticals),
        "warn_count":         len(warns),
        "compatible_count":   len(compat),
        "migration_required": len(breaking) > 0,
        "breaking_columns":   [c["column"] for c in breaking],
        "recommendation": (
            "BLOCK deployment — CRITICAL narrow-type changes detected. Immediate rollback required."
            if criticals else
            "BLOCK deployment — breaking changes detected. Run migration scripts before promoting."
            if breaking else
            "WARN — review distribution shifts before promoting."
            if warns else
            "SAFE to deploy — no breaking changes."
        ),
        "rollback_plan": rollback_steps,
        "consumer_impact": consumer_impact,
    }

    return result


# ── main analysis ─────────────────────────────────────────────────────────────

def analyze(
    contract_id: str,
    snapshots_dir: Path,
    output_dir: Path,
    since: str | None = None,
    registry_path: Path | None = None,
) -> list[dict]:
    snap_dir = snapshots_dir / contract_id
    if not snap_dir.exists():
        print(f"[analyzer] No snapshot directory for {contract_id}")
        return []

    snap_files = sorted(snap_dir.glob("*.yaml"))

    # Filter by --since date if provided
    if since:
        since_dt = datetime.fromisoformat(since)
        if since_dt.tzinfo is None:
            since_dt = since_dt.replace(tzinfo=timezone.utc)
        filtered = []
        for sf in snap_files:
            snap = load_snapshot(sf)
            captured = snap.get("captured_at", "")
            if captured:
                cap_str = str(captured).replace("'", "")
                try:
                    cap_dt = datetime.fromisoformat(cap_str.replace("Z", "+00:00"))
                    if cap_dt >= since_dt:
                        filtered.append(sf)
                except ValueError:
                    filtered.append(sf)  # include if can't parse
            else:
                filtered.append(sf)
        snap_files = filtered

    if len(snap_files) < 2:
        print(f"[analyzer] Need at least 2 snapshots to diff — found {len(snap_files)}")
        return []

    print(f"\n[analyzer] contract : {contract_id}")
    print(f"[analyzer] snapshots: {len(snap_files)} (diffing consecutive pairs)")

    # Load consumer registry for per-consumer failure analysis
    reg_path = registry_path or REGISTRY_PATH
    registry = load_registry(reg_path)

    output_dir.mkdir(parents=True, exist_ok=True)
    reports = []

    for i in range(len(snap_files) - 1):
        old_path = snap_files[i]
        new_path = snap_files[i + 1]

        old_snap = load_snapshot(old_path)
        new_snap = load_snapshot(new_path)

        changes = classify_changes(old_snap, new_snap)
        impact  = migration_impact(changes, old_snap=old_snap, registry=registry)

        report = {
            "report_id":    rng_uuid(),
            "contract_id":  contract_id,
            "analyzed_at":  now_iso(),
            "old_snapshot": {
                "snapshot_id":  old_snap.get("snapshot_id"),
                "captured_at":  old_snap.get("captured_at"),
                "file":         old_path.name,
            },
            "new_snapshot": {
                "snapshot_id":  new_snap.get("snapshot_id"),
                "captured_at":  new_snap.get("captured_at"),
                "file":         new_path.name,
            },
            "impact":   impact,
            "changes":  changes,
        }
        reports.append(report)

        # ── print summary ─────────────────────────────────────────────────────
        print(f"\n  diff: {old_path.name} → {new_path.name}")
        print(f"  changes: {impact['total_changes']} total  "
              f"({impact['breaking_count']} BREAKING, "
              f"{impact['warn_count']} WARN, "
              f"{impact['compatible_count']} COMPATIBLE)")
        print(f"  recommendation: {impact['recommendation']}")
        if impact["breaking_columns"]:
            print(f"  breaking columns: {', '.join(impact['breaking_columns'])}")
        for c in changes:
            marker = {"CRITICAL": "✗✗", "BREAKING": "✗", "WARN": "⚠", "COMPATIBLE": "✓"}.get(c["severity"], "?")
            print(f"    {marker} [{c['severity']:10s}] {c['column']:40s} {c['change_type']}")

    # Write report
    out_path = output_dir / f"{contract_id}_evolution.jsonl"
    with open(out_path, "w") as f:
        for r in reports:
            f.write(json.dumps(r) + "\n")

    print(f"\n[analyzer] wrote {len(reports)} diff report(s) → {out_path}")
    return reports


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SchemaEvolutionAnalyzer — diff snapshots, classify changes")
    parser.add_argument("--contract-id", default=None, help="Contract ID to analyze")
    parser.add_argument("--snapshots",   default="schema_snapshots/", help="Snapshot directory root")
    parser.add_argument("--output",      default="migration_reports/", help="Output directory")
    parser.add_argument("--all",         action="store_true", help="Analyze all contracts in snapshot dir")
    parser.add_argument("--since",       default=None, help="Only diff snapshots after this ISO date (e.g. 2025-01-01)")
    parser.add_argument("--registry",    default=None, help="Path to consumer registry YAML (default: contract_registry/subscriptions.yaml)")
    args = parser.parse_args()

    snap_root = ROOT / args.snapshots
    out_path  = ROOT / args.output
    reg_path  = Path(args.registry) if args.registry else REGISTRY_PATH

    # If --output ends with .json, collect all reports into a single file
    single_file = str(out_path).endswith(".json")
    out_root = out_path.parent if single_file else out_path

    all_reports = []
    if args.all or not args.contract_id:
        contract_dirs = [d for d in snap_root.iterdir() if d.is_dir()]
        for d in sorted(contract_dirs):
            reports = analyze(d.name, snap_root, out_root, since=args.since, registry_path=reg_path)
            if single_file and reports:
                all_reports.extend(reports)
    else:
        reports = analyze(args.contract_id, snap_root, out_root, since=args.since, registry_path=reg_path)
        if single_file and reports:
            all_reports.extend(reports)

    if single_file:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w") as f:
            json.dump(all_reports, f, indent=2)
        print(f"\n[analyzer] combined report → {out_path}")
