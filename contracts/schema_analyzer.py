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
from pathlib import Path

import yaml

ROOT = Path(__file__).parent.parent


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

        # Type change (BREAKING)
        old_dtype = str(old_c.get("dtype", ""))
        new_dtype = str(new_c.get("dtype", ""))
        if old_dtype != new_dtype:
            changes.append({
                "column": col,
                "change_type": "type_changed",
                "severity": "BREAKING",
                "detail": f"dtype changed from '{old_dtype}' to '{new_dtype}'.",
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

    return changes


# ── migration impact summary ──────────────────────────────────────────────────

def migration_impact(changes: list[dict]) -> dict:
    breaking = [c for c in changes if c["severity"] == "BREAKING"]
    warns    = [c for c in changes if c["severity"] == "WARN"]
    compat   = [c for c in changes if c["severity"] == "COMPATIBLE"]

    return {
        "total_changes":     len(changes),
        "breaking_count":    len(breaking),
        "warn_count":        len(warns),
        "compatible_count":  len(compat),
        "migration_required": len(breaking) > 0,
        "breaking_columns":  [c["column"] for c in breaking],
        "recommendation": (
            "BLOCK deployment — breaking changes detected. Run migration scripts before promoting."
            if breaking else
            "WARN — review distribution shifts before promoting."
            if warns else
            "SAFE to deploy — no breaking changes."
        ),
    }


# ── main analysis ─────────────────────────────────────────────────────────────

def analyze(contract_id: str, snapshots_dir: Path, output_dir: Path) -> list[dict]:
    snap_dir = snapshots_dir / contract_id
    if not snap_dir.exists():
        print(f"[analyzer] No snapshot directory for {contract_id}")
        return []

    snap_files = sorted(snap_dir.glob("*.yaml"))
    if len(snap_files) < 2:
        print(f"[analyzer] Need at least 2 snapshots to diff — found {len(snap_files)}")
        return []

    print(f"\n[analyzer] contract : {contract_id}")
    print(f"[analyzer] snapshots: {len(snap_files)} (diffing consecutive pairs)")

    output_dir.mkdir(parents=True, exist_ok=True)
    reports = []

    for i in range(len(snap_files) - 1):
        old_path = snap_files[i]
        new_path = snap_files[i + 1]

        old_snap = load_snapshot(old_path)
        new_snap = load_snapshot(new_path)

        changes = classify_changes(old_snap, new_snap)
        impact  = migration_impact(changes)

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
            marker = {"BREAKING": "✗", "WARN": "⚠", "COMPATIBLE": "✓"}.get(c["severity"], "?")
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
    args = parser.parse_args()

    snap_root = ROOT / args.snapshots
    out_root  = ROOT / args.output

    if args.all or not args.contract_id:
        contract_dirs = [d for d in snap_root.iterdir() if d.is_dir()]
        for d in sorted(contract_dirs):
            analyze(d.name, snap_root, out_root)
    else:
        analyze(args.contract_id, snap_root, out_root)
