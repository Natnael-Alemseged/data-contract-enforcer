"""
migrate_to_canonical.py
=======================
Converts each week's actual output schema to the canonical spec schema
defined in the Data Contract Enforcer challenge document.

Deviations from canonical are documented inline with DEVIATION: comments.

Run:  python scripts/migrate_to_canonical.py
"""

import json
import uuid
import hashlib
import random
from datetime import datetime, timezone
from pathlib import Path
from collections import defaultdict

ROOT = Path(__file__).parent.parent
OUTPUTS = ROOT / "outputs"

random.seed(99)


def rng_uuid() -> str:
    return str(uuid.uuid4())


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def sha256(s: str) -> str:
    return hashlib.sha256(s.encode()).hexdigest()


# ─── WEEK 1: generate intent_records from real codebase context ───────────────
# No actual output exists; generating from known Week 1 (roo-code fork) context.

WEEK1_INTENTS = [
    ("Track which intent triggered which code change", "intent_tracking", ["intent", "traceability"],
     [("src/intent_correlator/correlator.py", 45, 82, "correlate_intent_to_code", 0.91)]),
    ("Map natural-language descriptions to git commits", "commit_mapping", ["git", "traceability"],
     [("src/intent_correlator/git_bridge.py", 12, 38, "map_description_to_commit", 0.88)]),
    ("Validate that every code symbol has a documented intent", "symbol_coverage", ["governance", "documentation"],
     [("src/intent_correlator/validator.py", 22, 55, "validate_symbol_coverage", 0.79)]),
    ("Persist intent records to JSONL for downstream consumers", "persistence", ["storage", "pipeline"],
     [("src/intent_correlator/store.py", 8, 29, "write_intent_record", 0.95)]),
    ("Extract governance tags from intent descriptions using Claude", "tag_extraction", ["pii", "governance"],
     [("src/intent_correlator/tagger.py", 15, 60, "extract_governance_tags", 0.84)]),
    ("Compute confidence score for intent-code match", "confidence_scoring", ["quality", "traceability"],
     [("src/intent_correlator/scorer.py", 30, 78, "compute_match_confidence", 0.87)]),
    ("CLI entry point for the intent correlator tool", "cli", ["interface"],
     [("src/intent_correlator/__main__.py", 1, 35, "main", 0.97)]),
    ("Load repository symbols for matching via tree-sitter", "symbol_loading", ["parsing"],
     [("src/intent_correlator/symbol_loader.py", 18, 67, "load_symbols", 0.82),
      ("src/intent_correlator/symbol_loader.py", 70, 95, "filter_public_symbols", 0.76)]),
    ("Batch-process multiple intent descriptions against a codebase", "batch_processing", ["pipeline"],
     [("src/intent_correlator/batch.py", 5, 42, "run_batch", 0.89)]),
    ("Generate final intent report in markdown and JSONL", "report_generation", ["documentation"],
     [("src/intent_correlator/reporter.py", 20, 61, "generate_report", 0.83)]),
    ("Cache LLM calls to avoid redundant API costs", "llm_caching", ["cost", "performance"],
     [("src/intent_correlator/llm_cache.py", 10, 48, "get_or_call_llm", 0.78)]),
    ("Parse .intentrc config file for project-level settings", "config_loading", ["configuration"],
     [("src/intent_correlator/config.py", 5, 33, "load_config", 0.93)]),
]

BASE_DT = datetime(2025, 8, 1, tzinfo=timezone.utc)


def write_week1():
    dst = OUTPUTS / "week1" / "intent_records.jsonl"
    dst.parent.mkdir(parents=True, exist_ok=True)

    records = []
    for desc, symbol_hint, tags, refs in WEEK1_INTENTS:
        code_refs = [
            {
                "file": file,
                "line_start": ls,
                "line_end": le,
                "symbol": sym,
                "confidence": conf,
            }
            for file, ls, le, sym, conf in refs
        ]
        offset_days = random.randint(0, 45)
        created = BASE_DT.replace(day=BASE_DT.day + min(offset_days, 28))
        records.append({
            "intent_id": rng_uuid(),
            "description": desc,
            "code_refs": code_refs,
            "governance_tags": tags,
            "created_at": created.isoformat().replace("+00:00", "Z"),
        })

    with open(dst, "w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")
    print(f"[week1] generated {len(records)} intent records → {dst}")


# ─── WEEK 2: Github-Evaluator real output → canonical verdict_record ─────────
# Actual: flat rows, one per dimension, all for same repo in this run
# DEVIATION: actual is flat (one row per dimension); canonical groups all dimensions per verdict
# DEVIATION: actual uses judge_scores{Prosecutor, Defense, TechLead} not judge_opinions[]
# DEVIATION: actual has no rubric_id; derived from sha256 of rubric.json content
# DEVIATION: actual scores are integer 1-5 already (canonical also int 1-5) ✓
# DEVIATION: actual has no confidence; derived from overall_score / 5.0

def migrate_week2():
    src = OUTPUTS / "week2" / "verdicts.jsonl"
    rubric_path = OUTPUTS / "week2" / "rubric.json"
    dst = OUTPUTS / "week2" / "verdicts_canonical.jsonl"

    if not src.exists():
        print(f"[week2] {src} not found")
        return

    # Derive rubric_id from actual rubric file
    rubric_id = sha256_file(rubric_path) if rubric_path.exists() else sha256("rubric-v3.0.0")
    rubric_version = "3.0.0"

    # Group flat rows by (repo_url, generated_at) — one verdict per evaluation run
    groups: dict[tuple, list] = defaultdict(list)
    with open(src) as f:
        for line in f:
            r = json.loads(line)
            key = (r["repo_url"], r["generated_at"])
            groups[key].append(r)

    out = []
    for (repo_url, generated_at), rows in groups.items():
        scores = {}
        for row in rows:
            # Average judge scores → canonical score int 1-5
            judge_vals = list(row["judge_scores"].values())
            avg = sum(judge_vals) / len(judge_vals)
            scores[row["dimension_id"]] = {
                "score": int(round(avg)),
                "evidence": [f"{j}: {s}/5" for j, s in row["judge_scores"].items()],
                "notes": row.get("dissent_summary", ""),
            }

        overall_score = rows[0]["overall_score"]
        # Map verdict string → canonical enum (PASS/FAIL/WARN)
        raw_verdict = rows[0].get("verdict", "PASS").upper()
        verdict_map = {"PASS": "PASS", "FAIL": "FAIL", "WARN": "WARN",
                       "WARNING": "WARN", "PARTIAL": "WARN"}
        overall_verdict = verdict_map.get(raw_verdict, "PASS")

        out.append({
            "verdict_id": rng_uuid(),
            "target_ref": repo_url,
            "rubric_id": rubric_id,
            "rubric_version": rubric_version,
            "scores": scores,
            "overall_verdict": overall_verdict,
            "overall_score": round(overall_score, 2),
            "confidence": round(min(overall_score / 5.0, 1.0), 2),
            "evaluated_at": generated_at,
        })

    with open(dst, "w") as f:
        for r in out:
            f.write(json.dumps(r) + "\n")
    print(f"[week2] migrated {len(out)} verdict records ({len(rows)} dimensions each) → {dst}")


# ─── WEEK 3: extraction_ledger + synthetic extractions → canonical ────────────
# extraction_ledger.jsonl has 1 run-metadata record (strategy, cost, time_ms, status)
# DEVIATION: actual ledger tracks run metadata not document extractions
# DEVIATION: no confidence per fact, no entity extraction, no source_hash in actual
# Action: use synthetic extractions.jsonl (already canonical-shaped); document this deviation

def migrate_week3():
    ledger = OUTPUTS / "week3" / "extraction_ledger.jsonl"
    synthetic = OUTPUTS / "week3" / "extractions.jsonl"

    if not synthetic.exists():
        print(f"[week3] {synthetic} not found — run generate_outputs.py first")
        return

    # extractions.jsonl already has canonical shape from generate+migrate pipeline
    count = sum(1 for _ in open(synthetic))
    print(f"[week3] extractions.jsonl already canonical ({count} records) ✓")

    if ledger.exists():
        r = json.loads(open(ledger).readline())
        print(f"[week3] extraction_ledger has 1 run-metadata record: strategy={r['strategy']}, "
              f"confidence={r['confidence']}, status={r['status']}")
        print(f"[week3] DEVIATION: actual ledger is run-metadata only, not document extractions")
        print(f"[week3] Using synthetic extractions.jsonl for contract enforcement")


# ─── WEEK 4: real .cartography/lineage_graph.json → canonical snapshot ────────
# DEVIATION: actual uses bipartite transformation/dataset nodes (not FILE/TABLE)
# DEVIATION: actual node IDs: datasets use plain name, transformations use "transformation:file:range#n"
# DEVIATION: actual has no snapshot_id, codebase_root, git_commit, captured_at at top level
# DEVIATION: actual uses 'edges' key (spec uses 'edges' too) ✓
# DEVIATION: edge has edge_type, source_file, line_range alongside source/target

def migrate_week4():
    src = OUTPUTS / "week4" / ".cartography" / "lineage_graph.json"
    dst = OUTPUTS / "week4" / "lineage_snapshots.jsonl"

    if not src.exists():
        print(f"[week4] {src} not found")
        return

    with open(src) as f:
        raw = json.load(f)

    # Map actual node_type → canonical type enum
    node_type_map = {
        "dataset": "TABLE",
        "transformation": "MODEL",
    }

    nodes = []
    for n in raw["nodes"]:
        node_id = n.get("id") or n.get("name", "unknown")
        actual_type = n.get("node_type", "dataset")
        canonical_type = node_type_map.get(actual_type, "FILE")
        label = n.get("name") or node_id.split("/")[-1]
        nodes.append({
            "node_id": node_id,
            "type": canonical_type,
            "label": label,
            "metadata": {
                "path": n.get("source_file", n.get("name", "")),
                "language": "sql" if n.get("transformation_type") == "sql" else "unknown",
                "purpose": f"{actual_type}: {n.get('name', node_id)}",
                "last_modified": "",
            },
        })

    # Canonical relationship map
    rel_map = {
        "PRODUCES": "PRODUCES",
        "CONSUMES": "CONSUMES",
        "IMPORTS": "IMPORTS",
        "REFERENCES_SQL": "READS",
        "REFERENCES_CONFIG": "READS",
    }

    edges = []
    for e in raw.get("edges", []):
        edges.append({
            "source": e["source"],
            "target": e["target"],
            "relationship": rel_map.get(e.get("edge_type", "CONSUMES"), "CONSUMES"),
            "confidence": 0.95,  # DEVIATION: actual has no confidence on edges
        })

    # Use earliest git commit from git_history.txt as snapshot commit
    git_history_path = OUTPUTS / "week4" / ".cartography" / "git_history.txt"
    git_commit = "f72efd2a" + "0" * 32  # padded to 40 chars from real git log
    if git_history_path.exists():
        first_line = git_history_path.read_text().strip().splitlines()[-1]
        short_hash = first_line.split()[0]
        git_commit = short_hash.ljust(40, "0")

    snapshot = {
        "snapshot_id": rng_uuid(),
        "codebase_root": "/workspace/jaffle_shop_classic",
        "git_commit": git_commit,
        "nodes": nodes,
        "edges": edges,
        "captured_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }

    with open(dst, "w") as f:
        f.write(json.dumps(snapshot) + "\n")
    print(f"[week4] migrated real lineage graph ({len(nodes)} nodes, {len(edges)} edges) → {dst}")


# ─── WEEK 5: real events.jsonl → canonical event_record ─────────────────────
# Actual: {event_id, stream_id, stream_position, global_position, event_type,
#          event_version, payload, metadata, recorded_at}
# DEVIATION: uses stream_id instead of aggregate_id (same concept, different name)
# DEVIATION: uses stream_position instead of sequence_number ✓ (same semantics)
# DEVIATION: has global_position (extra field, not in canonical)
# DEVIATION: metadata is {} (empty object); canonical has structured metadata subfields
# DEVIATION: no aggregate_type field; derived from stream_id prefix
# DEVIATION: no occurred_at; using recorded_at for both (seed data has no separate occurred_at)
# DEVIATION: schema_version is int (event_version); canonical expects string

AGGREGATE_TYPE_MAP = {
    "loan": "LoanApplication",
    "docpkg": "DocumentPackage",
    "agent": "AgentSession",
    "credit": "CreditRecord",
    "compliance": "ComplianceRecord",
    "fraud": "FraudScreening",
    "audit": "AuditLedger",
}


def migrate_week5():
    src = OUTPUTS / "week5" / "events.jsonl"
    dst = OUTPUTS / "week5" / "events_canonical.jsonl"

    if not src.exists():
        print(f"[week5] {src} not found")
        return

    out = []
    with open(src) as f:
        for line in f:
            r = json.loads(line)
            stream_id = r["stream_id"]
            prefix = stream_id.split("-")[0]
            aggregate_type = AGGREGATE_TYPE_MAP.get(prefix, "Unknown")

            out.append({
                "event_id": r["event_id"],
                "event_type": r["event_type"],
                "aggregate_id": stream_id,           # DEVIATION: mapped from stream_id
                "aggregate_type": aggregate_type,
                "sequence_number": r["stream_position"],  # DEVIATION: mapped from stream_position
                "payload": r.get("payload", {}),
                "metadata": {
                    "causation_id": None,            # DEVIATION: not in actual
                    "correlation_id": rng_uuid(),     # DEVIATION: generated
                    "user_id": "system",              # DEVIATION: not in actual
                    "source_service": "week5-axiom-ledger",
                },
                "schema_version": str(r.get("event_version", 1)),  # DEVIATION: int→string
                "occurred_at": r["recorded_at"],     # DEVIATION: no separate occurred_at
                "recorded_at": r["recorded_at"],
            })

    with open(dst, "w") as f:
        for r in out:
            f.write(json.dumps(r) + "\n")
    print(f"[week5] migrated {len(out)} real event records → {dst}")


# ─── MAIN ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("Migrating to canonical schemas...\n")
    write_week1()
    migrate_week2()
    migrate_week3()
    migrate_week4()
    migrate_week5()
    print("\nDone.")
    print("\nSummary of deviations from canonical spec:")
    print("  week1: generated (no real output from roo-code fork)")
    print("  week2: flat rows grouped into 1 verdict; rubric_id from real rubric.json SHA")
    print("  week3: actual ledger is run-metadata only; using synthetic extraction records")
    print("  week4: bipartite nodes mapped to TABLE/MODEL; edge confidence hardcoded 0.95")
    print("  week5: stream_id→aggregate_id, stream_position→sequence_number, occurred_at=recorded_at")
