"""
contracts/attributor.py — ViolationAttributor (Phase 2B)
=========================================================
When a ValidationRunner report contains failures, traces each violation back
to its origin via lineage graph traversal + git blame.

Usage:
    python contracts/attributor.py \
        --report validation_reports/week3-document-refinery-extractions_confidence_scale_*.json \
        --lineage outputs/week4/lineage_snapshots.jsonl \
        --repo-path /path/to/source/repo \
        --output violation_log/
"""

import argparse
import json
import re
import subprocess
import uuid
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path

import yaml

ROOT = Path(__file__).parent.parent
LINEAGE_PATH = ROOT / "outputs" / "week4" / "lineage_snapshots.jsonl"

# ── helpers ───────────────────────────────────────────────────────────────────

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def rng_uuid() -> str:
    return str(uuid.uuid4())


# ── lineage graph loading ─────────────────────────────────────────────────────

def load_lineage_graph(lineage_path: Path) -> dict:
    """Load latest snapshot; build adjacency maps for BFS."""
    if not lineage_path.exists():
        return {"nodes": {}, "edges": [], "produces": defaultdict(list), "consumes": defaultdict(list)}

    lines = lineage_path.read_text().strip().splitlines()
    snapshot = json.loads(lines[-1])

    nodes = {n["node_id"]: n for n in snapshot.get("nodes", [])}
    edges = snapshot.get("edges", [])

    # PRODUCES: source → [target, ...]  (transformation produces dataset)
    # CONSUMES: target → [source, ...]  (transformation consumes dataset)
    produces = defaultdict(list)   # node_id → nodes it produces
    consumes = defaultdict(list)   # node_id → nodes it consumes (upstream)
    consumed_by = defaultdict(list) # dataset → transformations that consume it

    for e in edges:
        rel = e.get("relationship", "")
        src, tgt = e.get("source", ""), e.get("target", "")
        if rel == "PRODUCES":
            produces[src].append(tgt)
        elif rel == "CONSUMES":
            consumes[tgt].append(src)
            consumed_by[src].append(tgt)

    return {
        "nodes": nodes,
        "edges": edges,
        "produces": produces,
        "consumes": consumes,
        "consumed_by": consumed_by,
        "snapshot": snapshot,
    }


# ── reverse BFS blame traversal ───────────────────────────────────────────────

def find_upstream_files(failing_column: str, graph: dict) -> list[str]:
    """
    Starting from the failing column/table, traverse upstream via CONSUMES/PRODUCES
    edges to find source files that likely produce this data.
    Returns list of file paths for git blame.
    """
    nodes = graph["nodes"]
    produces = graph["produces"]
    consumes = graph["consumes"]

    # Anchor: find nodes whose path or label matches the failing column hints
    col_parts = failing_column.lower().replace(".", "_").split("_")
    keywords = [p for p in col_parts if len(p) > 3]

    # Start from nodes that match the column's likely origin
    anchor_nodes = []
    for node_id, node in nodes.items():
        path = node.get("metadata", {}).get("path", "").lower()
        label = node.get("label", "").lower()
        if any(kw in path or kw in label for kw in keywords):
            anchor_nodes.append(node_id)

    if not anchor_nodes:
        # Fall back to all source files
        anchor_nodes = [nid for nid, n in nodes.items() if n.get("type") in ("FILE", "MODEL")]

    # BFS upstream
    visited = set()
    queue = deque(anchor_nodes)
    source_files = []

    while queue:
        node_id = queue.popleft()
        if node_id in visited:
            continue
        visited.add(node_id)

        node = nodes.get(node_id, {})
        path = node.get("metadata", {}).get("path", "")
        if path and path.endswith((".py", ".sql", ".ts", ".js")):
            source_files.append(path)

        # Go upstream: find what produces/consumes this node
        for upstream in consumes.get(node_id, []):
            if upstream not in visited:
                queue.append(upstream)

    return source_files[:5]  # cap at 5 per spec


# ── git blame integration ──────────────────────────────────────────────────────

def run_git_log(repo_path: Path, file_path: str) -> list[dict]:
    """Run git log on a file, return list of {hash, author, email, timestamp, message}."""
    try:
        result = subprocess.run(
            ["git", "log", "--follow", "--since=90 days ago",
             "--format=%H|%an|%ae|%ai|%s", "--", file_path],
            cwd=repo_path, capture_output=True, text=True, timeout=15
        )
        commits = []
        for line in result.stdout.strip().splitlines():
            parts = line.split("|", 4)
            if len(parts) == 5:
                commits.append({
                    "hash": parts[0],
                    "author": parts[1],
                    "email": parts[2],
                    "timestamp": parts[3],
                    "message": parts[4],
                })
        return commits
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return []


def run_git_log_from_history(history_text: str, file_hint: str) -> list[dict]:
    """
    Parse pre-exported git history text (from .cartography/git_history.txt)
    when the source repo is not cloned locally.
    """
    commits = []
    for line in history_text.strip().splitlines():
        parts = line.strip().split(" ", 1)
        if len(parts) == 2:
            short_hash, message = parts
            commits.append({
                "hash": short_hash.ljust(40, "0"),
                "author": "unknown (from git_history.txt)",
                "email": "unknown",
                "timestamp": now_iso(),
                "message": message,
            })
    return commits[:10]


def confidence_score(days_since: float, lineage_hops: int) -> float:
    """
    Per spec: base = 1.0 − (days_since_commit × 0.1).
    Reduce by 0.2 per lineage hop. Floor at 0.1.
    """
    score = 1.0 - (days_since * 0.1) - (lineage_hops * 0.2)
    return round(max(score, 0.1), 2)


def build_blame_chain(commits: list[dict], lineage_hops: int) -> list[dict]:
    """Rank commits by recency and hops, return top 5."""
    chain = []
    for i, c in enumerate(commits[:5]):
        try:
            ts = datetime.fromisoformat(c["timestamp"].replace(" ", "T").split("+")[0])
            ts = ts.replace(tzinfo=timezone.utc)
            days = (datetime.now(timezone.utc) - ts).days
        except Exception:
            days = 30  # assume 30 days if unparseable

        chain.append({
            "rank":             i + 1,
            "file_path":        c.get("file_path", "unknown"),
            "commit_hash":      c["hash"],
            "author":           c["author"],
            "commit_timestamp": c["timestamp"],
            "commit_message":   c["message"],
            "confidence_score": confidence_score(days, lineage_hops),
        })
    return chain


# ── blast radius ──────────────────────────────────────────────────────────────

def compute_blast_radius(failing_column: str, graph: dict, records_failing: int) -> dict:
    """
    BFS forward from the failing node to find all affected downstream nodes.
    """
    nodes = graph["nodes"]
    produces = graph["produces"]
    consumed_by = graph["consumed_by"]

    col_parts = failing_column.lower().split(".")
    keywords = [p for p in col_parts if len(p) > 3]

    # Find anchor nodes
    anchors = []
    for node_id, node in nodes.items():
        path = node.get("metadata", {}).get("path", "").lower()
        if any(kw in path for kw in keywords):
            anchors.append(node_id)

    if not anchors:
        anchors = list(nodes.keys())[:2]

    visited = set()
    queue = deque(anchors)
    affected = []
    affected_pipelines = []

    while queue:
        node_id = queue.popleft()
        if node_id in visited:
            continue
        visited.add(node_id)
        node = nodes.get(node_id, {})
        affected.append(node_id)
        if node.get("type") == "MODEL":
            affected_pipelines.append(node_id)

        for downstream in produces.get(node_id, []):
            if downstream not in visited:
                queue.append(downstream)
        for downstream in consumed_by.get(node_id, []):
            if downstream not in visited:
                queue.append(downstream)

    return {
        "affected_nodes":     affected[:10],
        "affected_pipelines": affected_pipelines[:5],
        "estimated_records":  records_failing,
    }


# ── main attribution ──────────────────────────────────────────────────────────

def attribute_violations(
    report_path: Path,
    lineage_path: Path,
    repo_path: Path | None,
    output_dir: Path,
) -> list[dict]:
    output_dir.mkdir(parents=True, exist_ok=True)

    report = json.loads(report_path.read_text())
    contract_id = report["contract_id"]
    failures = [r for r in report["results"] if r["status"] in ("FAIL", "ERROR")]

    if not failures:
        print(f"[attributor] No failures in {report_path.name} — nothing to attribute.")
        return []

    print(f"\n[attributor] contract : {contract_id}")
    print(f"[attributor] failures : {len(failures)}")

    graph = load_lineage_graph(lineage_path)

    # Load git history fallback (from .cartography)
    git_history_text = ""
    git_history_path = ROOT / "outputs" / "week4" / ".cartography" / "git_history.txt"
    if git_history_path.exists():
        git_history_text = git_history_path.read_text()

    violations = []
    for failure in failures:
        col = failure["column_name"]
        check_id = failure["check_id"]
        records_failing = failure.get("records_failing", 0)

        print(f"  [attributor] attributing: {check_id}")

        # Find upstream files
        upstream_files = find_upstream_files(col, graph)
        lineage_hops = len(upstream_files) or 1

        # Get git commits
        all_commits = []
        if repo_path and repo_path.exists():
            for fp in upstream_files:
                commits = run_git_log(repo_path, fp)
                for c in commits:
                    c["file_path"] = fp
                all_commits.extend(commits)
        elif git_history_text:
            # Use pre-exported history
            raw = run_git_log_from_history(git_history_text, col)
            for c in raw:
                c["file_path"] = upstream_files[0] if upstream_files else "unknown"
            all_commits = raw

        if not all_commits:
            # Synthetic attribution when no git available
            all_commits = [{
                "hash":      "0" * 40,
                "author":    "unknown",
                "email":     "unknown",
                "timestamp": now_iso(),
                "message":   f"(no git history — contract violation in {col})",
                "file_path": upstream_files[0] if upstream_files else "unknown",
            }]

        blame_chain = build_blame_chain(all_commits, lineage_hops)
        blast_radius = compute_blast_radius(col, graph, records_failing)

        violation = {
            "violation_id":  rng_uuid(),
            "check_id":      check_id,
            "contract_id":   contract_id,
            "column_name":   col,
            "severity":      failure["severity"],
            "detected_at":   now_iso(),
            "failure_detail": {
                "actual_value":    failure["actual_value"],
                "expected":        failure["expected"],
                "message":         failure["message"],
                "records_failing": records_failing,
            },
            "blame_chain":   blame_chain,
            "blast_radius":  blast_radius,
        }
        violations.append(violation)

    # Write violation log
    out_path = output_dir / "violations.jsonl"
    with open(out_path, "a") as f:
        for v in violations:
            f.write(json.dumps(v) + "\n")

    print(f"\n[attributor] wrote {len(violations)} violation(s) → {out_path}")
    for v in violations:
        print(f"  violation_id={v['violation_id'][:8]}... check={v['check_id']}")
        if v["blame_chain"]:
            top = v["blame_chain"][0]
            print(f"    top blame: {top['commit_hash'][:8]} by {top['author']} "
                  f"(confidence={top['confidence_score']}) — {top['commit_message'][:60]}")
        print(f"    blast radius: {len(v['blast_radius']['affected_nodes'])} nodes, "
              f"~{v['blast_radius']['estimated_records']} records")

    return violations


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ViolationAttributor — trace violations to origin commits")
    parser.add_argument("--report",   required=True, help="Path to ValidationRunner JSON report")
    parser.add_argument("--lineage",  default=str(LINEAGE_PATH), help="Path to lineage_snapshots.jsonl")
    parser.add_argument("--repo-path", help="Path to cloned source repo for git blame (optional)")
    parser.add_argument("--output",   default="violation_log/", help="Output directory")
    args = parser.parse_args()

    # Support glob patterns in --report
    import glob
    report_files = glob.glob(args.report)
    if not report_files:
        print(f"No report files matched: {args.report}")
        exit(1)

    for rp in sorted(report_files):
        attribute_violations(
            report_path=Path(rp),
            lineage_path=Path(args.lineage),
            repo_path=Path(args.repo_path) if args.repo_path else None,
            output_dir=Path(args.output),
        )
