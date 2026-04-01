"""
generate_outputs.py
===================
Generates synthetic output data for weeks 2, 3, and 4 in their ACTUAL schemas
(as implemented, not the canonical spec schemas), then writes migration scripts
that convert each to the canonical spec format.

Run:  python scripts/generate_outputs.py
"""

import json
import random
import hashlib
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).parent.parent
OUTPUTS = ROOT / "outputs"

random.seed(42)


def rng_uuid() -> str:
    return str(uuid.uuid4())


def rng_dt(base: datetime = None, jitter_hours: int = 720) -> str:
    base = base or datetime(2025, 10, 1, tzinfo=timezone.utc)
    delta = timedelta(hours=random.randint(0, jitter_hours))
    return (base + delta).isoformat().replace("+00:00", "Z")


def sha256(s: str) -> str:
    return hashlib.sha256(s.encode()).hexdigest()


# ─── WEEK 2: Github-Evaluator actual schema ───────────────────────────────────
# Actual: {repo_url, repo_name, executive_summary, overall_score,
#          criteria[{dimension_id, dimension_name, final_score,
#                    judge_opinions[{judge, criterion_id, score, argument}]}],
#          remediation_plan, verified_paths, hallucinated_paths}

DIMENSIONS = [
    ("git_forensic_analysis", "Git Forensic Analysis"),
    ("code_quality", "Code Quality"),
    ("architecture_coherence", "Architecture Coherence"),
    ("test_coverage", "Test Coverage"),
    ("documentation_quality", "Documentation Quality"),
]

JUDGES = ["Prosecutor", "Defense", "Expert"]

REPOS = [
    ("https://github.com/Natnael-Alemseged/Document-Intelligence-Refinery-", "Document-Intelligence-Refinery"),
    ("https://github.com/Natnael-Alemseged/brownfield-cartographer", "brownfield-cartographer"),
    ("https://github.com/Natnael-Alemseged/Axiom-Ledger", "Axiom-Ledger"),
    ("https://github.com/Natnael-Alemseged/Github-Evaluator", "Github-Evaluator"),
]

ARGUMENTS = [
    "The commit history shows consistent, well-named commits following conventional commit format.",
    "Code structure is clean with clear separation of concerns across modules.",
    "Test coverage is present but lacks edge case handling for error conditions.",
    "Documentation is minimal; key classes and functions lack docstrings.",
    "Architecture follows a layered pattern but coupling between layers is too tight.",
    "Git log reveals frequent refactoring commits indicating unclear initial design.",
    "Public API surface is well-defined with Pydantic models for validation.",
    "Error handling is inconsistent; some paths raise bare exceptions.",
    "The module structure aligns well with domain concepts.",
    "Dependency injection is not used, making unit testing difficult.",
]


def make_week2_record(repo_url: str, repo_name: str) -> dict:
    num_dims = random.randint(2, 4)
    chosen = random.sample(DIMENSIONS, num_dims)
    criteria = []
    scores = []
    for dim_id, dim_name in chosen:
        judge_opinions = [
            {
                "judge": j,
                "criterion_id": dim_id,
                "score": random.randint(1, 5),
                "argument": random.choice(ARGUMENTS),
            }
            for j in JUDGES
        ]
        final_score = round(sum(o["score"] for o in judge_opinions) / len(judge_opinions), 1)
        scores.append(final_score)
        criteria.append(
            {
                "dimension_id": dim_id,
                "dimension_name": dim_name,
                "final_score": final_score,
                "judge_opinions": judge_opinions,
            }
        )
    overall = round(sum(scores) / len(scores), 1)
    verdict = "PASS" if overall >= 3.0 else ("WARN" if overall >= 2.0 else "FAIL")
    return {
        "repo_url": repo_url,
        "repo_name": repo_name,
        "executive_summary": f"Evaluation of {repo_name}. Overall score {overall}/5. Verdict: {verdict}.",
        "overall_score": overall,
        "criteria": criteria,
        "remediation_plan": f"Address issues in: {', '.join(d for d, _ in chosen[:2])}.",
        "verified_paths": [f"src/{repo_name.lower()}/main.py", "README.md", "pyproject.toml"],
        "hallucinated_paths": [],
        "_verdict": verdict,  # internal, used by migration
        "_evaluated_at": rng_dt(),
    }


def write_week2():
    path = OUTPUTS / "week2" / "verdicts_raw.jsonl"
    records = []
    for repo_url, repo_name in REPOS:
        for _ in range(5):  # 5 evaluations per repo over time
            records.append(make_week2_record(repo_url, repo_name))
    random.shuffle(records)
    with open(path, "w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")
    print(f"[week2] wrote {len(records)} records → {path}")
    return records


# ─── WEEK 3: Document-Intelligence-Refinery actual schema ─────────────────────
# Actual: FactRow{doc_id, page_ref, key, value, unit, bbox, content_hash,
#                 source_ldu_id}
# One row per extracted key-value fact (flat, not nested like canonical)

FACT_KEYS = [
    ("revenue", "USD"),
    ("net_income", "USD"),
    ("total_assets", "USD"),
    ("debt_ratio", None),
    ("employee_count", None),
    ("filing_date", None),
    ("company_name", None),
    ("fiscal_year", None),
    ("ebitda", "USD"),
    ("gross_margin", None),
    ("cash_equivalents", "USD"),
    ("operating_expenses", "USD"),
]

DOCUMENT_IDS = [rng_uuid() for _ in range(12)]


def make_week3_records(doc_id: str, num_facts: int = 8) -> list[dict]:
    chosen_keys = random.sample(FACT_KEYS, min(num_facts, len(FACT_KEYS)))
    records = []
    for key, unit in chosen_keys:
        page = random.randint(0, 15)
        value = (
            round(random.uniform(1e5, 1e9), 2) if unit == "USD"
            else (random.choice(["2023", "2024", "Q1 2024", "Q3 2023"]) if "date" in key or "year" in key
                  else round(random.uniform(0, 1), 4) if "ratio" in key or "margin" in key
                  else random.randint(50, 50000))
        )
        content_hash = sha256(f"{doc_id}:{key}:{value}")
        records.append(
            {
                "doc_id": doc_id,
                "page_ref": page,
                "key": key,
                "value": value,
                "unit": unit,
                "bbox": {
                    "x0": round(random.uniform(50, 200), 1),
                    "top": round(random.uniform(100, 700), 1),
                    "x1": round(random.uniform(300, 500), 1),
                    "bottom": round(random.uniform(720, 800), 1),
                },
                "content_hash": content_hash,
                "source_ldu_id": rng_uuid(),
            }
        )
    return records


def write_week3():
    path = OUTPUTS / "week3" / "extractions_raw.jsonl"
    all_records = []
    for doc_id in DOCUMENT_IDS:
        all_records.extend(make_week3_records(doc_id, num_facts=random.randint(5, 12)))
    with open(path, "w") as f:
        for r in all_records:
            f.write(json.dumps(r) + "\n")
    print(f"[week3] wrote {len(all_records)} fact rows → {path}")
    return all_records


# ─── WEEK 4: brownfield-cartographer actual schema ────────────────────────────
# Actual: NetworkX node_link_data format
# {directed, multigraph, graph, nodes[{id, ...attrs}], links[{source, target, ...attrs}], schema_version}
# node_type: "dataset" | "transformation" | "module"
# edge types: IMPORTS, REFERENCES_SQL, REFERENCES_CONFIG, PRODUCES, CONSUMES

MODULES = [
    ("src/week3/extractor.py", "python", "Extracts facts from financial documents using Claude"),
    ("src/week3/chunker.py", "python", "Splits documents into processable chunks"),
    ("src/week3/indexer.py", "python", "Indexes extracted facts into vector store"),
    ("src/week4/cartographer.py", "python", "Builds knowledge graph from codebase"),
    ("src/week4/graph_builder.py", "python", "Constructs NetworkX graph from module analysis"),
    ("src/week5/event_store.py", "python", "Appends and reads events from the ledger"),
    ("src/week5/stream_processor.py", "python", "Processes event streams and applies projections"),
    ("outputs/week3/extractions_raw.jsonl", "jsonl", "Week 3 fact extraction output"),
    ("outputs/week5/events_raw.jsonl", "jsonl", "Week 5 event ledger output"),
    ("configs/extraction_rules.yaml", "yaml", "Extraction configuration rules"),
]

EDGE_TYPES = ["IMPORTS", "REFERENCES_CONFIG", "PRODUCES", "CONSUMES", "IMPORTS", "IMPORTS"]


def make_week4_snapshot() -> dict:
    nodes = []
    for path, lang, purpose in MODULES:
        node_type = "dataset" if path.endswith((".jsonl", ".yaml", ".json")) else "transformation"
        nodes.append(
            {
                "id": f"file::{path}",
                "node_type": node_type,
                "path": path,
                "language": lang,
                "purpose_statement": purpose,
                "domain_cluster": "data_pipeline" if "week" in path else "config",
                "complexity_score": round(random.uniform(0.1, 0.9), 2),
                "change_velocity_30d": random.randint(0, 15),
                "is_dead_code_candidate": random.random() < 0.1,
                "last_modified": rng_dt(datetime(2025, 9, 1, tzinfo=timezone.utc), 60 * 24),
                "lines_of_code": random.randint(40, 400),
                "comment_ratio": round(random.uniform(0.05, 0.25), 2),
                "cyclomatic_complexity": random.randint(1, 12),
            }
        )

    # Build realistic edges
    edges = [
        {"source": "file::src/week3/extractor.py", "target": "file::src/week3/chunker.py", "key": 0, "edge_type": "IMPORTS", "confidence": 0.99},
        {"source": "file::src/week3/extractor.py", "target": "file::configs/extraction_rules.yaml", "key": 0, "edge_type": "REFERENCES_CONFIG", "confidence": 0.95},
        {"source": "file::src/week3/extractor.py", "target": "file::outputs/week3/extractions_raw.jsonl", "key": 0, "edge_type": "PRODUCES", "confidence": 0.97},
        {"source": "file::src/week3/indexer.py", "target": "file::outputs/week3/extractions_raw.jsonl", "key": 0, "edge_type": "CONSUMES", "confidence": 0.93},
        {"source": "file::src/week4/cartographer.py", "target": "file::src/week4/graph_builder.py", "key": 0, "edge_type": "IMPORTS", "confidence": 0.99},
        {"source": "file::src/week4/cartographer.py", "target": "file::outputs/week3/extractions_raw.jsonl", "key": 0, "edge_type": "CONSUMES", "confidence": 0.88},
        {"source": "file::src/week5/event_store.py", "target": "file::src/week5/stream_processor.py", "key": 0, "edge_type": "IMPORTS", "confidence": 0.99},
        {"source": "file::src/week5/event_store.py", "target": "file::outputs/week5/events_raw.jsonl", "key": 0, "edge_type": "PRODUCES", "confidence": 0.96},
        {"source": "file::src/week5/stream_processor.py", "target": "file::outputs/week5/events_raw.jsonl", "key": 0, "edge_type": "CONSUMES", "confidence": 0.91},
    ]

    return {
        "directed": True,
        "multigraph": True,
        "graph": {},
        "nodes": nodes,
        "links": edges,
        "schema_version": 1,
        "_captured_at": rng_dt(datetime(2025, 11, 1, tzinfo=timezone.utc), 24),
        "_git_commit": sha256("snapshot-v1")[:40],
        "_codebase_root": "/workspace/data-pipeline",
    }


def write_week4():
    path = OUTPUTS / "week4" / "lineage_graph_raw.json"
    snapshot = make_week4_snapshot()
    with open(path, "w") as f:
        json.dump(snapshot, f, indent=2)
    print(f"[week4] wrote lineage graph ({len(snapshot['nodes'])} nodes, {len(snapshot['links'])} edges) → {path}")
    return snapshot


# ─── WEEK 5: already downloaded, just report ──────────────────────────────────

def check_week5():
    path = OUTPUTS / "week5" / "events_raw.jsonl"
    if path.exists():
        count = sum(1 for _ in open(path))
        print(f"[week5] {count} real seed events already at {path}")
    else:
        print(f"[week5] WARNING: {path} not found — run download step first")


# ─── SYNTHETIC LANGSMITH TRACES ───────────────────────────────────────────────
# LangSmith project is empty; generate realistic synthetic traces

RUN_TYPES = ["llm", "chain", "tool", "retriever", "embedding"]
CHAIN_NAMES = [
    "document_extraction_chain", "fact_triage_chain", "entity_resolution_chain",
    "query_agent", "audit_chain", "chunker_chain",
]
LLM_NAMES = ["claude-3-5-sonnet-20241022", "claude-3-haiku-20240307"]


def make_trace(parent_id: str = None, session_id: str = None) -> dict:
    run_type = random.choice(RUN_TYPES)
    start = datetime(2025, 10, 1, tzinfo=timezone.utc) + timedelta(
        hours=random.randint(0, 24 * 60)
    )
    duration_ms = random.randint(200, 8000)
    end = start + timedelta(milliseconds=duration_ms)

    prompt_tokens = random.randint(800, 6000)
    completion_tokens = random.randint(100, 1200)

    return {
        "id": rng_uuid(),
        "name": random.choice(LLM_NAMES) if run_type == "llm" else random.choice(CHAIN_NAMES),
        "run_type": run_type,
        "inputs": {"prompt": "..."} if run_type == "llm" else {"input": "..."},
        "outputs": {"text": "..."} if run_type == "llm" else {"output": "..."},
        "error": None if random.random() > 0.05 else "RateLimitError: quota exceeded",
        "start_time": start.isoformat().replace("+00:00", "Z"),
        "end_time": end.isoformat().replace("+00:00", "Z"),
        "total_tokens": prompt_tokens + completion_tokens,
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "total_cost": round((prompt_tokens * 3e-6 + completion_tokens * 15e-6), 6),
        "tags": random.sample(["week3", "week5", "extraction", "audit", "query"], 2),
        "parent_run_id": parent_id,
        "session_id": session_id or rng_uuid(),
    }


def write_traces():
    path = OUTPUTS / "traces" / "runs.jsonl"
    session_id = rng_uuid()
    traces = [make_trace(session_id=session_id) for _ in range(80)]
    # Add some child traces with parent_run_id
    parents = random.sample(traces[:20], 5)
    for p in parents:
        for _ in range(random.randint(2, 4)):
            traces.append(make_trace(parent_id=p["id"], session_id=session_id))
    with open(path, "w") as f:
        for t in traces:
            f.write(json.dumps(t) + "\n")
    print(f"[traces] wrote {len(traces)} synthetic LangSmith traces → {path}")


# ─── MAIN ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("Generating sample outputs...\n")
    write_week2()
    write_week3()
    write_week4()
    check_week5()
    write_traces()
    print("\nDone. Run outputs/migrate/migrate_to_canonical.py next to produce canonical JSONL.")
