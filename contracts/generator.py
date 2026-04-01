"""
contracts/generator.py — ContractGenerator (Phase 1)
=====================================================
Auto-generates Bitol v3.0.0 YAML contracts from JSONL outputs + Week 4 lineage graph.
Also emits dbt schema.yml and writes a timestamped schema snapshot.

Usage:
    python contracts/generator.py \
        --source outputs/week3/extractions.jsonl \
        --contract-id week3-document-refinery-extractions \
        --lineage outputs/week4/lineage_snapshots.jsonl \
        --output generated_contracts/
"""

import argparse
import hashlib
import json
import os
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import yaml
from dotenv import load_dotenv

load_dotenv()

try:
    from langsmith import traceable as _traceable
    _LANGSMITH_AVAILABLE = True
except ImportError:
    _LANGSMITH_AVAILABLE = False
    def _traceable(**_kw):
        def _wrap(fn):
            return fn
        return _wrap

# ── helpers ──────────────────────────────────────────────────────────────────

ROOT = Path(__file__).parent.parent


def rng_uuid() -> str:
    return str(uuid.uuid4())


def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


# ── Stage 1: load + flatten ───────────────────────────────────────────────────

def load_jsonl(path: Path) -> list[dict]:
    with open(path) as f:
        return [json.loads(l) for l in f if l.strip()]


def flatten_for_profile(records: list[dict]) -> pd.DataFrame:
    """
    Flatten nested JSONL to a flat DataFrame for profiling.
    Arrays like extracted_facts[] are exploded to one row per item;
    nested dicts are dot-expanded one level.
    """
    rows = []
    for r in records:
        # top-level scalar fields
        base = {k: v for k, v in r.items() if not isinstance(v, (list, dict))}

        # find the first list field and explode it (covers extracted_facts, code_refs, etc.)
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

        # nested dicts at top level
        for k, v in r.items():
            if isinstance(v, dict):
                for dk, dv in v.items():
                    if not isinstance(dv, (list, dict)):
                        base[f"{k}.{dk}"] = dv

        if not exploded:
            rows.append(base)

    return pd.DataFrame(rows)


# ── Stage 2: structural + statistical profiling ───────────────────────────────

def profile_column(series: pd.Series, col_name: str) -> dict:
    result = {
        "name": col_name,
        "dtype": str(series.dtype),
        "null_fraction": float(series.isna().mean()),
        "cardinality_estimate": int(series.nunique()),
        "sample_values": [str(v) for v in series.dropna().unique()[:5]],
    }
    if pd.api.types.is_numeric_dtype(series):
        clean = series.dropna()
        if len(clean) > 0:
            result["stats"] = {
                "min":    float(clean.min()),
                "max":    float(clean.max()),
                "mean":   float(clean.mean()),
                "p25":    float(clean.quantile(0.25)),
                "p50":    float(clean.quantile(0.50)),
                "p75":    float(clean.quantile(0.75)),
                "p95":    float(clean.quantile(0.95)),
                "p99":    float(clean.quantile(0.99)),
                "stddev": float(clean.std()) if len(clean) > 1 else 0.0,
            }
            # flag suspicious confidence distributions
            if "confidence" in col_name:
                mean = result["stats"]["mean"]
                if mean > 0.99:
                    result["warning"] = "mean > 0.99 — distribution may be clamped"
                elif mean < 0.01:
                    result["warning"] = "mean < 0.01 — distribution likely broken"
                elif result["stats"]["max"] > 1.0:
                    result["warning"] = f"max={result['stats']['max']:.1f} — confidence appears to be on 0-100 scale, not 0.0-1.0"
    return result


def profile_all(df: pd.DataFrame) -> dict[str, dict]:
    return {col: profile_column(df[col], col) for col in df.columns}


# ── Stage 3: profiles → Bitol YAML clauses ───────────────────────────────────

UUID_RE = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
ISO_RE  = r"^\d{4}-\d{2}-\d{2}T"


def infer_type(dtype_str: str) -> str:
    mapping = {"float64": "number", "float32": "number",
               "int64": "integer", "int32": "integer",
               "bool": "boolean", "object": "string"}
    return mapping.get(dtype_str, "string")


def column_to_clause(profile: dict) -> dict:
    clause: dict = {
        "type": infer_type(profile["dtype"]),
        "required": profile["null_fraction"] == 0.0,
    }

    name = profile["name"]

    # confidence fields — always 0.0–1.0
    if "confidence" in name and clause["type"] == "number":
        clause["minimum"] = 0.0
        clause["maximum"] = 1.0
        clause["description"] = (
            "Confidence score. MUST remain float 0.0–1.0. "
            "BREAKING CHANGE if converted to integer 0–100."
        )
        if profile.get("warning"):
            clause["x-warning"] = profile["warning"]

    # UUID fields
    is_id = name.endswith("_id") or name.endswith(".id")
    if is_id:
        clause["format"] = "uuid"
        clause["pattern"] = UUID_RE

    # Uniqueness for identifier fields
    if is_id and profile["cardinality_estimate"] == len(profile["sample_values"]):
        clause["x-unique"] = True

    # timestamp fields
    if name.endswith("_at") or name.endswith("_time"):
        clause["format"] = "date-time"

    # enum detection: low cardinality string columns where sample covers all values
    # Skip if field looks like a UUID/ID or timestamp — those aren't enums
    is_id_or_ts = (is_id or name.endswith("_at") or name.endswith("_time"))
    if (clause["type"] == "string"
            and not is_id_or_ts
            and profile["cardinality_estimate"] <= 10
            and profile["cardinality_estimate"] > 0
            and profile["cardinality_estimate"] == len(profile["sample_values"])):
        clause["enum"] = sorted(profile["sample_values"])

    # numeric range from observed data (non-confidence)
    if "stats" in profile and "confidence" not in name:
        s = profile["stats"]
        clause["x-observed"] = {
            "min": s["min"], "max": s["max"],
            "mean": round(s["mean"], 4), "stddev": round(s["stddev"], 4),
        }
        # Promote non-negative floor to hard constraint for clearly non-negative fields
        if s["min"] >= 0 and clause["type"] in ("number", "integer"):
            clause["minimum"] = 0
        # Promote positive-only constraint for duration/count/size fields
        if any(kw in name for kw in ("_ms", "_count", "_size", "_bytes", "_length")):
            clause["minimum"] = 0

    # Sequence / monotonic fields (event sourcing: position, sequence, version)
    if clause["type"] == "integer" and any(kw in name for kw in ("position", "sequence")):
        clause["minimum"] = 0
        clause["x-monotonic"] = "increasing"
        clause["description"] = (
            f"Monotonically increasing per stream. "
            f"Observed range: [{profile.get('stats', {}).get('min', '?')}, "
            f"{profile.get('stats', {}).get('max', '?')}]."
        )

    # null_fraction stat
    if profile["null_fraction"] > 0:
        clause["x-null-fraction"] = round(profile["null_fraction"], 4)

    return clause


# ── Stage 4: lineage context injection ───────────────────────────────────────

def load_lineage(lineage_path: Path) -> dict | None:
    """Load the latest snapshot from lineage_snapshots.jsonl."""
    if not lineage_path.exists():
        return None
    lines = lineage_path.read_text().strip().splitlines()
    if not lines:
        return None
    return json.loads(lines[-1])


def find_downstream_consumers(snapshot: dict, contract_id: str) -> list[dict]:
    """
    Find nodes in the lineage graph that consume data from the contract's source.
    Matches on contract_id keywords against edge source node IDs.
    """
    if not snapshot:
        return []

    keywords = contract_id.lower().replace("-", "_").split("_")
    # e.g. "week3_document_refinery_extractions" → look for edges whose source contains "week3" or "extraction"
    search_terms = [k for k in keywords if len(k) > 3]

    consumers = []
    seen = set()
    for edge in snapshot.get("edges", []):
        src = str(edge.get("source", "")).lower()
        if any(term in src for term in search_terms):
            target = edge.get("target", "")
            if target not in seen:
                seen.add(target)
                consumers.append({
                    "id": target,
                    "description": f"Downstream node consuming from {edge['source']}",
                    "fields_consumed": [],  # populated by LLM annotation if available
                    "breaking_if_changed": [],
                })
    return consumers


# ── Stage 5: LLM annotation via OpenRouter ────────────────────────────────────

def is_ambiguous(profile: dict) -> bool:
    """Columns needing LLM annotation: not obviously typed by name/values."""
    name = profile["name"]
    obvious = (
        name.endswith("_id") or name.endswith("_at") or name.endswith("_time")
        or "confidence" in name
        or profile["cardinality_estimate"] <= 10
        or profile["dtype"] in ("float64", "float32", "int64", "int32", "bool")
    )
    return not obvious


@_traceable(name="contract-llm-annotation", run_type="llm")
def annotate_with_llm(profiles: dict[str, dict], table_name: str) -> dict[str, dict]:
    """
    For ambiguous columns, call Claude via OpenRouter to get:
    - plain-English description
    - business rule as a validation expression
    - cross-column relationships
    Returns updated profiles with 'llm_annotation' key added.
    """
    api_key = os.getenv("OPEN_ROUTER_KEY", "").strip()
    if not api_key:
        print("  [generator] OPEN_ROUTER_KEY not set — skipping LLM annotation")
        return profiles

    try:
        import httpx
    except ImportError:
        print("  [generator] httpx not installed — skipping LLM annotation")
        return profiles

    ambiguous = {k: v for k, v in profiles.items() if is_ambiguous(v)}
    if not ambiguous:
        return profiles

    # Build one batch prompt
    col_descriptions = []
    for col, p in ambiguous.items():
        col_descriptions.append(
            f"- column: {col}\n"
            f"  dtype: {p['dtype']}\n"
            f"  sample_values: {p['sample_values'][:3]}\n"
            f"  null_fraction: {p['null_fraction']}"
        )

    prompt = (
        f"You are a data contract expert. For the table '{table_name}', "
        f"provide annotations for these columns. "
        f"For each, give: (a) a plain-English description in one sentence, "
        f"(b) a validation rule (e.g. 'must be positive integer', 'must match regex X'), "
        f"(c) any cross-column relationship (e.g. 'must be <= end_time'). "
        f"Respond as JSON: {{\"column_name\": {{\"description\": \"...\", \"rule\": \"...\", \"cross_column\": \"...\"}}, ...}}\n\n"
        f"Columns:\n" + "\n".join(col_descriptions)
    )

    try:
        resp = httpx.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={
                "model": "anthropic/claude-3-haiku",
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 800,
            },
            timeout=30,
        )
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"]
        # Extract JSON from response
        match = re.search(r"\{.*\}", content, re.DOTALL)
        if match:
            annotations = json.loads(match.group())
            for col, ann in annotations.items():
                if col in profiles:
                    profiles[col]["llm_annotation"] = ann
            print(f"  [generator] LLM annotated {len(annotations)} ambiguous columns")
    except Exception as e:
        print(f"  [generator] LLM annotation failed: {e}")

    return profiles


# ── Stage 6: build full Bitol contract ───────────────────────────────────────

def build_contract(
    contract_id: str,
    source_path: Path,
    profiles: dict[str, dict],
    downstream_consumers: list[dict],
    snapshot_id: str | None,
) -> dict:
    schema_clauses = {col: column_to_clause(p) for col, p in profiles.items()}

    # Soda quality checks for key fields
    soda_checks = ["- row_count >= 1"]
    for col, p in profiles.items():
        flat = col.replace(".", "_")
        if p["null_fraction"] == 0.0:
            soda_checks.append(f"- missing_count({flat}) = 0")
        if "confidence" in col and p.get("stats"):
            soda_checks.append(f"- min({flat}) >= 0.0")
            soda_checks.append(f"- max({flat}) <= 1.0")
        # Uniqueness check for ID fields
        if (col.endswith("_id") or col.endswith(".id")) and p["null_fraction"] == 0.0:
            soda_checks.append(f"- duplicate_count({flat}) = 0")
        # Non-negative check for duration/count/position fields
        if (p["dtype"] in ("int64", "float64") and p.get("stats", {}).get("min", -1) >= 0
                and any(kw in col for kw in ("_ms", "_count", "position", "page_ref"))):
            soda_checks.append(f"- min({flat}) >= 0")

    source_hash = sha256_bytes(source_path.read_bytes())
    table_name = contract_id.replace("-", "_")

    contract = {
        "kind": "DataContract",
        "apiVersion": "v3.0.0",
        "id": contract_id,
        "info": {
            "title": contract_id.replace("-", " ").title(),
            "version": "1.0.0",
            "owner": "week7-data-contract-enforcer",
            "description": (
                f"Auto-generated contract for {source_path.name}. "
                f"Source hash: {source_hash[:16]}..."
            ),
        },
        "servers": {
            "local": {
                "type": "local",
                "path": str(source_path.resolve().relative_to(ROOT.resolve())),
                "format": "jsonl",
            }
        },
        "terms": {
            "usage": "Internal inter-system data contract. Do not publish.",
        },
        "schema": schema_clauses,
        "quality": {
            "type": "SodaChecks",
            "specification": {
                f"checks for {table_name}": soda_checks,
            },
        },
        "lineage": {
            "upstream": [],
            "downstream": downstream_consumers,
        },
        "x-generated-at": now_iso(),
        "x-source-hash": source_hash,
        "x-snapshot-id": snapshot_id,
    }
    return contract


# ── Stage 6b: dbt schema.yml output ──────────────────────────────────────────

def build_dbt_schema(contract_id: str, profiles: dict[str, dict]) -> dict:
    columns = []
    for col, p in profiles.items():
        col_def: dict = {"name": col.replace(".", "_")}
        tests = []
        is_numeric = p["dtype"] in ("float64", "float32", "int64", "int32")
        has_range = False

        if p["null_fraction"] == 0.0:
            tests.append("not_null")

        # Confidence fields — range test instead of accepted_values
        if "confidence" in col and is_numeric:
            tests.append({
                "dbt_expectations.expect_column_values_to_be_between": {
                    "min_value": 0.0,
                    "max_value": 1.0,
                }
            })
            has_range = True

        # Non-negative numeric fields — range test
        elif is_numeric and p.get("stats", {}).get("min", -1) >= 0:
            stats = p["stats"]
            range_test = {"min_value": 0}
            # Add observed max as soft upper bound for bounded fields
            if any(kw in col for kw in ("_version", "_count", "page_ref", "_sequence")):
                range_test["max_value"] = int(stats["max"] * 2) or 100
            tests.append({
                "dbt_expectations.expect_column_values_to_be_between": range_test,
            })
            has_range = True

        # Enum: only for string fields (not numeric — avoid overfitting)
        if (not is_numeric
                and p.get("cardinality_estimate", 999) <= 10
                and p["sample_values"]):
            tests.append({
                "accepted_values": {
                    "values": sorted(p["sample_values"])
                }
            })

        # Uniqueness for ID fields
        if col.endswith("_id") or col.endswith(".id"):
            tests.append("unique")

        if tests:
            col_def["tests"] = tests
        if p.get("llm_annotation", {}).get("description"):
            col_def["description"] = p["llm_annotation"]["description"]
        columns.append(col_def)

    return {
        "version": 2,
        "models": [
            {
                "name": contract_id.replace("-", "_"),
                "description": f"dbt model for {contract_id}",
                "columns": columns,
            }
        ],
    }


# ── Stage 7: schema snapshot ──────────────────────────────────────────────────

def write_snapshot(contract_id: str, profiles: dict[str, dict], contract: dict) -> str:
    """Write timestamped schema snapshot. Returns snapshot_id."""
    snap_dir = ROOT / "schema_snapshots" / contract_id
    snap_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    snap_path = snap_dir / f"{ts}.yaml"

    snapshot = {
        "snapshot_id": rng_uuid(),
        "contract_id": contract_id,
        "captured_at": now_iso(),
        "columns": {
            col: {
                "dtype": p["dtype"],
                "null_fraction": p["null_fraction"],
                "cardinality": p["cardinality_estimate"],
                "stats": p.get("stats"),
            }
            for col, p in profiles.items()
        },
    }
    with open(snap_path, "w") as f:
        yaml.dump(snapshot, f, default_flow_style=False, sort_keys=False)
    print(f"  [generator] snapshot → {snap_path}")
    return snapshot["snapshot_id"]


# ── Main pipeline ─────────────────────────────────────────────────────────────

@_traceable(name="contract-generator", run_type="chain")
def generate(
    source: Path,
    contract_id: str,
    lineage: Path,
    output_dir: Path,
    skip_llm: bool = False,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n[generator] source      : {source}")
    print(f"[generator] contract-id : {contract_id}")
    print(f"[generator] lineage     : {lineage}")

    # Stage 1 — load + flatten
    records = load_jsonl(source)
    print(f"  [1] loaded {len(records)} records")
    df = flatten_for_profile(records)
    print(f"  [1] flattened → {len(df)} rows × {len(df.columns)} columns")
    if df.empty:
        raise ValueError("Flattened DataFrame is empty — check source file")

    # Stage 2 — profile
    profiles = profile_all(df)
    print(f"  [2] profiled {len(profiles)} columns")
    for col, p in profiles.items():
        if p.get("warning"):
            print(f"  [2] ⚠  {col}: {p['warning']}")

    # Stage 3 (done inside build_contract via column_to_clause)

    # Stage 4 — lineage injection
    lineage_snapshot = load_lineage(lineage)
    consumers = find_downstream_consumers(lineage_snapshot, contract_id) if lineage_snapshot else []
    print(f"  [4] found {len(consumers)} downstream consumers in lineage graph")

    # Stage 5 — LLM annotation
    if not skip_llm:
        table_name = contract_id.split("-")[0] if "-" in contract_id else contract_id
        profiles = annotate_with_llm(profiles, table_name)

    # Stage 7 — snapshot (before contract so we can embed snapshot_id)
    snapshot_id = write_snapshot(contract_id, profiles, {})

    # Stage 6 — build contract
    contract = build_contract(contract_id, source, profiles, consumers, snapshot_id)

    # Write Bitol YAML
    out_path = output_dir / f"{contract_id}.yaml"
    with open(out_path, "w") as f:
        yaml.dump(contract, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
    print(f"  [6] contract → {out_path}")

    # Write dbt schema.yml
    dbt = build_dbt_schema(contract_id, profiles)
    dbt_path = output_dir / f"{contract_id}_dbt.yml"
    with open(dbt_path, "w") as f:
        yaml.dump(dbt, f, default_flow_style=False, sort_keys=False)
    print(f"  [6] dbt schema → {dbt_path}")

    return out_path


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ContractGenerator — auto-generate Bitol YAML contracts")
    parser.add_argument("--source",      required=True, help="Path to input JSONL file")
    parser.add_argument("--contract-id", required=True, help="Contract ID (used as output filename)")
    parser.add_argument("--lineage",     required=True, help="Path to lineage_snapshots.jsonl")
    parser.add_argument("--output",      required=True, help="Output directory for generated contracts")
    parser.add_argument("--skip-llm",    action="store_true", help="Skip LLM annotation step")
    args = parser.parse_args()

    generate(
        source=Path(args.source),
        contract_id=args.contract_id,
        lineage=Path(args.lineage),
        output_dir=Path(args.output),
        skip_llm=args.skip_llm,
    )
