"""
contracts/ai_extensions.py — AI Contract Extensions (Phase 4A)
===============================================================
Three AI-specific data contract checks beyond standard structural/statistical:

1. Embedding drift — cosine distance of text centroids from baseline
2. Prompt input schema validation — JSON Schema for extraction prompt inputs
3. LLM output schema violation rate — track verdict enum conformance over time

Usage:
    python contracts/ai_extensions.py \
        --extractions outputs/week3/extractions.jsonl \
        --verdicts outputs/week2/verdicts_canonical.jsonl \
        --output validation_reports/ai_extensions.json
"""

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import yaml

try:
    from jsonschema import ValidationError, validate
except ImportError:
    validate = None
    ValidationError = Exception

from dotenv import load_dotenv

load_dotenv()

ROOT = Path(__file__).parent.parent
BASELINE_PATH = ROOT / "schema_snapshots" / "embedding_baselines.npz"
QUARANTINE_PATH = ROOT / "outputs" / "quarantine"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def load_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path) as f:
        return [json.loads(l) for l in f if l.strip()]


# ── Extension 1: Embedding Drift ──────��──────────────────────────────────────

def embed_sample(texts: list[str], n: int = 200, model: str = "text-embedding-3-small") -> np.ndarray:
    """Embed text samples via OpenRouter or OpenAI-compatible API."""
    import httpx

    sample = texts[:n]
    api_key = os.getenv("OPEN_ROUTER_KEY") or os.getenv("OPENAI_API_KEY", "")
    base_url = "https://openrouter.ai/api/v1"

    if not api_key:
        raise ValueError("No OPEN_ROUTER_KEY or OPENAI_API_KEY found in environment")

    resp = httpx.post(
        f"{base_url}/embeddings",
        headers={"Authorization": f"Bearer {api_key}"},
        json={"input": sample, "model": model},
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()["data"]
    return np.array([d["embedding"] for d in data])


def check_embedding_drift(
    texts: list[str],
    baseline_path: Path = BASELINE_PATH,
    threshold: float = 0.15,
) -> dict:
    """Check semantic drift of text embeddings from stored baseline centroid."""
    if not texts:
        return {
            "status": "ERROR",
            "drift_score": None,
            "threshold": threshold,
            "message": "No text samples available for embedding drift check",
        }

    try:
        vecs = embed_sample(texts, n=200)
    except Exception as e:
        return {
            "status": "ERROR",
            "drift_score": None,
            "threshold": threshold,
            "message": f"Embedding API call failed: {e}",
        }

    centroid = vecs.mean(axis=0)

    if not baseline_path.exists():
        baseline_path.parent.mkdir(parents=True, exist_ok=True)
        np.savez(baseline_path, centroid=centroid)
        return {
            "status": "BASELINE_SET",
            "drift_score": 0.0,
            "threshold": threshold,
            "samples_embedded": len(texts[:200]),
            "message": "Baseline established. Run again to detect drift.",
        }

    baseline = np.load(baseline_path)["centroid"]
    sim = np.dot(centroid, baseline) / (np.linalg.norm(centroid) * np.linalg.norm(baseline) + 1e-9)
    drift = float(1 - sim)

    return {
        "status": "FAIL" if drift > threshold else "PASS",
        "drift_score": round(drift, 4),
        "threshold": threshold,
        "samples_embedded": len(texts[:200]),
        "interpretation": "semantic content shifted" if drift > threshold else "stable",
    }


# ── Extension 2: Prompt Input Schema Validation ──────���───────────────────────

WEEK3_PROMPT_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "required": ["doc_id", "source_path"],
    "properties": {
        "doc_id":          {"type": "string", "minLength": 1},
        "source_path":     {"type": "string", "minLength": 1},
        "source_hash":     {"type": "string"},
    },
    "additionalProperties": True,
}


def validate_prompt_inputs(
    records: list[dict],
    schema: dict = WEEK3_PROMPT_SCHEMA,
    quarantine_path: Path = QUARANTINE_PATH,
) -> dict:
    """Validate records against prompt input schema. Quarantine non-conforming."""
    if validate is None:
        return {
            "valid": len(records),
            "quarantined": 0,
            "status": "ERROR",
            "message": "jsonschema not installed",
        }

    valid_count = 0
    quarantined = []

    for r in records:
        try:
            validate(instance=r, schema=schema)
            valid_count += 1
        except ValidationError as e:
            quarantined.append({
                "record_id": r.get("doc_id", "unknown"),
                "error": str(e.message) if hasattr(e, "message") else str(e),
                "path": list(e.path) if hasattr(e, "path") else [],
            })

    if quarantined:
        quarantine_path.mkdir(parents=True, exist_ok=True)
        qfile = quarantine_path / "quarantine.jsonl"
        with open(qfile, "a") as f:
            for q in quarantined:
                f.write(json.dumps(q) + "\n")

    return {
        "valid": valid_count,
        "quarantined": len(quarantined),
        "total": len(records),
        "violation_rate": round(len(quarantined) / max(len(records), 1), 4),
        "status": "PASS" if len(quarantined) == 0 else "WARN",
    }


# ── Extension 3: LLM Output Schema Violation Rate ────────────────────────────

def check_output_violation_rate(
    outputs: list[dict],
    expected_enum_field: str = "overall_verdict",
    expected_values: set = {"PASS", "FAIL", "WARN"},
    baseline_rate: float | None = None,
    warn_threshold: float = 0.02,
) -> dict:
    """Track the rate of LLM output schema violations over time."""
    total = len(outputs)
    violations = sum(1 for o in outputs if o.get(expected_enum_field) not in expected_values)
    rate = violations / max(total, 1)

    trend = "unknown"
    if baseline_rate is not None:
        if rate > baseline_rate * 1.5:
            trend = "rising"
        elif rate < baseline_rate * 0.5:
            trend = "falling"
        else:
            trend = "stable"

    return {
        "total_outputs": total,
        "schema_violations": violations,
        "violation_rate": round(rate, 4),
        "trend": trend,
        "baseline_rate": baseline_rate,
        "status": "WARN" if (trend == "rising" or rate > warn_threshold) else "PASS",
    }


# ── Main pipeline ���────────────────────────────────────────────────────────────

def run_ai_extensions(
    extractions_path: Path,
    verdicts_path: Path,
    output_path: Path,
) -> dict:
    print("\n[ai_extensions] Running AI Contract Extensions...")

    results = {
        "generated_at": now_iso(),
        "embedding_drift": {},
        "prompt_input_validation": {},
        "output_violation_rate": {},
    }

    # Load data
    extractions = load_jsonl(extractions_path)
    verdicts = load_jsonl(verdicts_path)
    print(f"  loaded {len(extractions)} extraction records, {len(verdicts)} verdict records")

    # Extension 1: Embedding drift on extracted_facts[*].text
    texts = []
    for r in extractions:
        for fact in r.get("extracted_facts", []):
            text = fact.get("text", "")
            if text:
                texts.append(text)
    print(f"  [1] embedding drift: {len(texts)} text samples available")

    drift_result = check_embedding_drift(texts)
    results["embedding_drift"] = drift_result
    print(f"  [1] embedding drift: {drift_result['status']} "
          f"(score={drift_result.get('drift_score', 'N/A')})")

    # Extension 2: Prompt input validation
    prompt_result = validate_prompt_inputs(extractions)
    results["prompt_input_validation"] = prompt_result
    print(f"  [2] prompt validation: {prompt_result['valid']}/{prompt_result['total']} valid, "
          f"{prompt_result['quarantined']} quarantined")

    # Extension 3: LLM output schema violation rate
    violation_result = check_output_violation_rate(verdicts)
    results["output_violation_rate"] = violation_result
    print(f"  [3] output violation rate: {violation_result['violation_rate']:.4f} "
          f"({violation_result['schema_violations']}/{violation_result['total_outputs']})")

    # Write output
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\n[ai_extensions] report -> {output_path}")

    return results


# ── CLI ─────────���───────────────────────────────��─────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AI Contract Extensions — embedding drift, prompt validation, output schema")
    parser.add_argument("--extractions", required=True, help="Path to week3 extractions.jsonl")
    parser.add_argument("--verdicts",    required=True, help="Path to week2 verdicts JSONL")
    parser.add_argument("--output",      default="validation_reports/ai_extensions.json",
                        help="Output path for AI extensions report")
    args = parser.parse_args()

    run_ai_extensions(
        extractions_path=Path(args.extractions),
        verdicts_path=Path(args.verdicts),
        output_path=Path(args.output),
    )
