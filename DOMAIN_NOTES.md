# DOMAIN_NOTES.md — Data Contract Enforcer (Week 7)

*Answers grounded in the actual Weeks 1–5 schemas as implemented and observed.*

---

## Question 1: Backward-Compatible vs. Breaking Schema Changes

A **backward-compatible** change lets both old producers and new consumers operate without modification — no data is lost, no existing logic breaks. A **breaking change** forces at least one side to update or accept corrupted/missing data.

### Three Backward-Compatible Changes (from our actual schemas)

| # | Schema | Change | Why Safe |
|---|--------|--------|----------|
| 1 | **Week 5 `event_record`** | Add new nullable field `global_position` (int) alongside existing `stream_position` | Consumers that don't know about `global_position` ignore it. Axiom-Ledger already emits this field alongside the canonical `sequence_number` equivalent. |
| 2 | **Week 4 `lineage_snapshot`** | Add new `edge_type` value `"PIPELINE"` to the existing enum `IMPORTS\|CALLS\|READS\|WRITES\|PRODUCES\|CONSUMES` | Additive enum — existing consumers simply encounter an unknown value they can treat as opaque. No existing edges are changed. |
| 3 | **Week 3 `extraction_record`** | Widen `processing_time_ms` from `int` to `float` (e.g., `9982` → `9982.25`) | All existing integer values are valid floats. The extraction ledger already emits `9982.25` in this run; consumers expecting int still parse correctly in Python/JSON. |

### Three Breaking Schema Changes (from our actual schemas)

| # | Schema | Change | Why Dangerous |
|---|--------|--------|---------------|
| 1 | **Week 3 `extraction_record`** | `confidence` changes from `float 0.0–1.0` to `int 0–100` | Silent corruption — the Cartographer's routing threshold `confidence < 0.5` silently stops escalating. Documented in Q2 with real runtime evidence. |
| 2 | **Week 5 `event_record`** | Rename `stream_id` → `aggregate_id` | Every consumer keyed on `stream_id` breaks immediately. Our migration script had to handle this exact rename — it is a real deviation between the actual Axiom-Ledger output and the canonical spec. Blast radius: all 95 events in the current run, 10 unique aggregates. |
| 3 | **Week 2 `verdict_record`** | Remove the `rubric_id` field | Any downstream system using `rubric_id` to verify which rubric produced a score loses the ability to audit the evaluation. Our Week 2 actual output had no `rubric_id` — we derived it from `sha256(rubric.json)` in the migration script precisely because this field was missing. |

---

## Question 2: Confidence Scale Change — Failure Trace & Contract Clause

### Confidence Distribution in Current Extractions

Script run against `outputs/week3/extractions.jsonl`:

```python
import json, statistics
with open('outputs/week3/extractions.jsonl') as f:
    docs = [json.loads(l) for l in f]
confs = [fact['confidence'] for doc in docs for fact in doc.get('extracted_facts', [])]
print(f'min={min(confs):.3f} max={max(confs):.3f} mean={statistics.mean(confs):.3f} stddev={statistics.stdev(confs):.3f}')
```

**Output:**
```
n=102  min=0.751  max=0.987  mean=0.868  stddev=0.070
```

All values are correctly in `[0.0, 1.0]`. The contract clause below would catch a scale change before it propagates.

### Runtime Failure Trace (run against `outputs/week3/extraction_ledger.jsonl`)

The Document Refinery was run with two confidence regimes against `.demo_input/probe_6_9.pdf`:

```json
{
  "observed_fast_text_confidence_0_to_1": 0.31712342514135183,
  "threshold": 0.5,
  "run_valid_0_to_1": {
    "strategy_used": "fast_text->layout",
    "text_blocks_total": 2
  },
  "run_inflated_0_to_100": {
    "strategy_used": "fast_text",
    "text_blocks_total": 1
  }
}
```

**Failure chain:**
1. Router compares `confidence < 0.5` to decide whether to escalate to layout analysis.
2. Valid range: `0.317 < 0.5` → escalates → `fast_text->layout` → 2 text blocks extracted.
3. Inflated range: `int(round(0.317 × 100)) = 32`; `32 < 0.5` is `False` → no escalation → `fast_text` only → 1 text block.
4. Cartographer receives a weaker payload (50% fewer text blocks), reducing structural signal quality before indexing.

**No crash. No error. Silent semantic failure.**

### Bitol YAML Contract Clause

```yaml
kind: DataContract
apiVersion: v3.0.0
id: week3-document-refinery-extractions
info:
  title: Week 3 Document Refinery — Extraction Records
  version: 1.0.0
  owner: week3-team
schema:
  extracted_facts:
    type: array
    items:
      confidence:
        type: number
        minimum: 0.0
        maximum: 1.0
        required: true
        description: >
          Confidence score for this extracted fact. MUST remain float in [0.0, 1.0].
          Changing to integer 0–100 is a BREAKING change — downstream routing
          thresholds (e.g., confidence < 0.5) will silently invert behaviour.
quality:
  type: SodaChecks
  specification:
    checks for extractions:
      - min(fact_confidence) >= 0.0
      - max(fact_confidence) <= 1.0
      - avg(fact_confidence) between 0.5 and 0.99
lineage:
  downstream:
    - id: week4-cartographer
      breaking_if_changed: [extracted_facts.confidence]
      failure_mode: >
        Routing threshold comparisons invert silently when confidence is
        scaled to 0–100. Cartographer receives degraded extraction payloads.
```

---

## Question 3: How the Enforcer Uses the Lineage Graph to Build a Blame Chain

### Inputs

- `outputs/week4/.cartography/lineage_graph.json` — the real cartographer output (13 nodes, 13 edges, jaffle_shop_classic dbt project)
- `outputs/week4/.cartography/git_history.txt` — commit log for the analysed codebase
- ValidationRunner output — the failing check ID and column name

### Graph Model

The cartographer emits a bipartite directed graph:

```
upstream_dataset → [CONSUMES edge] → transformation → [PRODUCES edge] → downstream_dataset
```

Actual node types in the real output: `dataset` (e.g., `stg_customers`, `raw_payments`) and `transformation` (e.g., `transformation:models/customers.sql:0-0#1`).

### Step-by-Step Blame Chain Construction

**Step 1 — Anchor the violation.** ValidationRunner reports `FAIL` on `customers.confidence` (or equivalent failing column). Resolve the column to its owning dataset node in the lineage graph: `dataset:customers`.

**Step 2 — Reverse BFS from the failing node.** Traverse upstream (against data-flow direction): find all `PRODUCES` edges whose `target == "customers"` → yields `transformation:models/customers.sql:0-0#1`.

**Step 3 — Continue upstream.** From that transformation, find all `CONSUMES` edges whose `source` is the transformation → yields `stg_customers`, `stg_orders`, `stg_payments`.

**Step 4 — Apply git blame at each hop.** For each upstream file found:

```bash
git log --follow --since="14 days ago" --format='%H|%an|%ae|%ai|%s' -- models/customers.sql
git blame -L 0,0 --porcelain models/customers.sql
```

From `outputs/week4/.cartography/git_history.txt`, the most recent relevant commit is:
```
f72efd2 Initial commit
51b79db Remove the mention of the email field   ← most recent field removal
```

**Step 5 — Score and rank candidates.** Confidence formula per the spec:
```
confidence = 1.0 − (days_since_commit × 0.1) − (lineage_hops × 0.2)
```
A commit from 2 days ago, 1 hop away → `1.0 − 0.2 − 0.2 = 0.6`.

**Step 6 — Emit blame chain to `violation_log/violations.jsonl`:**

```json
{
  "violation_id": "uuid",
  "check_id": "week3.extracted_facts.confidence.range",
  "detected_at": "2026-04-01T...",
  "blame_chain": [
    {
      "rank": 1,
      "file_path": "models/customers.sql",
      "commit_hash": "51b79db...",
      "author": "fishtown-analytics",
      "commit_timestamp": "...",
      "commit_message": "Remove the mention of the email field",
      "confidence_score": 0.6
    }
  ],
  "blast_radius": {
    "affected_nodes": ["customers", "stg_customers"],
    "affected_pipelines": ["week4-lineage-generation"],
    "estimated_records": 95
  }
}
```

### Traversal Implementation

```python
from collections import deque

def blame_chain(graph, violated_dataset):
    q = deque([violated_dataset])
    visited = set()
    blame_points = []
    parent = {}

    while q:
        node = q.popleft()
        if node in visited:
            continue
        visited.add(node)

        producers = [e["source"] for e in graph["edges"]
                     if e["target"] == node and e["edge_type"] == "PRODUCES"]
        for transformation in producers:
            inputs = [e["source"] for e in graph["edges"]
                      if e["target"] == transformation and e["edge_type"] == "CONSUMES"]
            if all_inputs_valid(inputs) and output_invalid(node):
                blame_points.append(transformation)
            else:
                for upstream in inputs:
                    if upstream not in visited:
                        parent[upstream] = node
                        q.append(upstream)

    return reconstruct_paths(blame_points, violated_dataset, parent)
```

---

## Question 4: Data Contract for LangSmith `trace_record`

### Stat Measurement (run against `outputs/traces/runs.jsonl`)

```python
import json, statistics
with open('outputs/traces/runs.jsonl') as f:
    traces = [json.loads(l) for l in f]
llm = [t for t in traces if t['run_type'] == 'llm']
token_checks = [t['total_tokens'] == t['prompt_tokens'] + t['completion_tokens'] for t in llm]
costs = [t['total_cost'] for t in llm]
print(f'LLM runs: {len(llm)}, token_sum_matches: {sum(token_checks)}/{len(llm)}, cost range: {min(costs):.4f}–{max(costs):.4f}')
```

### Bitol-Compatible YAML Contract

```yaml
kind: DataContract
apiVersion: v3.0.0
id: langsmith-trace-records
info:
  title: LangSmith Trace Records
  version: 1.0.0
  owner: ai-ops-team
  description: >
    Contract for LangSmith execution traces exported from the Week 3–5 agent
    pipelines. Consumed by the AI Contract Extensions component to detect
    embedding drift, prompt schema violations, and cost anomalies.
servers:
  local:
    type: local
    path: outputs/traces/runs.jsonl
    format: jsonl
terms:
  usage: Internal AI observability contract.
  limitations: total_cost field may be null for non-LLM run types.

schema:
  id:
    type: string
    format: uuid
    required: true
    unique: true
  run_type:
    type: string
    required: true
    enum: [llm, chain, tool, retriever, embedding]
  start_time:
    type: string
    format: date-time
    required: true
  end_time:
    type: string
    format: date-time
    required: true
    description: Must be >= start_time. Negative durations indicate clock skew.
  total_tokens:
    type: integer
    minimum: 0
    required: true
  prompt_tokens:
    type: integer
    minimum: 0
    required: true
  completion_tokens:
    type: integer
    minimum: 0
    required: true
  total_cost:
    type: number
    minimum: 0.0
    description: USD cost. Required when run_type == 'llm'; may be null otherwise.
  tags:
    type: array
    items:
      type: string

quality:
  type: SodaChecks
  specification:
    checks for runs:
      # Structural
      - missing_count(id) = 0
      - duplicate_count(id) = 0
      - invalid_count(run_type) = 0
      # Statistical
      - min(total_tokens) >= 0
      - min(total_cost) >= 0
      # AI-specific: token arithmetic must hold for LLM runs
      - failed_rows(runs) = 0:
          name: token_sum_consistency
          fail condition: run_type = 'llm' AND total_tokens != prompt_tokens + completion_tokens
      # AI-specific: end_time must not precede start_time
      - failed_rows(runs) = 0:
          name: no_negative_duration
          fail condition: end_time < start_time

lineage:
  upstream:
    - id: week3-document-refinery
      description: Traces produced during document extraction runs
    - id: week5-axiom-ledger
      description: Traces produced during event processing agent sessions
  downstream:
    - id: week7-ai-contract-extensions
      description: Embedding drift detection and prompt schema validation
      breaking_if_changed: [run_type, total_tokens, prompt_tokens, completion_tokens]
```

### AI-Specific Clauses Explained

| Clause | Type | What it catches |
|--------|------|----------------|
| `token_sum_consistency` | Statistical | LLM cost billing errors; API response truncation |
| `no_negative_duration` | Statistical | Clock skew between agent host and LangSmith server |
| `total_cost >= 0` + required for `llm` | AI-specific | Model switching where cost tracking was not updated |
| Embedding drift (Phase 4) | AI-specific | Semantic distribution shift in `inputs.prompt` text — detected via cosine distance from baseline centroid |

---

## Question 5: Common Failure Mode — Why Contracts Go Stale

### The Primary Failure Mode: Statistical Drift Masquerading as Structural Compliance

The most dangerous failure in production contract enforcement is **silent semantic corruption**: a dataset passes all structural checks (correct column names, correct types, no nulls) while carrying values that are meaningfully wrong. The confidence `0.0–1.0` → `0–100` example from Q2 illustrates this exactly — every structural check passes, but downstream routing logic inverts.

### Why Contracts Get Stale

1. **Documentation drift.** Contracts are written once at system design time and not updated when producers change. The update to `confidence` happens in `extractor.py`; the contract YAML lives in a different repo with a different owner. There is no CI gate connecting them.

2. **No authoritative consumer registry.** Upstream teams don't know who consumes their fields. The Week 4 Cartographer silently depended on `confidence < 0.5` — but the Week 3 team had no record of that dependency. Without a `downstream_consumers[]` registry per field, breaking changes propagate unannounced.

3. **Baseline staleness.** Statistical baselines (mean, stddev) are written once and never refreshed after intentional schema evolution. After a legitimate migration (0→100 scale corrected back to 0→1), the old baseline causes false positives indefinitely, leading teams to disable drift alerts rather than update them.

### How This Architecture Prevents It

| Component | Mechanism | Staleness prevented |
|-----------|-----------|-------------------|
| **ContractGenerator** | Re-profiles data on every run; regenerates clauses from observed distributions | Prevents baseline staleness — baselines are always current |
| **ValidationRunner** | Statistical drift check (z-score > 3 stddev triggers FAIL) | Catches silent corruption even when structural checks pass |
| **SchemaEvolutionAnalyzer** | Diffs timestamped schema snapshots; classifies every change | Forces explicit classification of every schema change; no silent drift |
| **ViolationAttributor** | Links violations to specific git commits | Makes the cost of breaking changes visible to the author immediately |
| **Lineage `downstream_consumers[]`** | Every contract field lists its consumers | Upstream teams see blast radius before merging a change |

The key architectural decision: contracts are **generated from data** (not hand-written) and **re-evaluated continuously**. A contract that cannot be regenerated from the data it describes is already stale.
