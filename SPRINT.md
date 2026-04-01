# Data Contract Enforcer — Sprint Document

*Living document. Update ticket status and results as work completes. Resume from here in future sessions.*

---

## Sprint Goal

Build a fully working Data Contract Enforcer that validates schemas across 5 prior-week systems, traces violations to origin commits, and produces a stakeholder report — all grounded in real output data.

## Data Status (settled — do not regenerate)

| Week | Canonical File | Records | Source |
|------|---------------|---------|--------|
| 1 | `outputs/week1/intent_records.jsonl` | 12 | Generated from roo-code fork context |
| 2 | `outputs/week2/verdicts_canonical.jsonl` | 1 | Real Github-Evaluator self-eval, rubric_id from sha256(rubric.json) |
| 3 | `outputs/week3/extractions.jsonl` | 12 docs / 102 facts | Synthetic (actual ledger was run-metadata only, 1 record) |
| 4 | `outputs/week4/lineage_snapshots.jsonl` | 1 snapshot | Real .cartography/lineage_graph.json (13 nodes, 13 edges, jaffle_shop_classic) |
| 5 | `outputs/week5/events_canonical.jsonl` | 95 | Real Axiom-Ledger run |
| traces | `outputs/traces/runs.jsonl` | 95 | Synthetic (LangSmith project was empty) |

Key deviations documented in `scripts/migrate_to_canonical.py` and `DOMAIN_NOTES.md`.

---

## Tickets

### PHASE 0 — Domain Reconnaissance

| ID | Ticket | Status | Result |
|----|--------|--------|--------|
| P0-1 | Write DOMAIN_NOTES.md (5 questions) | ✅ DONE | Real runtime evidence in Q2 (confidence 0.317→32 silent failure); real cartographer graph in Q3; Bitol YAML in Q2+Q4; stat measurements embedded |
| P0-2 | Set up repo + directory structure | ✅ DONE | https://github.com/Natnael-Alemseged/data-contract-enforcer |
| P0-3 | Generate / migrate canonical output files for all weeks | ✅ DONE | See data status table above |

---

### PHASE 1 — ContractGenerator (`contracts/generator.py`)

**Goal:** CLI tool that profiles a JSONL file + lineage graph and emits a Bitol YAML contract + dbt schema.yml.

**CLI:**
```bash
python contracts/generator.py \
  --source outputs/week3/extractions.jsonl \
  --contract-id week3-document-refinery-extractions \
  --lineage outputs/week4/lineage_snapshots.jsonl \
  --output generated_contracts/
```

| ID | Ticket | Status | Result |
|----|--------|--------|--------|
| P1-1 | Stage 1 — `load_jsonl` + `flatten_for_profile` (handle nested arrays like `extracted_facts[]`) | 🔲 TODO | |
| P1-2 | Stage 2 — `profile_column`: dtype, null_fraction, cardinality, sample_values, numeric stats (min/max/mean/p25/p50/p75/p95/p99/stddev) | 🔲 TODO | |
| P1-3 | Stage 3 — `column_to_clause`: map profiles → Bitol schema clauses (confidence→range, _id→uuid, _at→date-time, enum detection) | 🔲 TODO | |
| P1-4 | Stage 4 — `inject_lineage`: load lineage_snapshots.jsonl, find downstream consumers of each field, add to contract | 🔲 TODO | |
| P1-5 | LLM annotation — for ambiguous columns, call Claude via OpenRouter to get plain-English description + business rule | 🔲 TODO | |
| P1-6 | dbt output — parallel `{name}_dbt.yml` with not_null/accepted_values/relationships tests | 🔲 TODO | |
| P1-7 | Schema snapshot write — on every run, write `schema_snapshots/{contract_id}/{timestamp}.yaml` | 🔲 TODO | |
| P1-8 | Run on week3 + week5 (required minimums) and measure clause correctness fraction (target >70%) | 🔲 TODO | |

**Acceptance:** `generated_contracts/week3_extractions.yaml` and `week5_events.yaml` exist, are human-readable, and >70% of clauses are correct without manual edits.

---

### PHASE 2A — ValidationRunner (`contracts/runner.py`)

**Goal:** Execute every clause in a contract against a data snapshot. Never crash. Always produce a complete report.

**CLI:**
```bash
python contracts/runner.py \
  --contract generated_contracts/week3_extractions.yaml \
  --data outputs/week3/extractions.jsonl \
  --output validation_reports/week3_$(date +%Y%m%d_%H%M).json
```

**Output schema:** `{report_id, contract_id, snapshot_id, run_timestamp, total_checks, passed, failed, warned, errored, results[]}`

| ID | Ticket | Status | Result |
|----|--------|--------|--------|
| P2A-1 | Structural checks: required field present (CRITICAL if null_fraction > 0) | 🔲 TODO | |
| P2A-2 | Structural checks: type match (number/integer/boolean/string) | 🔲 TODO | |
| P2A-3 | Structural checks: enum conformance (accepted_values) | 🔲 TODO | |
| P2A-4 | Structural checks: UUID pattern regex `^[0-9a-f-]{36}$` | 🔲 TODO | |
| P2A-5 | Structural checks: date-time format via `datetime.fromisoformat()` | 🔲 TODO | |
| P2A-6 | Statistical checks: range (min >= contract minimum, max <= contract maximum) | 🔲 TODO | |
| P2A-7 | Statistical drift: load `schema_snapshots/baselines.json`, z-score check (WARN >2σ, FAIL >3σ) | 🔲 TODO | |
| P2A-8 | Write baselines file after first run if none exists | 🔲 TODO | |
| P2A-9 | Inject known violation into week3 data (confidence × 100) and confirm FAIL is produced | 🔲 TODO | |
| P2A-10 | Run on week3 + week5; commit `validation_reports/thursday_baseline.json` | 🔲 TODO | |

**Acceptance:** Report JSON matches spec schema exactly. Injected confidence violation returns `status: FAIL, severity: CRITICAL`. No crashes on malformed input.

---

### PHASE 2B — ViolationAttributor (`contracts/attributor.py`)

**Goal:** When a check fails, traverse the lineage graph + git log to build a blame chain.

**CLI:**
```bash
python contracts/attributor.py \
  --violation violation_log/violations.jsonl \
  --lineage outputs/week4/lineage_snapshots.jsonl \
  --repo-path /path/to/source/repo \
  --output violation_log/
```

**Blame chain format:** `{violation_id, check_id, detected_at, blame_chain[{rank, file_path, commit_hash, author, commit_timestamp, commit_message, confidence_score}], blast_radius{affected_nodes, affected_pipelines, estimated_records}}`

| ID | Ticket | Status | Result |
|----|--------|--------|--------|
| P2B-1 | Load lineage graph from `lineage_snapshots.jsonl`; build adjacency for reverse BFS | 🔲 TODO | |
| P2B-2 | Implement reverse BFS from failing schema element to upstream producers | 🔲 TODO | |
| P2B-3 | Git integration: `git log --follow --since="14 days ago"` + `git blame -L` per upstream file | 🔲 TODO | |
| P2B-4 | Confidence scoring: `1.0 − (days_since_commit × 0.1) − (lineage_hops × 0.2)`, rank 1–5 candidates | 🔲 TODO | |
| P2B-5 | Blast radius: BFS forward from violating node to count affected downstream nodes + pipelines | 🔲 TODO | |
| P2B-6 | Write `violation_log/violations.jsonl` with full blame chain | 🔲 TODO | |
| P2B-7 | Test with injected confidence violation from P2A-9; verify blame chain points to correct file | 🔲 TODO | |

**Acceptance:** At least 1 attributed violation with a blame chain of ≥1 entry and a populated blast_radius.

---

### PHASE 3 — SchemaEvolutionAnalyzer (`contracts/schema_analyzer.py`)

**Goal:** Diff schema snapshots over time, classify every change, generate migration impact reports for breaking changes.

**CLI:**
```bash
python contracts/schema_analyzer.py \
  --contract-id week3-document-refinery-extractions \
  --since "7 days ago" \
  --output validation_reports/schema_evolution_week3.json
```

| ID | Ticket | Status | Result |
|----|--------|--------|--------|
| P3-1 | Load and diff two consecutive snapshots from `schema_snapshots/{contract_id}/` | 🔲 TODO | |
| P3-2 | Classify each detected change per taxonomy (add nullable / add non-nullable / rename / widen / narrow / remove / enum change) | 🔲 TODO | |
| P3-3 | Generate `migration_impact_{contract_id}_{timestamp}.json` for breaking changes (diff, verdict, blast radius, migration checklist, rollback plan) | 🔲 TODO | |
| P3-4 | Simulate the confidence scale change: manually write two snapshots (v1: float 0–1, v2: int 0–100), run analyzer, confirm it classifies as "narrowing — BREAKING" | 🔲 TODO | |

**Acceptance:** Analyzer correctly classifies the simulated confidence change as BREAKING with a migration impact report.

---

### PHASE 4A — AI Contract Extensions (`contracts/ai_extensions.py`)

**Goal:** Three extensions beyond standard data contracts: embedding drift, prompt input validation, LangSmith trace enforcement.

| ID | Ticket | Status | Result |
|----|--------|--------|--------|
| P4A-1 | **Embedding drift** — baseline run: embed 200 samples of `extracted_facts[*].text`, store centroid in `schema_snapshots/embedding_baselines.npz` | 🔲 TODO | |
| P4A-2 | Embedding drift — subsequent runs: embed fresh 200 samples, compute cosine distance from centroid, FAIL if drift > 0.15 | 🔲 TODO | |
| P4A-3 | **Prompt input validation** — JSON Schema for document metadata object passed to extraction prompt; quarantine non-conforming records to `outputs/quarantine/` | 🔲 TODO | |
| P4A-4 | **Trace schema enforcement** — run all clauses from the `langsmith-trace-records` contract (Q4 of DOMAIN_NOTES) against `outputs/traces/runs.jsonl`; report token arithmetic violations + negative durations | 🔲 TODO | |
| P4A-5 | Write `ai_metrics.json` with: embedding drift score, prompt schema violation rate, trace contract pass rate | 🔲 TODO | |

**Acceptance:** `ai_metrics.json` exists with real (non-null) numbers. Embedding drift baseline is stored. At least one trace contract check runs and reports a result.

---

### PHASE 4B — ReportGenerator (`contracts/report_generator.py`)

**Goal:** Auto-generate the stakeholder Enforcer Report from live validation data.

| ID | Ticket | Status | Result |
|----|--------|--------|--------|
| P4B-1 | Load and aggregate: all `validation_reports/*.json`, `violation_log/violations.jsonl`, `ai_metrics.json` | 🔲 TODO | |
| P4B-2 | Write `enforcer_report/report_data.json` — structured JSON with all validation results, violations, blast radii | 🔲 TODO | |
| P4B-3 | Generate `enforcer_report/report_{date}.pdf` — use `reportlab` or `weasyprint`; include: data flow diagram summary, per-system contract health, top violations, blast radius table, AI metrics | 🔲 TODO | |

**Acceptance:** PDF renders without errors and includes all 5 system contract results.

---

## Implementation Order

```
P0 (done) → P1 → P2A → P2B → P3 → P4A → P4B
              ↑
         Must have contracts before you can validate them
```

P1 and P2A are tightly coupled — build generator first, immediately test with runner.
P2B requires P2A output (a real FAIL result) to have something to attribute.
P3 requires at least 2 P1 snapshot runs.
P4A can be started in parallel with P3 once P2A is working.
P4B is last — it consumes everything.

---

## Key Design Decisions (settled)

| Decision | Choice | Rationale |
|----------|--------|-----------|
| LLM provider | Claude via OpenRouter (`OPEN_ROUTER_KEY`) | OpenAI key not set; OpenRouter key in .env |
| Embedding model | `text-embedding-3-small` via OpenRouter or direct | Small, fast, sufficient for drift detection |
| YAML format | Bitol v3.0.0 (`kind: DataContract, apiVersion: v3.0.0`) | Spec requirement |
| Quality checks format | SodaChecks (in `quality.specification`) | Spec requirement |
| Week 4 lineage graph | Real `.cartography/lineage_graph.json` (jaffle_shop_classic) | 13 nodes, 13 edges, real dbt lineage |
| Week 5 events | Real Axiom-Ledger run (95 events) | `events_canonical.jsonl` after migration |
| Week 3 extractions | Synthetic (actual ledger was run-metadata only) | Documented in DOMAIN_NOTES Q2 + migration script |
| Git integration | Python `gitpython` library | Already in requirements |
| Statistical drift | Z-score baseline in `schema_snapshots/baselines.json` | Per spec |

---

## Open Questions

| # | Question | Owner | Resolution |
|---|----------|-------|------------|
| OQ-1 | Week 1 output — the roo-code fork is too large to run; synthetic intent_records generated from ARCHITECTURE_NOTES context. Acceptable? | User | Pending |
| OQ-2 | Week 3 — only 12 synthetic extraction docs. Is this enough to demonstrate statistical profiling meaningfully? Could add more documents. | Dev | Likely fine; can extend if needed |
| OQ-3 | Embedding drift (P4A-1/2) — needs an API key that supports embeddings. OpenRouter supports this via `text-embedding-3-small`. Confirm budget OK. | User | Pending |
| OQ-4 | PDF generation library — `reportlab` (pure Python, no system deps) vs `weasyprint` (HTML→PDF, prettier). Preference? | User | Pending |

---

## Files Created So Far

```
contracts/                    # empty — implementation starts here
generated_contracts/          # empty — populated by P1
validation_reports/           # empty — populated by P2A
violation_log/                # empty — populated by P2B
schema_snapshots/             # empty — populated by P1 snapshot writes
enforcer_report/              # empty — populated by P4B
scripts/
  generate_outputs.py         # synthetic data generation (weeks 2–4, traces)
  migrate_to_canonical.py     # actual→canonical migration for all weeks
outputs/
  week1/intent_records.jsonl
  week2/verdicts_canonical.jsonl + rubric.json
  week3/extractions.jsonl + extraction_ledger.jsonl
  week4/lineage_snapshots.jsonl + .cartography/ (real)
  week5/events_canonical.jsonl
  traces/runs.jsonl
DOMAIN_NOTES.md               ✅ complete
SPRINT.md                     ✅ this file
README.md                     ✅ basic, will update as phases complete
```
