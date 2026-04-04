# Data Contract Enforcer

Schema integrity and lineage attribution system for a 5-system AI pipeline.

Turns every inter-system data dependency into a formal, machine-checked contract. When a contract is violated — by a schema change, type drift, or statistical shift — the system traces it to the originating commit and produces a blast-radius report showing every downstream system affected.

**Week 7 · 10 Academy TRP1 · All Phases Complete (P0–P4B)**

---

## Systems Under Contract

| Week | System | Canonical Output |
|------|--------|-----------------|
| 1 | Intent-Code Correlator | `outputs/week1/intent_records.jsonl` |
| 2 | Digital Courtroom | `outputs/week2/verdicts_canonical.jsonl` |
| 3 | Document Refinery | `outputs/week3/extractions.jsonl` |
| 4 | Brownfield Cartographer | `outputs/week4/lineage_snapshots.jsonl` |
| 5 | Axiom Ledger (Event Sourcing) | `outputs/week5/events_canonical.jsonl` |

---

## Components

| Component | File | Role | Status |
|-----------|------|------|--------|
| ContractRegistry | `contract_registry/subscriptions.yaml` | Consumer dependency registry (blast radius primary source) | ✅ Done |
| ContractGenerator | `contracts/generator.py` | Profiles JSONL → Bitol v3.0.0 YAML + dbt schema.yml + snapshot | ✅ Done |
| ValidationRunner | `contracts/runner.py` | Executes schema checks (type, range, enum, uuid, drift), produces PASS/FAIL reports | ✅ Done |
| ViolationAttributor | `contracts/attributor.py` | Registry-first blast radius + lineage BFS + git blame → blame chain | ✅ Done |
| SchemaEvolutionAnalyzer | `contracts/schema_analyzer.py` | Diffs consecutive snapshots, classifies BREAKING/WARN/COMPATIBLE changes | ✅ Done |
| AI Contract Extensions | `contracts/ai_extensions.py` | Embedding drift, prompt input validation, LLM output schema rate | ✅ Done |
| ReportGenerator | `contracts/report_generator.py` | Auto-generates stakeholder report from live validation data | ✅ Done |

---

## Setup

Requires Python 3.12 (ydata-profiling is incompatible with 3.13+).

```bash
# Create venv with uv
uv venv --python 3.12
source .venv/bin/activate

# Install dependencies
uv pip install -r requirements.txt
```

Copy `.env.example` to `.env` and set:
```
OPEN_ROUTER_KEY=...        # for LLM annotation (Claude-3-Haiku via OpenRouter)
LANGSMITH_TRACING=true
LANGSMITH_API_KEY=...      # for @traceable instrumentation
LANGSMITH_PROJECT=...
```

---

## Running the System (Step-by-Step)

Follow these steps on a fresh clone to reproduce all results.

### Prerequisites

```bash
pip install -r requirements.txt
# Requires: outputs/week3/extractions.jsonl (>=12 records)
# Requires: outputs/week4/lineage_snapshots.jsonl
# Requires: outputs/week5/events_canonical.jsonl (>=50 records)
# Requires: outputs/week2/verdicts_canonical.jsonl
```

### Step 1: Verify registry (already committed)

```bash
cat contract_registry/subscriptions.yaml | grep subscriber_id
# Expected: at least 5 subscriber entries
```

### Step 2: Generate contracts

```bash
# Week 3 (required minimum)
python contracts/generator.py \
  --source outputs/week3/extractions.jsonl \
  --contract-id week3-document-refinery-extractions \
  --lineage outputs/week4/lineage_snapshots.jsonl \
  --registry contract_registry/subscriptions.yaml \
  --output generated_contracts/ \
  --skip-llm
# Expected: generated_contracts/week3-document-refinery-extractions.yaml (>=8 clauses)
#           generated_contracts/week3-document-refinery-extractions_dbt.yml
#           schema_snapshots/week3-document-refinery-extractions/<timestamp>.yaml

# Week 5 (required minimum)
python contracts/generator.py \
  --source outputs/week5/events_canonical.jsonl \
  --contract-id week5-axiom-ledger-events \
  --lineage outputs/week4/lineage_snapshots.jsonl \
  --registry contract_registry/subscriptions.yaml \
  --output generated_contracts/ \
  --skip-llm
# Expected: generated_contracts/week5-axiom-ledger-events.yaml (>=6 clauses)
```

### Step 3: Validate clean data (establishes baselines)

```bash
python contracts/runner.py \
  --contract generated_contracts/week3-document-refinery-extractions.yaml \
  --data outputs/week3/extractions.jsonl \
  --mode AUDIT \
  --output validation_reports/
# Expected: all checks PASS, baselines written to schema_snapshots/baselines.json
```

### Step 4: Inject violation and validate (ENFORCE mode)

```bash
python contracts/runner.py \
  --contract generated_contracts/week3-document-refinery-extractions.yaml \
  --data outputs/week3/extractions.jsonl \
  --mode ENFORCE \
  --inject-violation confidence_scale \
  --output validation_reports/
# Expected: FAIL for confidence range (CRITICAL) AND statistical drift (HIGH)
#           pipeline_action: BLOCK
```

### Step 5: Attribute violations (registry-first blast radius)

```bash
python contracts/attributor.py \
  --report validation_reports/<latest_confidence_scale_report>.json \
  --lineage outputs/week4/lineage_snapshots.jsonl \
  --registry contract_registry/subscriptions.yaml \
  --output violation_log/
# Expected: violation_log/violations.jsonl with blame chain + registry blast radius
# Note: --violation is also accepted as an alias for --report
```

### Step 6: Schema evolution analysis

```bash
python contracts/schema_analyzer.py --all --output validation_reports/
# Expected: *_evolution.jsonl files with BREAKING/WARN/COMPATIBLE classifications

# Single contract to file:
python contracts/schema_analyzer.py \
  --contract-id week3-document-refinery-extractions \
  --output validation_reports/schema_evolution_week3.json
```

### Step 7: AI Contract Extensions

```bash
python contracts/ai_extensions.py \
  --extractions outputs/week3/extractions.jsonl \
  --verdicts outputs/week2/verdicts_canonical.jsonl \
  --output validation_reports/ai_extensions.json
# Expected: ai_extensions.json with embedding_drift, prompt_input_validation, output_violation_rate
```

### Step 8: Generate Enforcer Report

```bash
python contracts/report_generator.py
# Expected: enforcer_report/report_data.json (data_health_score 0-100)
#           enforcer_report/report_<date>.md (5 sections: health score, violations, schema changes, AI risk, recommendations)
```

### CLI Reference

| Flag | Script | Values | Description |
|------|--------|--------|-------------|
| `--mode` | runner.py | AUDIT / WARN / ENFORCE | AUDIT: log only. WARN: block CRITICAL. ENFORCE: block CRITICAL+HIGH. Default: AUDIT |
| `--registry` | generator.py, attributor.py | path to subscriptions.yaml | Injects registry subscribers into contracts / blast radius |
| `--skip-llm` | generator.py | flag | Skip Claude LLM annotation step |
| `--inject-violation` | runner.py | confidence_scale / missing_required / bad_enum | Inject a known violation for testing |
| `--all` | schema_analyzer.py | flag | Analyze all contracts in snapshot directory |
| `--violation` | attributor.py | path | Alias for --report |

### Verify end-to-end

```bash
# Check violation count (need >=3, >=1 injected, >=1 real)
wc -l violation_log/violations.jsonl

# Check health score
python3 -c "import json; print(json.load(open('enforcer_report/report_data.json'))['data_health_score'])"

# Check report sections
head -50 enforcer_report/report_*.md
```

---

## Key Findings (Phases 1–3)

| Finding | Severity | System |
|---------|----------|--------|
| `confidence` scale silent change (×100) caught by range check | CRITICAL | Week 3 |
| `aggregate_id` uses `loan-APEX-*` business keys, not UUID-v4 | CRITICAL | Week 5 |
| `rubric_id` is SHA256 (64 chars), not UUID-v4 (36 chars) | CRITICAL | Week 2 |
| Week 5 canonical migration: 9 breaking column changes detected | BREAKING | Week 5 |
| Statistical drift: confidence mean shifted 1,223σ after scale injection | HIGH | Week 3 |

---

## Repository Layout

```
contracts/
  generator.py          # Phase 1 — ContractGenerator
  runner.py             # Phase 2A — ValidationRunner
  attributor.py         # Phase 2B — ViolationAttributor
  schema_analyzer.py    # Phase 3 — SchemaEvolutionAnalyzer
  ai_extensions.py      # Phase 4A — AI Contract Extensions
  report_generator.py   # Phase 4B — ReportGenerator
contract_registry/
  subscriptions.yaml    # Consumer dependency registry (5 subscriptions)
scripts/
  verify_contracts.py   # Streamlit dashboard
  generate_outputs.py   # Synthetic data generation
  migrate_to_canonical.py  # Actual → canonical schema migration
generated_contracts/    # Bitol YAML + dbt schema.yml (6 contracts)
validation_reports/     # Structured JSON validation reports
violation_log/          # Attributed violations JSONL
schema_snapshots/       # Timestamped schema snapshots per contract
enforcer_report/        # Auto-generated stakeholder report (JSON + Markdown)
outputs/                # Canonical JSONL inputs
DOMAIN_NOTES.md         # 5 graded domain questions with real runtime evidence
SPRINT.md               # Living sprint document with ticket status
```
