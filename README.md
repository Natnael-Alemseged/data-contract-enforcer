# Data Contract Enforcer

Schema integrity and lineage attribution system for a 5-system AI pipeline.

Turns every inter-system data dependency into a formal, machine-checked contract. When a contract is violated — by a schema change, type drift, or statistical shift — the system traces it to the originating commit and produces a blast-radius report showing every downstream system affected.

**Week 7 · 10 Academy TRP1 · Phases 1–3 complete**

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

## Components (Phases 1–3)

| Component | File | Role | Status |
|-----------|------|------|--------|
| ContractGenerator | `contracts/generator.py` | Profiles JSONL → Bitol v3.0.0 YAML + dbt schema.yml + snapshot | ✅ Done |
| ValidationRunner | `contracts/runner.py` | Executes schema checks (type, range, enum, uuid, drift), produces PASS/FAIL reports | ✅ Done |
| ViolationAttributor | `contracts/attributor.py` | Lineage BFS + git blame → ranked blame chain + blast radius | ✅ Done |
| SchemaEvolutionAnalyzer | `contracts/schema_analyzer.py` | Diffs consecutive snapshots, classifies BREAKING/WARN/COMPATIBLE changes | ✅ Done |

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

## Usage

```bash
# 1. Generate a contract
python contracts/generator.py \
  --source outputs/week3/extractions.jsonl \
  --contract-id week3-document-refinery-extractions \
  --lineage outputs/week4/lineage_snapshots.jsonl \
  --output generated_contracts/

# 2. Run validation (with optional violation injection for testing)
python contracts/runner.py \
  --contract generated_contracts/week3-document-refinery-extractions.yaml \
  --data outputs/week3/extractions.jsonl \
  --output validation_reports/ \
  [--inject-violation confidence_scale|missing_required|bad_enum]

# 3. Attribute violations to origin commits
python contracts/attributor.py \
  --report validation_reports/week3-document-refinery-extractions_*.json \
  --lineage outputs/week4/lineage_snapshots.jsonl \
  --output violation_log/

# 4. Analyze schema evolution across snapshots
python contracts/schema_analyzer.py \
  --contract-id week3-document-refinery-extractions \
  --snapshots schema_snapshots/ \
  --output migration_reports/

# Run analyzer across all contracts
python contracts/schema_analyzer.py --all --snapshots schema_snapshots/ --output migration_reports/

# 5. Verify everything in the browser
uv run streamlit run scripts/verify_contracts.py --browser.gatherUsageStats false
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
scripts/
  verify_contracts.py   # Streamlit dashboard (Phases 1–3)
  generate_outputs.py   # Synthetic data generation
  migrate_to_canonical.py  # Actual → canonical schema migration
generated_contracts/    # Bitol YAML + dbt schema.yml (5 contracts)
validation_reports/     # Structured JSON validation reports
violation_log/          # Attributed violations JSONL
schema_snapshots/       # Timestamped schema snapshots per contract
migration_reports/      # Schema evolution diffs JSONL
enforcer_report/        # Interim stakeholder report (Markdown)
outputs/                # Canonical JSONL inputs (not committed — large files)
DOMAIN_NOTES.md         # 5 graded domain questions with real runtime evidence
SPRINT.md               # Living sprint document with ticket status
```
