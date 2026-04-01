# Data Contract Enforcer

Schema integrity and lineage attribution system for a 5-system AI pipeline.

Turns every inter-system data dependency into a formal, machine-checked contract. When a contract is violated — by a schema change, type drift, or statistical shift — the system traces it to the originating commit and produces a blast-radius report showing every downstream system affected.

## Systems Under Contract

| Week | System | Output |
|------|--------|--------|
| 1 | Intent-Code Correlator | `outputs/week1/intent_records.jsonl` |
| 2 | Digital Courtroom | `outputs/week2/verdicts.jsonl` |
| 3 | Document Refinery | `outputs/week3/extractions.jsonl` |
| 4 | Brownfield Cartographer | `outputs/week4/lineage_snapshots.jsonl` |
| 5 | Axiom Ledger (Event Sourcing) | `outputs/week5/events.jsonl` |

## Components

| Component | Entry Point | Role |
|-----------|-------------|------|
| ContractGenerator | `contracts/generator.py` | Auto-generates Bitol YAML contracts from JSONL outputs |
| ValidationRunner | `contracts/runner.py` | Executes contract checks, produces structured PASS/FAIL reports |
| ViolationAttributor | `contracts/attributor.py` | Git blame + lineage traversal to find the commit that caused a violation |
| SchemaEvolutionAnalyzer | `contracts/schema_analyzer.py` | Classifies schema changes as breaking/compatible, generates migration reports |
| AI Contract Extensions | `contracts/ai_extensions.py` | Embedding drift, prompt input validation, LLM output schema enforcement |
| ReportGenerator | `contracts/report_generator.py` | Stakeholder PDF report from live validation data |

## Setup

```bash
pip install ydata-profiling pandas numpy scikit-learn jsonschema pyyaml anthropic langsmith gitpython soda-core
```

Copy `.env.example` to `.env` and fill in your API keys.

## Usage

```bash
# Generate a contract from Week 3 output
python contracts/generator.py \
  --source outputs/week3/extractions.jsonl \
  --contract-id week3-document-refinery-extractions \
  --lineage outputs/week4/lineage_snapshots.jsonl \
  --output generated_contracts/

# Run validation
python contracts/runner.py \
  --contract generated_contracts/week3_extractions.yaml \
  --data outputs/week3/extractions.jsonl \
  --output validation_reports/
```

## Repository Layout

```
contracts/          # All enforcer components
generated_contracts/  # Auto-generated contract YAML files
validation_reports/   # Structured validation report JSON
violation_log/        # Violation records JSONL
schema_snapshots/     # Timestamped schema snapshots
enforcer_report/      # Stakeholder PDF + data
outputs/              # Input: JSONL outputs from weeks 1–5 (not committed)
DOMAIN_NOTES.md       # Domain research and architecture decisions
```
