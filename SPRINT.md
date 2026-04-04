# Data Contract Enforcer — Sprint Document

*Living document. Update ticket status and results as work completes. Resume from here in future sessions.*

---

## Sprint Goal


Build a fully working Data Contract Enforcer that validates schemas across 5 prior-week systems, traces violations to origin commits via registry-first blast radius, and produces a stakeholder report — all grounded in real output data.

## Submission Deadlines

| Milestone | Deadline | Deliverables |
|-----------|----------|-------------|
| Interim (Thursday) | Thursday 03:00 UTC | GitHub link + Google Drive PDF (DOMAIN_NOTES, generator, runner, contracts for week3+week5, 1 validation report, registry with 4+ subscriptions) |
| Final (Sunday) | Sunday 03:00 UTC | GitHub link + Google Drive PDF + Demo Video (all components, 3+ violations, 2+ snapshots/contract, enforcer report, README) |

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

**Data threshold note:** Requirement says >=50 records for week3 and week5. Week 5 (95) meets threshold. Week 3 (12 docs) is below — documented in OQ-2.

---

## Tickets

### PHASE 0 — Domain Reconnaissance

| ID | Ticket | Status | Result |
|----|--------|--------|--------|
| P0-1 | Write DOMAIN_NOTES.md (5 questions) | ✅ DONE | Real runtime evidence in Q2 (confidence 0.317→32 silent failure); real cartographer graph in Q3; Bitol YAML in Q2+Q4; stat measurements embedded |
| P0-2 | Set up repo + directory structure | ✅ DONE | https://github.com/Natnael-Alemseged/data-contract-enforcer |
| P0-3 | Generate / migrate canonical output files for all weeks | ✅ DONE | See data status table above |
| P0-4 | Draw data flow diagram (6 systems: weeks 1–5 + LangSmith, annotated arrows with schema names + breaking fields) | 🔲 TODO | Required in Thursday PDF |

---

### PHASE 0.5 — ContractRegistry (`contract_registry/subscriptions.yaml`)

**Goal:** Bootstrap the registry BEFORE any generator code. Forces thinking about consumers before contracts.

**Rationale (from requirement):** The registry is the correct source for blast radius, not the lineage graph alone. The lineage graph enriches; the registry is authoritative. This distinction is what makes the system degrade gracefully from Tier 1 → Tier 2.

| ID | Ticket | Status | Result |
|----|--------|--------|--------|
| P0.5-1 | Create `contract_registry/subscriptions.yaml` with minimum 4 subscriptions: Week 3→Week 4, Week 4→Week 7, Week 5→Week 7, LangSmith→Week 7 | ✅ DONE | 5 subscriptions created |
| P0.5-2 | Each subscription must have `breaking_fields` with `field` + `reason` | ✅ DONE | All have breaking_fields with reasons |
| P0.5-3 | Include `validation_mode` per subscription (AUDIT/WARN/ENFORCE) | ✅ DONE | Mixed modes: ENFORCE for critical, AUDIT for observational |

**Schema:**
```yaml
subscriptions:
  - contract_id: week3-document-refinery-extractions
    subscriber_id: week4-cartographer
    subscriber_team: week4
    fields_consumed: [doc_id, extracted_facts, extraction_model]
    breaking_fields:
      - field: extracted_facts.confidence
        reason: used for node ranking; scale change breaks ranking logic
      - field: doc_id
        reason: primary key for node identity in lineage graph
    validation_mode: ENFORCE
    registered_at: '2025-01-10T09:00:00Z'
    contact: week4-team@org.com
```

**Acceptance:** 4+ subscriptions covering the required interfaces. Each has breaking_fields with reasons.

---

### PHASE 1 — ContractGenerator (`contracts/generator.py`)

**Goal:** CLI tool that profiles a JSONL file + lineage graph + registry and emits a Bitol YAML contract + dbt schema.yml + schema snapshot.

**CLI:**
```bash
python contracts/generator.py \
  --source outputs/week3/extractions.jsonl \
  --contract-id week3-document-refinery-extractions \
  --lineage outputs/week4/lineage_snapshots.jsonl \
  --registry contract_registry/subscriptions.yaml \
  --output generated_contracts/
```

**Required contracts (minimum):**

| Contract | File | Min Clauses | Threshold |
|----------|------|-------------|-----------|
| Week 1 intent_records | `week1_intent_records.yaml` | 8 | Thursday (optional), Sunday (required) |
| Week 3 extractions | `week3_extractions.yaml` | 8 | **Thursday** |
| Week 4 lineage | `week4_lineage.yaml` | 6 | Sunday |
| Week 5 events | `week5_events.yaml` | 6 | **Thursday** |
| LangSmith traces | `langsmith_traces.yaml` | 6 | Sunday |

| ID | Ticket | Status | Result |
|----|--------|--------|--------|
| P1-1 | Stage 1 — `load_jsonl` + `flatten_for_profile` (handle nested arrays like `extracted_facts[]`) | ✅ DONE | Handles all JSONL formats |
| P1-2 | Stage 2 — `profile_column`: dtype, null_fraction, cardinality, sample_values, numeric stats (min/max/mean/p25/p50/p75/p95/p99/stddev) | ✅ DONE | Full statistical profiling |
| P1-3 | Stage 3 — `column_to_clause`: map profiles → Bitol schema clauses (confidence→range 0.0–1.0, _id→uuid, _at→date-time, enum detection ≤8 cardinality) | ✅ DONE | Correct clause inference |
| P1-4 | Stage 4 — `inject_lineage_context`: load lineage graph + `contract_registry/subscriptions.yaml` for registry subscribers | ✅ DONE | `--registry` flag added, subscribers embedded in lineage section |
| P1-5 | LLM annotation — for ambiguous columns, call Claude via OpenRouter to get plain-English description + business rule | ✅ DONE | Via `--skip-llm` flag to control |
| P1-6 | dbt output — parallel `{name}_dbt.yml` with not_null/accepted_values/relationships tests | ✅ DONE | All 6 contracts have dbt counterparts |
| P1-7 | Schema snapshot write — on every run, write `schema_snapshots/{contract_id}/{timestamp}.yaml` | ✅ DONE | All contracts have >=2 snapshots |
| P1-8 | Run on all 6 contracts (week1–5 + langsmith) | ✅ DONE | 6 contracts generated successfully |

**Acceptance:** `generated_contracts/week3_extractions.yaml` and `week5_events.yaml` exist with >=8 and >=6 clauses respectively, are Bitol-compatible, human-readable, and >70% of clauses are correct without manual edits. Evaluator runs generator without errors.

---

### PHASE 2A — ValidationRunner (`contracts/runner.py`)

**Goal:** Execute every clause in a contract against a data snapshot. Never crash. Always produce a complete report. Supports `--mode` flag (AUDIT/WARN/ENFORCE).

**CLI:**
```bash
python contracts/runner.py \
  --contract generated_contracts/week3_extractions.yaml \
  --data outputs/week3/extractions.jsonl \
  --mode AUDIT \
  --output validation_reports/week3_$(date +%Y%m%d_%H%M).json
```

**Mode behavior:**

| Mode | Behavior | When to use |
|------|----------|-------------|
| AUDIT (default) | Run checks, log results, never block | First run on any new dataset; calibration period |
| WARN | Block on CRITICAL only. Warn on HIGH/MEDIUM. Pass data with annotations | After calibration; downstream can handle annotated data |
| ENFORCE | Block pipeline on any CRITICAL or HIGH violation. Quarantine data. Alert | Mature contracts with low false positive rate |

**Output schema:** `{report_id, contract_id, snapshot_id, run_timestamp, total_checks, passed, failed, warned, errored, results[]}`

**Severity levels:** CRITICAL (structural/type), HIGH (statistical drift >3σ), MEDIUM (drift 2–3σ), LOW (informational), WARNING (near-threshold)

**Partial failure rule:** If a check cannot execute (column missing), return `status: "ERROR"` with diagnostic and continue. Never crash.

| ID | Ticket | Status | Result |
|----|--------|--------|--------|
| P2A-1 | Structural checks: required field present (CRITICAL if nulls found for required:true) | ✅ DONE | |
| P2A-2 | Structural checks: type match (number/integer/boolean/string) | ✅ DONE | |
| P2A-3 | Structural checks: enum conformance (accepted_values) — report count + sample of violators | ✅ DONE | |
| P2A-4 | Structural checks: UUID pattern regex `^[0-9a-f-]{36}$` — sample 100 if >10k records | ✅ DONE | |
| P2A-5 | Structural checks: date-time format via `datetime.fromisoformat()` — count failures | ✅ DONE | |
| P2A-6 | Statistical checks: range (min >= contract minimum, max <= contract maximum) — catches 0.0–1.0 → 0–100 | ✅ DONE | Fires CRITICAL on injected violation |
| P2A-7 | Statistical drift: load `schema_snapshots/baselines.json`, z-score check (WARN >2σ, FAIL >3σ) | ✅ DONE | z=1223.4σ drift detected on scale change |
| P2A-8 | Write baselines file after first run if none exists | ✅ DONE | |
| P2A-9 | Implement `--mode` flag (AUDIT/WARN/ENFORCE) with correct blocking behavior | ✅ DONE | `--mode` with pipeline_action in report |
| P2A-10 | Inject known violation into week3 data (confidence × 100) and confirm FAIL on both range AND statistical drift | ✅ DONE | Both CRITICAL range + HIGH drift fire |
| P2A-11 | Run on week3 + week5 clean data first (establishes baseline), then on violated data | ✅ DONE | Clean passes, violated fails |

**Acceptance:** Report JSON matches spec schema exactly. Injected confidence violation returns `status: FAIL, severity: CRITICAL` for range check AND fires statistical drift (mean ~0.87 → ~87.0, well over 3σ). No crashes on malformed input.

---

### PHASE 2B — ViolationAttributor (`contracts/attributor.py`)

**Goal:** When a check fails, use **registry as primary blast radius source** and lineage graph as enrichment. Build a blame chain via git log.

**Architecture (from requirement):**
- Step 1: **Registry blast radius query** (PRIMARY) — load `contract_registry/subscriptions.yaml`, find subscribers where `breaking_fields` contains the failing field
- Step 2: **Lineage transitive depth** (ENRICHMENT) — BFS from producer node, compute contamination_depth
- Step 3: **Git blame** — `git log --follow --since="14 days ago"` + `git blame -L` per upstream file
- Step 4: **Write violation log** — registry-sourced blast radius + lineage enrichment + blame chain

**CLI:**
```bash
python contracts/attributor.py \
  --violation validation_reports/violated.json \
  --lineage outputs/week4/lineage_snapshots.jsonl \
  --registry contract_registry/subscriptions.yaml \
  --output violation_log/violations.jsonl
```

**Blame chain format:**
```json
{
  "violation_id": "uuid-v4",
  "check_id": "week3.extracted_facts.confidence.range",
  "detected_at": "ISO 8601",
  "blast_radius": {
    "source": "registry",
    "direct_subscribers": [...],
    "transitive_nodes": [...],
    "contamination_depth": 2,
    "note": "direct_subscribers from registry; transitive_nodes from lineage graph enrichment"
  },
  "blame_chain": [{rank, file_path, commit_hash, author, commit_timestamp, commit_message, confidence_score}],
  "records_failing": 847
}
```

**Confidence score formula:** `1.0 − (days_since_commit × 0.1) − (lineage_hops × 0.2)`, rank 1–5 candidates max.

| ID | Ticket | Status | Result |
|----|--------|--------|--------|
| P2B-1 | Step 1 — Registry blast radius query: load subscriptions.yaml, find subscribers where breaking_fields matches failing field | ✅ DONE | `registry_blast_radius()` function; `--registry` flag |
| P2B-2 | Step 2 — Lineage transitive depth: BFS from producer node, compute contamination_depth (enrichment only) | ✅ DONE | `compute_lineage_enrichment()` renamed from old blast radius |
| P2B-3 | Step 3 — Git integration: `git log --follow --since="14 days ago"` + `git blame -L` per upstream file | ✅ DONE | Falls back to git_history.txt |
| P2B-4 | Step 4 — Confidence scoring: `1.0 − (days × 0.1) − (hops × 0.2)`, rank 1–5 candidates | ✅ DONE | |
| P2B-5 | Step 5 — Write `violation_log/violations.jsonl` with full blast_radius (registry-sourced) + blame chain | ✅ DONE | `source: "registry"`, direct_subscribers populated |
| P2B-6 | Test with injected confidence violation; verify blame chain + registry blast radius populated | ✅ DONE | 2 registry subscribers found for confidence violation |
| P2B-7 | Ensure minimum 3 violation entries: >=1 real, >=1 injected with `injection_note: true` | ✅ DONE | 3 entries: 2 injected, 1 real (week2 rubric_id) |

**Acceptance:** At least 3 attributed violations. Registry-sourced blast_radius with direct_subscribers populated. Blame chain of >=1 entry per violation. Evaluator can trace from failing check → git commit.

---

### PHASE 3 — SchemaEvolutionAnalyzer (`contracts/schema_analyzer.py`)

**Goal:** Diff schema snapshots over time, classify every change per taxonomy, generate migration impact reports for breaking changes.

**CLI:**
```bash
python contracts/schema_analyzer.py \
  --contract-id week3-document-refinery-extractions \
  --output validation_reports/schema_evolution_week3.json
```

**Change taxonomy:**

| Change | Compatible? | Action |
|--------|-------------|--------|
| Add nullable field | Yes | None |
| Add required field | **No** | Coordinate, provide default/migration |
| Rename field | **No** | Deprecation period w/ alias, notify registry subscribers |
| Widen type (INT→BIGINT) | Usually yes | Validate no precision loss |
| Narrow type (float 0.0–1.0 → int 0–100) | **No — data loss** | CRITICAL. Migration plan + rollback. Re-establish baseline. |
| Remove field | **No** | Two-sprint deprecation minimum |
| Enum: add value | Yes | Notify subscribers |
| Enum: remove value | **No** | Blast radius required |

**Snapshot discipline:** Ensure >=2 timestamped snapshots per contract. If only 1 exists, re-run generator on violated data to create a second snapshot with different stats.

| ID | Ticket | Status | Result |
|----|--------|--------|--------|
| P3-1 | Load and diff two consecutive snapshots from `schema_snapshots/{contract_id}/` | ✅ DONE | --all flag diffs all contracts |
| P3-2 | Classify each change per taxonomy (add nullable/required, rename, widen, narrow, remove, enum change) | ✅ DONE | column_removed, type_changed, range_narrowed, cardinality_spike, stat_drift |
| P3-3 | Generate migration impact report for breaking changes | ✅ DONE | Evolution reports in validation_reports/ |
| P3-4 | Week5 evolution shows real BREAKING changes (column removals, range narrowing) | ✅ DONE | 9→43 breaking changes detected across week5 snapshots |

**Acceptance:** Analyzer correctly classifies the simulated confidence change as BREAKING. Migration impact report includes diff, verdict, blast radius, migration checklist, and rollback plan.

---

### PHASE 4A — AI Contract Extensions (`contracts/ai_extensions.py`)

**Goal:** Three extensions beyond standard data contracts: embedding drift, prompt input validation, LLM output schema violation rate.

**CLI:**
```bash
python contracts/ai_extensions.py \
  --extractions outputs/week3/extractions.jsonl \
  --verdicts outputs/week2/verdicts_canonical.jsonl \
  --output validation_reports/ai_extensions.json
```

| ID | Ticket | Status | Result |
|----|--------|--------|--------|
| P4A-1 | **Embedding drift** — baseline run: embed 200 samples via `text-embedding-3-small` (OpenRouter), store centroid in `schema_snapshots/embedding_baselines.npz` | ✅ DONE | Baseline set, 102 samples embedded |
| P4A-2 | Embedding drift — subsequent runs: cosine distance from centroid, FAIL if drift > 0.15 | ✅ DONE | PASS, drift=0.0 (same data) |
| P4A-3 | **Prompt input validation** — JSON Schema (draft-07) for document metadata; quarantine to `outputs/quarantine/` | ✅ DONE | 12/12 valid, 0 quarantined |
| P4A-4 | **LLM output schema violation rate** — check `overall_verdict` in {PASS, FAIL, WARN}; track rate + trend | ✅ DONE | rate=0.0, 0/1 violations |
| P4A-5 | Write `validation_reports/ai_extensions.json` | ✅ DONE | All 3 extension results with real numbers |

**Acceptance:** `ai_extensions.json` exists with real (non-null) numbers. Embedding drift baseline is stored. All 3 extensions run on real data. Rising violation rate triggers WARN.

---

### PHASE 4B — ReportGenerator (`contracts/report_generator.py`)

**Goal:** Auto-generate the stakeholder Enforcer Report from live validation data. Must be machine-generated, not hand-written.

**CLI:**
```bash
python contracts/report_generator.py
```

**Required report sections:**

1. **Data Health Score** — `(checks_passed / total_checks) × 100`, adjusted down by 20 points per CRITICAL violation. One-sentence narrative.
2. **Violations this week** — Count by severity. Plain-language description of top 3 violations (name failing system, field, impact on downstream consumers via registry subscribers).
3. **Schema changes detected** — Plain-language summary, compatibility verdict, required action.
4. **AI system risk assessment** — Embedding drift within bounds? LLM output violation rate stable?
5. **Recommended actions** — 3 prioritized actions, each specific enough to open a ticket without follow-up (e.g., "update src/week3/extractor.py to output confidence as float 0.0–1.0 per contract week3-document-refinery-extractions clause extracted_facts.confidence.range").

| ID | Ticket | Status | Result |
|----|--------|--------|--------|
| P4B-1 | Load and aggregate: all `validation_reports/*.json`, `violation_log/violations.jsonl`, `validation_reports/ai_extensions.json` | ✅ DONE | 7 reports, 3 violations, AI extensions loaded |
| P4B-2 | Write `enforcer_report/report_data.json` — structured JSON with health score, violations, blast radii, AI metrics, recommendations | ✅ DONE | Health score: 60/100 |
| P4B-3 | Generate `enforcer_report/report_{date}.md` — Markdown report with all 5 sections | ✅ DONE | report_2026-04-04.md with all 5 sections |

**Acceptance:** `report_data.json` has `data_health_score` between 0–100 with real numbers matching validation runs. Recommendations reference real file paths from this repository. A non-engineer can identify the correct action from the report.

---

## Implementation Order

```
P0 (done) → P0.5 (registry) → P1 → P2A → P2B → P3 → P4A → P4B
                ↑                ↑
    Registry BEFORE contracts    Must have contracts before you can validate them
```

P0.5 is first — the registry forces consumer-first thinking.
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
| Blast radius source | **Registry (primary) + lineage (enrichment)** | Requirement: registry-first. Degrades gracefully Tier 1→2→3 |
| Enforcement mode | AUDIT/WARN/ENFORCE via `--mode` flag, default AUDIT | Requirement: all three modes. Start AUDIT to avoid false-positive blocking |
| Report format | Markdown (`enforcer_report/report_{date}.md`) | OQ-4 resolved: no PDF |

---

## Key Architectural Principles (from requirement)

1. **Enforcement always runs at the consumer** — ValidationRunner runs at consumer ingestion boundary, never inside producer. SchemaEvolutionAnalyzer is the exception (producer-side pre-deploy gate).

2. **Blast radius comes from the registry, not the lineage graph** — Registry = who depends on you. Lineage = how deeply the contamination spreads within systems you own. Registry is primary; lineage enriches.

3. **Three trust boundary tiers** — Tier 1 (this project): own everything, registry + lineage. Tier 2 (multi-team): registry primary, lineage within your systems only. Tier 3 (cross-company): registry subscriber count only.

---

## Open Questions

| # | Question | Owner | Resolution |
|---|----------|-------|------------|
| OQ-1 | Week 1 output — the roo-code fork is too large to run; synthetic intent_records generated from ARCHITECTURE_NOTES context. Acceptable? | User | Pending |
| OQ-2 | Week 3 — only 12 synthetic extraction docs (requirement says >=50). Is this enough to demonstrate statistical profiling meaningfully? Could add more documents. | Dev | Likely fine; can extend if needed |
| OQ-3 | Embedding drift (P4A-1/2) — needs an API key that supports embeddings. OpenRouter supports this via `text-embedding-3-small`. Confirm budget OK. | User | ✅ Use OpenRouter `text-embedding-3-small` with existing `OPEN_ROUTER_KEY` |
| OQ-4 | PDF generation library. | User | ✅ No PDF — generate report as `enforcer_report/report_{date}.md` instead |

---

## Required Directory Structure (from spec)

```
your-week7-repo/
├── contracts/
│   ├── generator.py           # ContractGenerator entry point
│   ├── runner.py              # ValidationRunner entry point
│   ├── attributor.py          # ViolationAttributor entry point
│   ├── schema_analyzer.py     # SchemaEvolutionAnalyzer entry point
│   ├── ai_extensions.py       # AI Contract Extensions entry point
│   └── report_generator.py    # EnforcerReport entry point
├── contract_registry/
│   └── subscriptions.yaml     # Consumer dependency registry (min 4 subscriptions)
├── generated_contracts/       # OUTPUT: auto-generated YAML contract files
│   ├── week1_intent_records.yaml
│   ├── week3_extractions.yaml
│   ├── week4_lineage.yaml
│   ├── week5_events.yaml
│   └── langsmith_traces.yaml
├── validation_reports/        # OUTPUT: structured validation report JSON
├── violation_log/             # OUTPUT: violation records JSONL (min 3: 1 real + 1 injected)
├── schema_snapshots/          # OUTPUT: timestamped schema snapshots (min 2 per contract)
├── enforcer_report/           # OUTPUT: stakeholder report + data
├── outputs/                   # INPUT: weeks 1–5 outputs
│   ├── week1/intent_records.jsonl
│   ├── week2/verdicts_canonical.jsonl + rubric.json
│   ├── week3/extractions.jsonl + extraction_ledger.jsonl
│   ├── week4/lineage_snapshots.jsonl + .cartography/
│   ├── week5/events_canonical.jsonl
│   └── traces/runs.jsonl
├── scripts/
│   ├── generate_outputs.py
│   └── migrate_to_canonical.py
├── DOMAIN_NOTES.md            ✅ complete
├── SPRINT.md                  ✅ this file
└── README.md                  # Must enable evaluator to reproduce all steps on fresh clone
```

---

## Submission Checklists

### Thursday Interim Checklist

- [ ] GitHub link submitted (public or evaluator as collaborator)
- [ ] Google Drive PDF link (opens without login)
- [ ] `DOMAIN_NOTES.md` — all 5 questions with evidence + examples from own systems (min 800 words)
- [ ] `contract_registry/subscriptions.yaml` — min 4 subscriptions, breaking_fields with reasons
- [ ] `generated_contracts/week3_extractions.yaml` — min 8 clauses, Bitol-compatible, confidence 0.0–1.0 range clause
- [ ] `generated_contracts/week5_events.yaml` — min 6 clauses
- [ ] `contracts/generator.py` — evaluator runs it without errors, produces YAML + snapshot
- [ ] `contracts/runner.py` — evaluator runs it, produces validation report JSON matching spec schema
- [ ] `validation_reports/` — at least 1 real validation report + baselines.json
- [ ] PDF: Data flow diagram (6 systems, annotated arrows)
- [ ] PDF: Contract coverage table
- [ ] PDF: Registry snapshot (full subscriptions.yaml with rationale)
- [ ] PDF: First validation run results (real numbers)
- [ ] PDF: Reflection (max 400 words)

### Sunday Final Checklist

- [ ] All Thursday items present and up to date
- [ ] `contracts/attributor.py` — registry as primary blast radius source; violation log with blame chain
- [ ] `contracts/schema_analyzer.py` — diffs 2 snapshots; classifies confidence scale change as BREAKING
- [ ] `contracts/ai_extensions.py` — all 3 checks on real data
- [ ] `violation_log/violations.jsonl` — min 3 entries (>=1 real, >=1 injected with `injection_note: true`)
- [ ] `schema_snapshots/` — min 2 timestamped snapshots per contract
- [ ] `enforcer_report/report_data.json` — machine-generated, health score 0–100, recommendations reference real paths
- [ ] `README.md` — evaluator reproduces all steps on fresh clone, expected output per command
- [ ] PDF: Enforcer Report (auto-generated, labelled)
- [ ] PDF: Violation deep-dive (registry blast radius + blame chain to git commit)
- [ ] PDF: AI extension results (real drift score + violation rate with trends)
- [ ] PDF: Schema evolution case study (diff, verdict, migration checklist, rollback plan)
- [ ] PDF: Trust boundary reflection (what changes at Tier 2? recommend real tool for 50-team org)
- [ ] Video demo (max 6 min): contract gen → violation detection → blame chain → schema evolution → AI extensions → enforcer report
