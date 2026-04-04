| Page | What to verify |
|---|---|
| 🏠 Overview | 5 contracts, 7 runs, 15 failing checks (real findings), 14 attributed violations |
| 📄 Contracts (P1) | Pick week3 → Schema Clauses tab → confidence field shows min: 0.0, max: 1.0 · week5 → LLM annotations on ambiguous columns |
| ✅ Validation (P2A) | Pick the [INJECTED: confidence_scale] run → Failures tab → CRITICAL range violation + 1223σ drift |
| 🔍 Violations (P2B) | 14 violations → blame chain table → blast radius bar chart (customers.sql, stg_customers) |
| 📈 Schema Evolution (P3) | week5 diff → 🔴 BLOCK deployment · 9 breaking columns listed · compatible 4 column_added |
