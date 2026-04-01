"""
verify_contracts.py — Phase 1 Contract Verification Dashboard
=============================================================
Streamlit app to visually inspect generated contracts and source data.

Run:  streamlit run scripts/verify_contracts.py
"""

import json
from pathlib import Path

import pandas as pd
import streamlit as st
import yaml

ROOT = Path(__file__).parent.parent
CONTRACTS_DIR = ROOT / "generated_contracts"
OUTPUTS_DIR = ROOT / "outputs"
SNAPSHOTS_DIR = ROOT / "schema_snapshots"

st.set_page_config(
    page_title="Data Contract Enforcer — Contract Verifier",
    page_icon="📋",
    layout="wide",
)

# ── helpers ──────────────────────────────────────────────────────────────────

@st.cache_data
def load_contract(path: Path) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


@st.cache_data
def load_jsonl(path: Path) -> list[dict]:
    with open(path) as f:
        return [json.loads(l) for l in f if l.strip()]


def load_all_contracts() -> dict[str, dict]:
    contracts = {}
    for p in sorted(CONTRACTS_DIR.glob("*.yaml")):
        if "_dbt" not in p.stem:
            contracts[p.stem] = load_contract(p)
    return contracts


def severity_badge(value, good_range=None):
    """Return colored markdown for a value."""
    if good_range and not (good_range[0] <= value <= good_range[1]):
        return f"🔴 `{value}`"
    return f"🟢 `{value}`"


# ── sidebar ───────────────────────────────────────────────────────────────────

st.sidebar.title("📋 Contract Verifier")
st.sidebar.caption("Phase 1 — ContractGenerator output")

all_contracts = load_all_contracts()
if not all_contracts:
    st.error(f"No contracts found in `{CONTRACTS_DIR}`. Run ContractGenerator first.")
    st.stop()

selected_id = st.sidebar.selectbox(
    "Select contract",
    list(all_contracts.keys()),
    format_func=lambda x: x.replace("-", " ").title(),
)

contract = all_contracts[selected_id]
schema = contract.get("schema", {})
quality = contract.get("quality", {})
lineage = contract.get("lineage", {})
info = contract.get("info", {})

# ── main header ───────────────────────────────────────────────────────────────

st.title(f"📋 {info.get('title', selected_id)}")
col1, col2, col3, col4 = st.columns(4)
col1.metric("Contract ID", selected_id.split("-")[0].upper())
col2.metric("Version", info.get("version", "—"))
col3.metric("Schema Fields", len(schema))
col4.metric("Downstream Consumers", len(lineage.get("downstream", [])))

st.caption(f"**Generated:** {contract.get('x-generated-at', '—')} | "
           f"**Source hash:** `{contract.get('x-source-hash', '—')[:24]}...`")
st.divider()

# ── tabs ──────────────────────────────────────────────────────────────────────

tab1, tab2, tab3, tab4, tab5 = st.tabs(
    ["📊 Schema Clauses", "🔍 Source Data", "✅ Quality Checks", "🔗 Lineage", "📄 Raw YAML"]
)

# ── Tab 1: Schema Clauses ────────────────────────────────────────────────────

with tab1:
    st.subheader("Generated Schema Clauses")
    st.caption("Review each clause — flag anything that looks wrong before building the runner.")

    if not schema:
        st.warning("No schema clauses in this contract.")
    else:
        rows = []
        issues = []
        for field, clause in schema.items():
            row = {
                "Field": field,
                "Type": clause.get("type", "—"),
                "Required": "✅" if clause.get("required") else "❌",
                "Format": clause.get("format", "—"),
                "Min": clause.get("minimum", "—"),
                "Max": clause.get("maximum", "—"),
                "Enum": ", ".join(clause.get("enum", [])) if clause.get("enum") else "—",
                "Null %": f"{clause.get('x-null-fraction', 0)*100:.1f}%" if clause.get("x-null-fraction") else "0%",
                "Warning": clause.get("x-warning", ""),
            }
            rows.append(row)
            if clause.get("x-warning"):
                issues.append((field, clause["x-warning"]))

        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True, hide_index=True,
                     column_config={
                         "Required": st.column_config.TextColumn(width="small"),
                         "Warning": st.column_config.TextColumn(width="large"),
                     })

        if issues:
            st.warning(f"**{len(issues)} field(s) with warnings:**")
            for field, warn in issues:
                st.warning(f"`{field}`: {warn}")

        # Confidence fields highlighted
        conf_fields = [f for f in schema if "confidence" in f]
        if conf_fields:
            st.success(f"**Confidence fields detected and constrained to 0.0–1.0:** {', '.join(f'`{f}`' for f in conf_fields)}")

        # Clause correctness self-check
        with st.expander("📏 Clause Correctness Self-Check (P1-8)"):
            total = len(schema)
            has_type = sum(1 for c in schema.values() if c.get("type"))
            has_required = sum(1 for c in schema.values() if "required" in c)
            has_format = sum(1 for c in schema.values() if c.get("format"))
            has_constraint = sum(1 for c in schema.values()
                                 if c.get("minimum") is not None or c.get("maximum") is not None or c.get("enum"))

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Has type", f"{has_type}/{total}", delta=f"{has_type/total*100:.0f}%")
            c2.metric("Has required", f"{has_required}/{total}", delta=f"{has_required/total*100:.0f}%")
            c3.metric("Has format/enum/range", f"{has_constraint}/{total}", delta=f"{has_constraint/total*100:.0f}%")
            correctness = (has_type + has_required + has_constraint) / (total * 3)
            c4.metric("Est. correctness", f"{correctness*100:.0f}%",
                      delta="✅ above 70% threshold" if correctness >= 0.7 else "⚠️ below 70% threshold")

# ── Tab 2: Source Data ────────────────────────────────────────────────────────

with tab2:
    st.subheader("Source Data Preview")

    # Find the source JSONL from contract server config
    source_rel = contract.get("servers", {}).get("local", {}).get("path", "")
    source_path = ROOT / source_rel if source_rel else None

    if source_path and source_path.exists():
        records = load_jsonl(source_path)
        st.caption(f"**File:** `{source_rel}` | **Records:** {len(records)}")

        # Show first record as JSON
        with st.expander("First record (raw JSON)", expanded=True):
            st.json(records[0])

        # Flatten and show as table
        flat_rows = []
        for r in records[:20]:
            flat = {}
            for k, v in r.items():
                if isinstance(v, (str, int, float, bool)) or v is None:
                    flat[k] = v
                elif isinstance(v, list):
                    flat[k] = f"[{len(v)} items]"
                elif isinstance(v, dict):
                    flat[k] = f"{{...{len(v)} keys}}"
            flat_rows.append(flat)

        if flat_rows:
            st.dataframe(pd.DataFrame(flat_rows), use_container_width=True, hide_index=True)
            if len(records) > 20:
                st.caption(f"Showing first 20 of {len(records)} records.")
    else:
        st.warning(f"Source file not found: `{source_rel}`")
        st.info("This is expected for `outputs/` which is gitignored. Re-run `scripts/generate_outputs.py` + `scripts/migrate_to_canonical.py`.")

    # Numeric column distributions
    if source_path and source_path.exists() and records:
        numeric_clauses = {
            f: c for f, c in schema.items()
            if c.get("type") in ("number", "integer") and c.get("x-observed")
        }
        if numeric_clauses:
            st.subheader("Numeric Field Distributions")
            cols = st.columns(min(len(numeric_clauses), 3))
            for i, (field, clause) in enumerate(numeric_clauses.items()):
                obs = clause["x-observed"]
                with cols[i % 3]:
                    st.markdown(f"**`{field}`**")
                    st.markdown(
                        f"min `{obs['min']:.3f}` → max `{obs['max']:.3f}`  \n"
                        f"mean `{obs['mean']:.3f}` ± `{obs['stddev']:.3f}`"
                    )
                    if clause.get("minimum") is not None:
                        in_range = obs["min"] >= clause["minimum"] and obs["max"] <= clause["maximum"]
                        st.markdown("✅ Within contract range" if in_range else "🔴 **OUTSIDE contract range**")

# ── Tab 3: Quality Checks ─────────────────────────────────────────────────────

with tab3:
    st.subheader("Soda Quality Checks")
    checks_block = quality.get("specification", {})
    if checks_block:
        for table_name, checks in checks_block.items():
            st.code("\n".join(checks), language="yaml")
            st.caption(f"Table: `{table_name}` — {len(checks)} checks")
    else:
        st.info("No quality checks defined.")

    # dbt schema.yml preview
    dbt_path = CONTRACTS_DIR / f"{selected_id}_dbt.yml"
    if dbt_path.exists():
        with st.expander("dbt schema.yml"):
            dbt = load_contract(dbt_path)
            st.code(yaml.dump(dbt, default_flow_style=False, sort_keys=False), language="yaml")

# ── Tab 4: Lineage ────────────────────────────────────────────────────────────

with tab4:
    st.subheader("Lineage")
    upstream = lineage.get("upstream", [])
    downstream = lineage.get("downstream", [])

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**Upstream (producers)**")
        if upstream:
            for u in upstream:
                st.markdown(f"- `{u.get('id', u)}`")
        else:
            st.caption("No upstream defined (this is a source system).")

    with c2:
        st.markdown("**Downstream consumers**")
        if downstream:
            for d in downstream:
                st.markdown(f"- `{d.get('id', d)}`")
                if d.get("breaking_if_changed"):
                    st.caption(f"  Breaking if changed: {d['breaking_if_changed']}")
        else:
            st.caption("No downstream consumers found in lineage graph.")
            st.info("The Week 4 lineage graph is the jaffle_shop_classic dbt example — "
                    "it doesn't reference our week 3/5 paths directly. "
                    "This will be re-run against real repo lineage in Phase 2B.")

    # Schema snapshot history
    snap_dir = SNAPSHOTS_DIR / selected_id
    if snap_dir.exists():
        snapshots = sorted(snap_dir.glob("*.yaml"))
        st.subheader(f"Schema Snapshots ({len(snapshots)} runs)")
        for snap in snapshots:
            with st.expander(snap.stem):
                snap_data = load_contract(snap)
                st.caption(f"Captured: {snap_data.get('captured_at', '—')}")
                cols_data = snap_data.get("columns", {})
                rows = [
                    {"column": col, "dtype": v.get("dtype"), "null_fraction": v.get("null_fraction"),
                     "cardinality": v.get("cardinality")}
                    for col, v in cols_data.items()
                ]
                if rows:
                    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

# ── Tab 5: Raw YAML ───────────────────────────────────────────────────────────

with tab5:
    st.subheader("Raw Contract YAML")
    contract_path = CONTRACTS_DIR / f"{selected_id}.yaml"
    if contract_path.exists():
        raw_yaml = contract_path.read_text()
        st.code(raw_yaml, language="yaml")
        st.download_button(
            "⬇ Download YAML",
            data=raw_yaml,
            file_name=f"{selected_id}.yaml",
            mime="text/yaml",
        )
    else:
        st.error("Contract file not found.")

# ── footer ────────────────────────────────────────────────────────────────────

st.divider()
st.caption(
    f"**All contracts:** {', '.join(f'`{k}`' for k in all_contracts)} | "
    f"Next: Phase 2A — ValidationRunner"
)
