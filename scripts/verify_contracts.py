"""
verify_contracts.py — Data Contract Enforcer Dashboard
=======================================================
Streamlit app covering Phase 1 (contracts) and Phase 2 (validation + violations).

Run:  uv run streamlit run scripts/verify_contracts.py
"""

import json
from pathlib import Path

import pandas as pd
import streamlit as st
import yaml

ROOT = Path(__file__).parent.parent
CONTRACTS_DIR    = ROOT / "generated_contracts"
OUTPUTS_DIR      = ROOT / "outputs"
SNAPSHOTS_DIR    = ROOT / "schema_snapshots"
REPORTS_DIR      = ROOT / "validation_reports"
VIOLATIONS_FILE  = ROOT / "violation_log" / "violations.jsonl"

st.set_page_config(
    page_title="Data Contract Enforcer",
    page_icon="📋",
    layout="wide",
)

# ── helpers ───────────────────────────────────────────────────────────────────

@st.cache_data
def load_yaml(path: Path) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)

@st.cache_data
def load_jsonl(path: Path) -> list[dict]:
    with open(path) as f:
        return [json.loads(l) for l in f if l.strip()]

@st.cache_data
def load_json(path: Path) -> dict:
    return json.loads(path.read_text())

def all_contracts() -> dict[str, dict]:
    return {p.stem: load_yaml(p)
            for p in sorted(CONTRACTS_DIR.glob("*.yaml"))
            if "_dbt" not in p.stem}

def all_reports() -> list[dict]:
    reports = []
    for p in sorted(REPORTS_DIR.glob("*.json")):
        try:
            reports.append(load_json(p))
        except Exception:
            pass
    return reports

def all_violations() -> list[dict]:
    if not VIOLATIONS_FILE.exists():
        return []
    return load_jsonl(VIOLATIONS_FILE)

def status_icon(s: str) -> str:
    return {"PASS": "✅", "FAIL": "❌", "WARN": "⚠️", "ERROR": "🔴"}.get(s, "❓")

def severity_color(s: str) -> str:
    return {"CRITICAL": "🔴", "HIGH": "🟠", "MEDIUM": "🟡", "LOW": "🟢", "WARNING": "🟡"}.get(s, "⚪")

# ── top nav ───────────────────────────────────────────────────────────────────

st.title("📋 Data Contract Enforcer")
page = st.radio(
    "View",
    ["🏠 Overview", "📄 Contracts (P1)", "✅ Validation Reports (P2A)", "🔍 Violations (P2B)"],
    horizontal=True,
    label_visibility="collapsed",
)
st.divider()

# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: OVERVIEW
# ═══════════════════════════════════════════════════════════════════════════════

if page == "🏠 Overview":
    contracts = all_contracts()
    reports   = all_reports()
    violations = all_violations()

    # Dedupe reports: keep latest per contract (skip injected ones)
    latest: dict[str, dict] = {}
    for r in reports:
        cid = r["contract_id"]
        if r.get("injected_violation"):
            continue
        if cid not in latest or r["run_timestamp"] > latest[cid]["run_timestamp"]:
            latest[cid] = r

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Contracts generated", len(contracts))
    col2.metric("Validation runs", len(reports))
    total_fail = sum(r["failed"] for r in latest.values())
    col3.metric("Checks failing (latest)", total_fail, delta="real findings" if total_fail else None,
                delta_color="inverse")
    col4.metric("Violations attributed", len(violations))

    st.subheader("Contract Health Summary")
    rows = []
    for cid, r in sorted(latest.items()):
        icon = status_icon("PASS" if r["failed"] == 0 and r["errored"] == 0 else "FAIL")
        rows.append({
            "Status": icon,
            "Contract": cid,
            "Checks": r["total_checks"],
            "✅ Pass": r["passed"],
            "❌ Fail": r["failed"],
            "⚠️ Warn": r["warned"],
            "🔴 Error": r["errored"],
            "Run": r["run_timestamp"][:19].replace("T", " "),
        })
    if rows:
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    if violations:
        st.subheader("Recent Violations")
        vrows = []
        for v in violations[-10:]:
            vrows.append({
                "Severity": severity_color(v["severity"]) + " " + v["severity"],
                "Check": v["check_id"].split(".")[-2] + "." + v["check_id"].split(".")[-1],
                "Column": v["column_name"],
                "Contract": v["contract_id"],
                "Blast Radius": len(v["blast_radius"]["affected_nodes"]),
                "Records": v["failure_detail"]["records_failing"],
            })
        st.dataframe(pd.DataFrame(vrows), use_container_width=True, hide_index=True)

# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: CONTRACTS (Phase 1)
# ═══════════════════════════════════════════════════════════════════════════════

elif page == "📄 Contracts (P1)":
    contracts = all_contracts()
    if not contracts:
        st.error("No contracts found. Run ContractGenerator first.")
        st.stop()

    selected_id = st.selectbox(
        "Contract",
        list(contracts.keys()),
        format_func=lambda x: x.replace("-", " ").title(),
    )
    contract = contracts[selected_id]
    schema   = contract.get("schema", {})
    quality  = contract.get("quality", {})
    lineage  = contract.get("lineage", {})
    info     = contract.get("info", {})

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Fields", len(schema))
    c2.metric("Version", info.get("version", "—"))
    c3.metric("Downstream", len(lineage.get("downstream", [])))
    c4.metric("Owner", info.get("owner", "—"))
    st.caption(f"Generated: `{contract.get('x-generated-at', '—')}` | "
               f"Source hash: `{contract.get('x-source-hash', '—')[:20]}...`")

    tab1, tab2, tab3, tab4 = st.tabs(["📊 Schema Clauses", "✅ Quality Checks", "🔗 Lineage", "📄 Raw YAML"])

    with tab1:
        rows, issues = [], []
        for field, clause in schema.items():
            row = {
                "Field": field,
                "Type": clause.get("type", "—"),
                "Req": "✅" if clause.get("required") else "—",
                "Format": clause.get("format", "—"),
                "Min": clause.get("minimum", "—"),
                "Max": clause.get("maximum", "—"),
                "Enum": "Yes (" + str(clause.get("cardinality", len(clause.get("enum", [])))) + ")" if clause.get("enum") else "—",
                "Null%": f"{clause.get('x-null-fraction', 0)*100:.0f}%" if clause.get("x-null-fraction") else "0%",
                "⚠": "⚠️" if clause.get("x-warning") else "",
            }
            rows.append(row)
            if clause.get("x-warning"):
                issues.append((field, clause["x-warning"]))

        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

        if issues:
            for field, w in issues:
                st.warning(f"`{field}`: {w}")

        conf = [f for f in schema if "confidence" in f]
        if conf:
            st.success(f"Confidence fields constrained to 0.0–1.0: {', '.join(f'`{f}`' for f in conf)}")

        # Correctness metric
        total = len(schema)
        if total:
            score = sum(1 for c in schema.values()
                        if c.get("minimum") is not None or c.get("maximum") is not None
                        or c.get("enum") or c.get("format") or c.get("required")) / total
            st.metric("Clause richness", f"{score*100:.0f}%",
                      delta="✅ ≥70%" if score >= 0.7 else "⚠️ <70%")

    with tab2:
        checks_block = quality.get("specification", {})
        for tbl, checks in checks_block.items():
            st.code("\n".join(checks), language="yaml")
            st.caption(f"{len(checks)} checks for `{tbl}`")

        dbt_path = CONTRACTS_DIR / f"{selected_id}_dbt.yml"
        if dbt_path.exists():
            with st.expander("dbt schema.yml"):
                st.code(dbt_path.read_text(), language="yaml")

    with tab3:
        up   = lineage.get("upstream", [])
        down = lineage.get("downstream", [])
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**Upstream**")
            [st.markdown(f"- `{u.get('id', u)}`") for u in up] if up else st.caption("Source system")
        with c2:
            st.markdown("**Downstream**")
            if down:
                for d in down:
                    st.markdown(f"- `{d.get('id', d)}`")
            else:
                st.caption("None found in lineage graph")
                st.info("Week 4 lineage is jaffle_shop_classic — doesn't reference our paths directly.")

        snap_dir = SNAPSHOTS_DIR / selected_id
        if snap_dir.exists():
            snaps = sorted(snap_dir.glob("*.yaml"))
            st.subheader(f"Schema Snapshots ({len(snaps)})")
            for s in snaps:
                with st.expander(s.stem):
                    d = load_yaml(s)
                    cols = d.get("columns", {})
                    st.dataframe(pd.DataFrame([
                        {"column": c, "dtype": v.get("dtype"), "nulls": v.get("null_fraction"),
                         "cardinality": v.get("cardinality")}
                        for c, v in cols.items()
                    ]), use_container_width=True, hide_index=True)

    with tab4:
        path = CONTRACTS_DIR / f"{selected_id}.yaml"
        st.code(path.read_text(), language="yaml")
        st.download_button("⬇ Download YAML", path.read_text(),
                           file_name=f"{selected_id}.yaml", mime="text/yaml")

# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: VALIDATION REPORTS (Phase 2A)
# ═══════════════════════════════════════════════════════════════════════════════

elif page == "✅ Validation Reports (P2A)":
    reports = all_reports()
    if not reports:
        st.warning("No validation reports found. Run `contracts/runner.py` first.")
        st.stop()

    # Build report index
    index_rows = []
    for r in reports:
        injected = f" [{r['injected_violation']}]" if r.get("injected_violation") else ""
        overall = "PASS" if r["failed"] == 0 and r["errored"] == 0 else "FAIL"
        index_rows.append({
            "_path": r.get("data_path", ""),
            "Status": status_icon(overall) + " " + overall + injected,
            "Contract": r["contract_id"],
            "Checks": r["total_checks"],
            "✅": r["passed"],
            "❌": r["failed"],
            "⚠️": r["warned"],
            "🔴": r["errored"],
            "Run": r["run_timestamp"][:19].replace("T", " "),
        })

    st.subheader("All Runs")
    df_index = pd.DataFrame(index_rows).drop(columns=["_path"])
    st.dataframe(df_index, use_container_width=True, hide_index=True)

    # Detail view
    report_labels = [
        f"{r['contract_id']} — {r['run_timestamp'][:19]}"
        + (f" [INJECTED: {r['injected_violation']}]" if r.get("injected_violation") else "")
        for r in reports
    ]
    selected_idx = st.selectbox("Inspect report", range(len(reports)),
                                 format_func=lambda i: report_labels[i])
    report = reports[selected_idx]

    c1, c2, c3, c4 = st.columns(4)
    overall = "PASS" if report["failed"] == 0 and report["errored"] == 0 else "FAIL"
    c1.metric("Overall", status_icon(overall) + " " + overall)
    c2.metric("Total checks", report["total_checks"])
    c3.metric("Failed", report["failed"])
    c4.metric("Warned", report["warned"])

    if report.get("injected_violation"):
        st.info(f"🧪 Injected violation: **{report['injected_violation']}** — this is a test run.")

    results = report.get("results", [])

    tab1, tab2 = st.tabs(["All Checks", "Failures Only"])

    with tab1:
        rows = []
        for r in results:
            rows.append({
                "Status": status_icon(r["status"]),
                "Severity": severity_color(r["severity"]) + " " + r["severity"],
                "Check": r["check_id"].split(".")[-2] + "." + r["check_id"].split(".")[-1],
                "Column": r["column_name"],
                "Type": r["check_type"],
                "Actual": r["actual_value"][:60],
                "Expected": r["expected"][:60],
            })
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    with tab2:
        failures = [r for r in results if r["status"] in ("FAIL", "ERROR")]
        if not failures:
            st.success("No failures in this report.")
        else:
            for f in failures:
                with st.expander(
                    f"{severity_color(f['severity'])} [{f['severity']}] {f['check_id']}",
                    expanded=True
                ):
                    c1, c2 = st.columns(2)
                    c1.markdown(f"**Column:** `{f['column_name']}`")
                    c1.markdown(f"**Check type:** `{f['check_type']}`")
                    c1.markdown(f"**Records failing:** `{f['records_failing']}`")
                    c2.markdown(f"**Actual:** `{f['actual_value']}`")
                    c2.markdown(f"**Expected:** `{f['expected']}`")
                    st.error(f["message"])
                    if f.get("sample_failing"):
                        st.caption(f"Sample: {f['sample_failing'][:3]}")

# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: VIOLATIONS (Phase 2B)
# ═══════════════════════════════════════════════════════════════════════════════

elif page == "🔍 Violations (P2B)":
    violations = all_violations()
    if not violations:
        st.warning("No violations found. Run `contracts/attributor.py` first.")
        st.stop()

    st.subheader(f"{len(violations)} Attributed Violations")

    # Summary table
    rows = []
    for v in violations:
        top_blame = v["blame_chain"][0] if v["blame_chain"] else {}
        rows.append({
            "Severity": severity_color(v["severity"]) + " " + v["severity"],
            "Contract": v["contract_id"].replace("-", " "),
            "Column": v["column_name"],
            "Check": v["check_id"].split(".")[-1],
            "Records": v["failure_detail"]["records_failing"],
            "Blast Radius": len(v["blast_radius"]["affected_nodes"]),
            "Top Blame": top_blame.get("commit_message", "—")[:50],
            "Confidence": top_blame.get("confidence_score", "—"),
        })
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    # Filter
    contracts_with_violations = sorted(set(v["contract_id"] for v in violations))
    filter_contract = st.selectbox("Filter by contract", ["All"] + contracts_with_violations)
    filtered = violations if filter_contract == "All" else [
        v for v in violations if v["contract_id"] == filter_contract
    ]

    st.subheader("Violation Detail")
    for v in filtered:
        severity_icon = severity_color(v["severity"])
        with st.expander(
            f"{severity_icon} [{v['severity']}] {v['check_id']}",
            expanded=(v["severity"] == "CRITICAL")
        ):
            col1, col2 = st.columns(2)

            with col1:
                st.markdown("**Failure**")
                st.markdown(f"- Column: `{v['column_name']}`")
                st.markdown(f"- Actual: `{v['failure_detail']['actual_value'][:80]}`")
                st.markdown(f"- Expected: `{v['failure_detail']['expected'][:80]}`")
                st.markdown(f"- Records failing: `{v['failure_detail']['records_failing']}`")
                st.error(v["failure_detail"]["message"])

            with col2:
                st.markdown("**Blast Radius**")
                blast = v["blast_radius"]
                st.metric("Affected nodes", len(blast["affected_nodes"]))
                st.metric("Affected pipelines", len(blast["affected_pipelines"]))
                st.metric("Estimated records", blast["estimated_records"])
                if blast["affected_nodes"]:
                    st.caption("Nodes: " + ", ".join(f"`{n}`" for n in blast["affected_nodes"][:4]))

            st.markdown("**Blame Chain**")
            if v["blame_chain"]:
                blame_rows = []
                for b in v["blame_chain"]:
                    blame_rows.append({
                        "Rank": b["rank"],
                        "Commit": b["commit_hash"][:8],
                        "Author": b["author"],
                        "Message": b["commit_message"][:60],
                        "File": b["file_path"],
                        "Confidence": b["confidence_score"],
                    })
                st.dataframe(pd.DataFrame(blame_rows), use_container_width=True, hide_index=True)
            else:
                st.caption("No blame chain available.")

    # Blast radius summary
    st.subheader("Blast Radius Summary")
    node_counts = {}
    for v in violations:
        for node in v["blast_radius"]["affected_nodes"]:
            node_counts[node] = node_counts.get(node, 0) + 1
    if node_counts:
        df_blast = pd.DataFrame([
            {"Node": k, "Violation Count": c}
            for k, c in sorted(node_counts.items(), key=lambda x: -x[1])
        ])
        st.bar_chart(df_blast.set_index("Node"))

st.divider()
st.caption("Data Contract Enforcer · Week 7 · Phases 1–2 complete · Phase 3 next: SchemaEvolutionAnalyzer")
