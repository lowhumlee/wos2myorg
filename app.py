"""
app.py — WoS MUV Affiliation Ingestion Tool · Streamlit GUI
Medical University of Varna · Research Information Systems

Run with:
  streamlit run app.py
"""

from __future__ import annotations

import csv
import io
import json
import os
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import streamlit as st

# Make sure core.py is importable from same directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core import (
    DEFAULT_CONFIG,
    normalize_name, name_similarity,
    build_person_index, parse_org_hierarchy, parse_wos_csv,
    extract_muv_author_pairs, match_person, batch_process,
    build_upload_csv, build_audit_json, build_review_excel,
    StagingDB,
)

try:
    import openpyxl
    HAS_OPENPYXL = True
except ImportError:
    HAS_OPENPYXL = False

# ─── Page & Theme Setup ───────────────────────────────────────────────────────

st.set_page_config(
    page_title="WoS MUV Ingestion Tool",
    page_icon="🔬",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
/* ── Main header ── */
.muv-header {
    background: linear-gradient(135deg, #0d2d4e 0%, #1a5276 60%, #2980b9 100%);
    border-radius: 14px;
    padding: 1.6rem 2.2rem 1.4rem;
    margin-bottom: 1.6rem;
    box-shadow: 0 4px 20px rgba(0,0,0,0.18);
}
.muv-header h1 { color: #ffffff; margin: 0; font-size: 1.75rem; letter-spacing: -0.02em; }
.muv-header .sub { color: #a8d4f5; margin: 0.35rem 0 0; font-size: 0.92rem; }

/* ── Metric cards ── */
.metric-grid { display: flex; gap: 1rem; margin: 1rem 0; flex-wrap: wrap; }
.metric-card {
    flex: 1 1 140px;
    background: #fff;
    border: 1px solid #d0dde8;
    border-radius: 12px;
    padding: 1.1rem 1.2rem;
    text-align: center;
    box-shadow: 0 2px 8px rgba(0,0,0,0.06);
}
.metric-card .num { font-size: 2.4rem; font-weight: 800; line-height: 1; }
.metric-card .lbl { font-size: 0.78rem; color: #6b7a8d; margin-top: 0.3rem; font-weight: 500; text-transform: uppercase; letter-spacing: 0.04em; }
.num-blue { color: #1a5276; }
.num-green { color: #1e8449; }
.num-orange { color: #d35400; }
.num-yellow { color: #9a7d0a; }

/* ── Badges ── */
.badge {
    display: inline-block;
    padding: 3px 10px;
    border-radius: 20px;
    font-size: 0.78rem;
    font-weight: 700;
    letter-spacing: 0.03em;
}
.badge-new    { background: #d5f5e3; color: #1e8449; }
.badge-exact  { background: #d6eaf8; color: #1a5276; }
.badge-fuzzy  { background: #fef9e7; color: #9a7d0a; border: 1px solid #f9e79f; }

/* ── Section headers ── */
.sec-head {
    background: #eaf2fb;
    border-left: 5px solid #1a5276;
    padding: 0.55rem 1rem;
    border-radius: 0 8px 8px 0;
    margin: 1.2rem 0 0.6rem;
    font-weight: 700;
    color: #1a3a5c;
    font-size: 1rem;
}

/* ── Affiliation chips ── */
.chip {
    display: inline-block;
    background: #eaf2fb;
    border: 1px solid #aed6f1;
    color: #1a5276;
    padding: 3px 10px;
    border-radius: 16px;
    font-size: 0.8rem;
    margin: 2px;
}

/* ── Upload zone ── */
div[data-testid="stFileUploader"] {
    border: 2px dashed #aed6f1;
    border-radius: 10px;
    padding: 0.4rem;
}

/* ── Buttons ── */
div.stButton > button {
    border-radius: 8px;
    font-weight: 600;
}
div.stButton > button[kind="primary"] {
    background: linear-gradient(135deg, #1a5276, #2980b9);
    border: none;
    color: white;
}

/* ── Expander ── */
div[data-testid="stExpander"] {
    border: 1px solid #d0dde8 !important;
    border-radius: 10px !important;
    margin-bottom: 0.5rem;
}

/* ── Download button ── */
div.stDownloadButton > button {
    border-radius: 8px;
    font-weight: 600;
    background: #1e8449;
    color: white;
    border: none;
}
</style>
""", unsafe_allow_html=True)

# ─── Session State Init ───────────────────────────────────────────────────────

_DEFAULTS = {
    "cfg": DEFAULT_CONFIG.copy(),
    "person_index": {},
    "max_pid": 0,
    "orgs": [],
    "wos_records": [],
    "muv_pairs": [],
    "batch_result": None,        # dict from batch_process()
    "decisions": {},             # norm -> decision dict (for interactive review)
    "output_rows": [],           # finalized rows
    "rejected_rows": [],
    "processed": False,
    "finalized": False,
    "source_file": "unknown.csv",
}

for k, v in _DEFAULTS.items():
    if k not in st.session_state:
        st.session_state[k] = v


def reset_state():
    for k, v in _DEFAULTS.items():
        st.session_state[k] = v if not callable(v) else v()


# ─── Sidebar ─────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("### ⚙️ Configuration")

    cfg = st.session_state.cfg
    cfg["fuzzy_threshold"] = st.slider(
        "Fuzzy match threshold", 0.5, 1.0,
        float(cfg.get("fuzzy_threshold", 0.85)), 0.01,
        help="Minimum name similarity to flag as possible match (0 = everything, 1 = exact only)"
    )
    cfg["allow_multi_org"] = st.checkbox(
        "Allow multiple organizations per author",
        value=bool(cfg.get("allow_multi_org", True))
    )
    cfg["new_person_id_start"] = st.number_input(
        "New PersonID starts at", min_value=1000, max_value=999999,
        value=int(cfg.get("new_person_id_start", 9000)), step=1
    )

    st.markdown("---")
    st.markdown("### 🔍 MUV Affiliation Patterns")
    pat_text = st.text_area(
        "Patterns (one per line, case-insensitive)",
        value="\n".join(cfg.get("muv_affiliation_patterns", [])),
        height=160,
        help="Substrings that identify a MUV affiliation in WoS C1 field"
    )
    cfg["muv_affiliation_patterns"] = [p.strip() for p in pat_text.splitlines() if p.strip()]

    st.markdown("---")
    if st.button("🔄 Reset All", width='stretch'):
        reset_state()
        st.rerun()

    st.markdown("---")
    st.caption("WoS MUV Ingestion Tool · v2.0")
    st.caption("Medical University of Varna")

# ─── Header ──────────────────────────────────────────────────────────────────

st.markdown("""
<div class="muv-header">
  <h1>🔬 WoS My Organization — Affiliation Ingestion Tool</h1>
  <div class="sub">Medical University of Varna · Bibliometric Data Curation Workflow</div>
</div>
""", unsafe_allow_html=True)

# ─── Tabs ─────────────────────────────────────────────────────────────────────

tab_load, tab_review, tab_output, tab_stats, tab_help = st.tabs([
    "📂 1 · Load Files",
    "🔎 2 · Review & Resolve",
    "📤 3 · Export Output",
    "📊 4 · Statistics",
    "❓ Help",
])

# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — LOAD FILES
# ══════════════════════════════════════════════════════════════════════════════

with tab_load:
    st.markdown('<div class="sec-head">Upload Input Files</div>', unsafe_allow_html=True)

    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("**📋 WoS Export (new records)**")
        wos_file = st.file_uploader("WoS CSV (input*.csv)", type=["csv", "txt"], key="wos_up")
        if wos_file:
            st.success(f"✓ {wos_file.name}")
            st.session_state.source_file = wos_file.name

    with col2:
        st.markdown("**👥 Existing Researchers**")
        res_file = st.file_uploader("ResearcherAndDocument.csv", type=["csv"], key="res_up")
        if res_file:
            st.success(f"✓ {res_file.name}")

    with col3:
        st.markdown("**🏛️ Organization Hierarchy**")
        org_file = st.file_uploader("OrganizationHierarchy.csv", type=["csv"], key="org_up")
        if org_file:
            st.success(f"✓ {org_file.name}")

    st.markdown("---")

    # Previews
    if res_file:
        res_file.seek(0)
        df_preview = list(csv.DictReader(io.StringIO(res_file.read().decode("utf-8-sig"))))
        res_file.seek(0)
        import pandas as pd
        with st.expander(f"Preview: Researchers ({len(df_preview)} rows)", expanded=False):
            st.dataframe(pd.DataFrame(df_preview).head(15), width='stretch')

    if org_file:
        org_file.seek(0)
        df_org_preview = list(csv.DictReader(io.StringIO(org_file.read().decode("utf-8-sig"))))
        org_file.seek(0)
        with st.expander(f"Preview: Organizations ({len(df_org_preview)} orgs)", expanded=False):
            st.dataframe(pd.DataFrame(df_org_preview), width='stretch')

    if wos_file:
        wos_file.seek(0)
        wos_preview = list(csv.DictReader(io.StringIO(wos_file.read().decode("utf-8-sig"))))
        wos_file.seek(0)
        with st.expander(f"Preview: WoS Records ({len(wos_preview)} records)", expanded=False):
            if wos_preview:
                df_wos_prev = pd.DataFrame(wos_preview)
                # Strip whitespace and drop None columns (WoS trailing delimiter bug)
                df_wos_prev.columns = [
                    c.strip() if c is not None else "__EMPTY__"
                    for c in df_wos_prev.columns
                ]
                df_wos_prev = df_wos_prev[[c for c in df_wos_prev.columns if c != "__EMPTY__"]]
                # Show UT + AF if present, otherwise show all columns
                preferred = [c for c in ["UT", "AF"] if c in df_wos_prev.columns]
                st.caption(f"Detected columns: {list(df_wos_prev.columns)}")
                if preferred:
                    st.dataframe(df_wos_prev[preferred].head(10), width='stretch')
                else:
                    st.dataframe(df_wos_prev.head(10), width='stretch')
            else:
                st.warning("No rows found in WoS file.")

    st.markdown("---")

    proc_btn = st.button(
        "🚀  Detect MUV Authors",
        type="primary",
        disabled=not (wos_file and res_file and org_file),
        width='stretch',
    )

    if proc_btn:
        cfg = st.session_state.cfg

        with st.spinner("Parsing files and detecting MUV-affiliated authors…"):
            # Read file contents
            res_file.seek(0)
            res_content = res_file.read().decode("utf-8-sig")
            org_file.seek(0)
            org_content = org_file.read().decode("utf-8-sig")
            wos_file.seek(0)
            wos_content = wos_file.read().decode("utf-8-sig")

            person_index, max_pid = build_person_index(res_content)
            orgs = parse_org_hierarchy(org_content)
            records = parse_wos_csv(wos_content)
            muv_pairs = extract_muv_author_pairs(records, cfg)

            start_pid = max(int(cfg["new_person_id_start"]), max_pid + 1)
            batch_result = batch_process(muv_pairs, person_index, orgs, cfg, start_pid)

            # Build decisions dict for interactive review
            decisions = {}
            for norm, pairs in defaultdict(list, {
                normalize_name(p["author_full"]): [] for p in muv_pairs
            }).items():
                pass  # rebuilt below

            decisions_by_norm: dict[str, dict] = {}
            for item in batch_result["needs_review"]:
                norm = item["norm"]
                if norm not in decisions_by_norm:
                    decisions_by_norm[norm] = {
                        **item,
                        "org_ids": [item.get("OrganizationID", "")],
                        "resolved_pid": item.get("suggested_pid", ""),
                        "resolved_name": item.get("suggested_name", item.get("AuthorFullName", "")),
                        "approved": True,
                    }

            st.session_state.person_index = person_index
            st.session_state.max_pid = max_pid
            st.session_state.orgs = orgs
            st.session_state.wos_records = records
            st.session_state.muv_pairs = muv_pairs
            st.session_state.batch_result = batch_result
            st.session_state.decisions = decisions_by_norm
            st.session_state.processed = True
            st.session_state.finalized = False
            st.session_state.output_rows = []

        # ── Summary metrics
        confirmed = batch_result["confirmed"]
        review = batch_result["needs_review"]
        new_p = batch_result["new_persons"]

        n_exact = len([r for r in confirmed if r.get("match_type") == "exact"])
        n_new = len([r for r in review if r.get("match_type") == "new"])
        n_fuzzy = len([r for r in review if r.get("match_type") == "fuzzy"])

        st.markdown(f"""
<div class="metric-grid">
  <div class="metric-card"><div class="num num-blue">{len(muv_pairs)}</div><div class="lbl">MUV Pairs Found</div></div>
  <div class="metric-card"><div class="num num-blue">{n_exact}</div><div class="lbl">Auto-Confirmed (exact)</div></div>
  <div class="metric-card"><div class="num num-green">{len(new_p)}</div><div class="lbl">New Persons Staged</div></div>
  <div class="metric-card"><div class="num num-yellow">{n_fuzzy}</div><div class="lbl">Fuzzy / Ambiguous</div></div>
  <div class="metric-card"><div class="num num-orange">{len(review)}</div><div class="lbl">Needs Review</div></div>
</div>
""", unsafe_allow_html=True)

        if len(review) > 0:
            st.info(f"➡️ **{len(review)} entries need your decision.** Go to Tab 2 to review and assign organizations.")
        else:
            st.success("✅ All authors matched automatically. Go to Tab 3 to export.")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — REVIEW & RESOLVE
# ══════════════════════════════════════════════════════════════════════════════

with tab_review:
    if not st.session_state.processed:
        st.info("⬅️ Please load and process data in **Tab 1** first.")
    else:
        batch_result = st.session_state.batch_result
        decisions = st.session_state.decisions
        orgs = st.session_state.orgs
        cfg = st.session_state.cfg

        confirmed_auto = batch_result["confirmed"]
        needs_review = batch_result["needs_review"]

        # ── Filters
        fcol1, fcol2, fcol3 = st.columns([2, 2, 1])
        with fcol1:
            ftype = st.selectbox("Filter", [
                "All needing review",
                "New persons only",
                "Fuzzy matches only",
            ])
        with fcol2:
            fsearch = st.text_input("Search author name", "")
        with fcol3:
            st.markdown("<br>", unsafe_allow_html=True)
            show_exact = st.checkbox("Show auto-confirmed", value=False)

        # ── Org dropdown options
        org_map = {f"[{o['OrganizationID']}] {o['OrganizationName']}": o["OrganizationID"] for o in orgs}
        org_labels = ["— none / skip —"] + list(org_map.keys())

        def label_for_org(oid: str) -> str:
            for lbl, v in org_map.items():
                if v == oid:
                    return lbl
            return org_labels[0]

        # ── Show auto-confirmed section
        if show_exact and confirmed_auto:
            st.markdown('<div class="sec-head">✅ Auto-Confirmed (Exact Matches)</div>', unsafe_allow_html=True)
            import pandas as pd
            df_conf = pd.DataFrame([{
                "PersonID": r["PersonID"],
                "Name": r["AuthorFullName"],
                "UT": r["UT"],
                "OrgID": r["OrganizationID"],
            } for r in confirmed_auto])
            st.dataframe(df_conf, width='stretch', height=200)

        # ── Needs review section
        st.markdown(f'<div class="sec-head">🔍 Needs Human Decision ({len(needs_review)} entries)</div>', unsafe_allow_html=True)

        # Group by author (norm)
        by_norm: dict[str, list] = defaultdict(list)
        for item in needs_review:
            norm = item["norm"]
            if ftype == "New persons only" and item["match_type"] != "new":
                continue
            if ftype == "Fuzzy matches only" and item["match_type"] != "fuzzy":
                continue
            if fsearch and fsearch.lower() not in item["AuthorFullName"].lower():
                continue
            by_norm[norm].append(item)

        if not by_norm:
            st.success("✅ No entries match the current filter.")
        else:
            for norm, items in by_norm.items():
                first = items[0]
                mt = first["match_type"]
                author = first["AuthorFullName"]

                badge = {
                    "new":   '<span class="badge badge-new">🆕 NEW PERSON</span>',
                    "fuzzy": '<span class="badge badge-fuzzy">⚠ AMBIGUOUS MATCH</span>',
                }.get(mt, "")

                uts_str = ", ".join(i["UT"] for i in items)
                label = f"{author}  —  {len(items)} document(s)"

                with st.expander(label, expanded=(mt == "fuzzy")):
                    st.markdown(f"{badge}", unsafe_allow_html=True)

                    # MUV affiliation chips
                    all_muv = []
                    for it in items:
                        all_muv.extend(it["muv_affils"])
                    unique_muv = list(dict.fromkeys(all_muv))
                    chips_html = " ".join(f'<span class="chip">{a}</span>' for a in unique_muv[:4])
                    st.markdown(f"<small><b>MUV affiliations:</b> {chips_html}</small>",
                                unsafe_allow_html=True)

                    st.markdown(f"<small><b>Documents:</b> {uts_str}</small>", unsafe_allow_html=True)

                    # ── Identity decision
                    dec = decisions.get(norm, {
                        "resolved_pid": first.get("suggested_pid", ""),
                        "resolved_name": first.get("AuthorFullName", ""),
                        "org_ids": [""],
                        "approved": True,
                    })

                    id_col1, id_col2 = st.columns(2)

                    with id_col1:
                        if mt == "fuzzy" and first.get("candidates"):
                            cands = first["candidates"]
                            cand_labels = [f"[{p['PersonID']}] {p['AuthorFullName']} ({s:.2f})" for _, p, s in cands]
                            cand_labels.append("➕ Create as NEW PERSON")
                            choice = st.selectbox(
                                f"Identity for {author}",
                                cand_labels,
                                key=f"identity_{norm}",
                            )
                            if "NEW PERSON" in choice:
                                dec["resolved_pid"] = first.get("suggested_pid", "")
                                dec["resolved_name"] = author
                                dec["match_type"] = "new"
                            else:
                                idx = cand_labels.index(choice)
                                _, chosen_person, _ = cands[idx]
                                dec["resolved_pid"] = chosen_person["PersonID"]
                                dec["resolved_name"] = chosen_person["AuthorFullName"]
                                dec["match_type"] = "fuzzy_resolved"
                        else:
                            dec["resolved_pid"] = st.text_input(
                                "PersonID",
                                value=dec.get("resolved_pid", first.get("suggested_pid", "")),
                                key=f"pid_{norm}",
                            )
                            dec["resolved_name"] = st.text_input(
                                "Author Full Name",
                                value=dec.get("resolved_name", author),
                                key=f"name_{norm}",
                            )

                    with id_col2:
                        # ── Organization assignment
                        if cfg.get("allow_multi_org", True):
                            selected_labels = st.multiselect(
                                "Assign organization(s)",
                                options=list(org_map.keys()),
                                default=[label_for_org(oid) for oid in dec.get("org_ids", [""])
                                         if oid and label_for_org(oid) != org_labels[0]],
                                key=f"orgs_{norm}",
                            )
                            dec["org_ids"] = [org_map[lbl] for lbl in selected_labels] or [""]
                        else:
                            sel = st.selectbox(
                                "Assign organization",
                                options=org_labels,
                                key=f"org_{norm}",
                            )
                            dec["org_ids"] = [org_map[sel]] if sel in org_map else [""]

                    # ── Approve toggle
                    dec["approved"] = st.checkbox("✅ Approve this entry", value=dec.get("approved", True),
                                                  key=f"approve_{norm}")

                    decisions[norm] = dec

        st.session_state.decisions = decisions

        st.markdown("---")

        if st.button("💾  Save Decisions & Prepare Output", type="primary", width='stretch'):
            # Merge auto-confirmed + user decisions
            output_rows = []
            rejected_rows = []
            seen: set[tuple] = set()

            # Auto-confirmed (exact)
            for row in confirmed_auto:
                key = (row["PersonID"], row["UT"], row["OrganizationID"])
                if key not in seen:
                    seen.add(key)
                    output_rows.append(row)

            # User decisions
            needs_by_norm: dict[str, list] = defaultdict(list)
            for item in needs_review:
                needs_by_norm[item["norm"]].append(item)

            for norm, items in needs_by_norm.items():
                dec = decisions.get(norm)
                if not dec or not dec.get("approved", True):
                    for it in items:
                        rejected_rows.append({"AuthorFullName": it["AuthorFullName"],
                                              "UT": it["UT"], "Reason": "User rejected"})
                    continue

                pid = dec.get("resolved_pid", "")
                resolved_name = dec.get("resolved_name", items[0]["AuthorFullName"])
                org_ids = dec.get("org_ids", [""])

                for item in items:
                    for oid in org_ids:
                        key = (pid, item["UT"], oid)
                        if key in seen:
                            rejected_rows.append({"AuthorFullName": resolved_name,
                                                  "UT": item["UT"], "Reason": "Duplicate"})
                            continue
                        seen.add(key)
                        output_rows.append({
                            "PersonID": pid,
                            "AuthorFullName": resolved_name,
                            "UT": item["UT"],
                            "OrganizationID": oid,
                            "match_type": dec.get("match_type", ""),
                        })

            st.session_state.output_rows = output_rows
            st.session_state.rejected_rows = rejected_rows
            st.session_state.finalized = True
            st.success(f"✅ {len(output_rows)} rows finalized ({len(rejected_rows)} rejected). Go to **Tab 3** to export.")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — EXPORT OUTPUT
# ══════════════════════════════════════════════════════════════════════════════

with tab_output:
    if not st.session_state.processed:
        st.info("⬅️ Please complete Tabs 1 and 2 first.")
    elif not st.session_state.finalized:
        st.warning("⚠️ Please save decisions in **Tab 2** before exporting.")
    else:
        output_rows = st.session_state.output_rows
        rejected_rows = st.session_state.rejected_rows
        source_file = st.session_state.source_file
        orgs = st.session_state.orgs
        batch_result = st.session_state.batch_result

        st.markdown('<div class="sec-head">📤 Export Files</div>', unsafe_allow_html=True)

        # ── Metrics
        st.markdown(f"""
<div class="metric-grid">
  <div class="metric-card"><div class="num num-green">{len(output_rows)}</div><div class="lbl">Upload-Ready Rows</div></div>
  <div class="metric-card"><div class="num num-orange">{len(rejected_rows)}</div><div class="lbl">Rejected / Skipped</div></div>
</div>
""", unsafe_allow_html=True)

        # ── Upload-ready CSV
        st.markdown("#### 1. Upload-Ready CSV")
        st.markdown("Compatible with WoS My Organization bulk import format.")

        csv_bytes = build_upload_csv(output_rows, source_file).encode("utf-8")
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")

        st.download_button(
            label="⬇️  Download Upload-Ready CSV",
            data=csv_bytes,
            file_name=f"upload_ready_{ts}.csv",
            mime="text/csv",
            width='stretch',
        )

        with st.expander("Preview upload CSV", expanded=False):
            import pandas as pd
            st.dataframe(pd.DataFrame(output_rows).head(30), width='stretch')

        st.markdown("---")

        # ── Review Excel (for batch workflows)
        needs_review = batch_result.get("needs_review", [])
        if needs_review:
            st.markdown("#### 2. Review Excel (for batch workflows)")
            st.markdown("Share with library staff to fill in decisions offline, then re-import.")
            if HAS_OPENPYXL:
                excel_bytes = build_review_excel(needs_review, orgs)
                st.download_button(
                    label="⬇️  Download Review Excel",
                    data=excel_bytes,
                    file_name=f"review_{ts}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    width='stretch',
                )
            else:
                st.warning("openpyxl not installed — Excel export unavailable.")

            st.markdown("---")

        # ── Audit log
        st.markdown("#### 3. Audit Log (JSON)")
        new_persons_list = [
            {"PersonID": v["PersonID"], "AuthorFullName": v["AuthorFullName"]}
            for v in batch_result.get("new_persons", {}).values()
        ]
        audit_json = build_audit_json(
            batch_result.get("confirmed", []),
            output_rows,
            rejected_rows,
            new_persons_list,
        )
        st.download_button(
            label="⬇️  Download Audit Log (JSON)",
            data=audit_json.encode("utf-8"),
            file_name=f"audit_{ts}.json",
            mime="application/json",
            width='stretch',
        )

        with st.expander("Preview audit log", expanded=False):
            st.json(json.loads(audit_json))

        st.markdown("---")

        # ── Re-import filled Excel
        st.markdown("#### 4. Re-import Filled Review Excel")
        st.markdown("After library staff have filled in the review Excel, upload it here to merge.")

        reimport_file = st.file_uploader("Upload filled review Excel", type=["xlsx"], key="reimport")
        if reimport_file and st.button("🔄 Merge Review Decisions"):
            wb = __import__("openpyxl").load_workbook(reimport_file)
            ws = wb["Review Candidates"]
            headers = [c.value for c in ws[1]]

            def col(n): return headers.index(n)

            extra_rows = []
            skip_count = 0
            for row in ws.iter_rows(min_row=2, values_only=True):
                approved = row[col("APPROVED")]
                if approved and str(approved).strip().upper() == "YES":
                    extra_rows.append({
                        "PersonID": str(row[col("PersonID")] or row[col("SuggestedPersonID")] or ""),
                        "AuthorFullName": str(row[col("AuthorFullName")] or ""),
                        "UT": str(row[col("UT")] or ""),
                        "OrganizationID": str(row[col("OrganizationID")] or ""),
                    })
                else:
                    skip_count += 1

            merged = output_rows + extra_rows
            merged_csv = build_upload_csv(merged, source_file).encode("utf-8")
            st.success(f"✅ Merged {len(merged)} rows ({len(extra_rows)} from review, {skip_count} skipped)")
            st.download_button(
                "⬇️ Download Merged CSV",
                data=merged_csv,
                file_name=f"upload_ready_merged_{ts}.csv",
                mime="text/csv",
                width='stretch',
            )


# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — STATISTICS
# ══════════════════════════════════════════════════════════════════════════════

with tab_stats:
    if not st.session_state.processed:
        st.info("⬅️ Load and process data in Tab 1 to see statistics.")
    else:
        import pandas as pd

        muv_pairs = st.session_state.muv_pairs
        batch_result = st.session_state.batch_result
        person_index = st.session_state.person_index
        orgs = st.session_state.orgs

        confirmed = batch_result["confirmed"]
        needs_review = batch_result["needs_review"]
        new_persons = batch_result["new_persons"]

        st.markdown('<div class="sec-head">Processing Summary</div>', unsafe_allow_html=True)

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("WoS Records", len(st.session_state.wos_records))
        c2.metric("MUV Author-Doc Pairs", len(muv_pairs))
        c3.metric("Existing Persons (index)", len(person_index))
        c4.metric("Organizations", len(orgs))

        c5, c6, c7, c8 = st.columns(4)
        c5.metric("Auto-Confirmed (exact)", len(confirmed))
        c6.metric("New Persons Staged", len(new_persons))
        c7.metric("Needs Review", len(needs_review))
        c8.metric("Fuzzy Matches", len([r for r in needs_review if r["match_type"] == "fuzzy"]))

        st.markdown("---")

        # ── MUV authors chart
        st.markdown('<div class="sec-head">MUV Authors in Input</div>', unsafe_allow_html=True)
        author_doc_counts = defaultdict(int)
        for p in muv_pairs:
            author_doc_counts[p["author_full"]] += 1

        if author_doc_counts:
            df_authors = pd.DataFrame(
                [{"Author": k, "Documents": v} for k, v in
                 sorted(author_doc_counts.items(), key=lambda x: -x[1])]
            )
            st.bar_chart(df_authors.set_index("Author")["Documents"])

        st.markdown("---")

        # ── MUV affiliation distribution
        st.markdown('<div class="sec-head">MUV Affiliation Strings Detected</div>', unsafe_allow_html=True)
        affil_counts: dict[str, int] = defaultdict(int)
        for p in muv_pairs:
            for a in p["muv_affils"]:
                affil_counts[a] += 1

        if affil_counts:
            df_affils = pd.DataFrame(
                [{"Affiliation": k, "Count": v} for k, v in
                 sorted(affil_counts.items(), key=lambda x: -x[1])]
            )
            st.dataframe(df_affils, width='stretch')

        st.markdown("---")

        # ── All MUV pairs table
        st.markdown('<div class="sec-head">All MUV Author-Document Pairs</div>', unsafe_allow_html=True)
        df_pairs = pd.DataFrame([{
            "Author": p["author_full"],
            "UT": p["ut"],
            "MUV Affiliations": " | ".join(p["muv_affils"]),
        } for p in muv_pairs])
        st.dataframe(df_pairs, width='stretch', height=300)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 5 — HELP
# ══════════════════════════════════════════════════════════════════════════════

with tab_help:
    st.markdown("""
## WoS MUV Affiliation Ingestion Tool · User Guide

### Overview
This tool identifies **Medical University of Varna (MUV)**-affiliated authors from
Web of Science (WoS) exports and generates upload-ready entries for the
**WoS My Organization** module.

---

### Workflow

| Step | Tab | What to do |
|------|-----|-----------|
| 1 | Load Files | Upload WoS export CSV, ResearcherAndDocument.csv, and OrganizationHierarchy.csv |
| 2 | Review & Resolve | Verify fuzzy matches, assign organizations, approve/reject entries |
| 3 | Export Output | Download upload-ready CSV, review Excel, and audit log |

---

### Input Files

**WoS Export CSV** (`input*.csv`)
- Must contain columns: `AF`, `C1`, `UT`
- Export from WoS using "Tab-delimited (Win, UTF-8)" format → save as CSV

**ResearcherAndDocument.csv**
- Download from WoS My Organization → Export
- Columns: `PersonID`, `FirstName`, `LastName`, `OrganizationID`, `DocumentID`

**OrganizationHierarchy.csv**
- Download from WoS My Organization → Settings → Org Hierarchy
- Columns: `OrganizationID`, `OrganizationName`, `ParentOrgaID`

---

### Match Types

| Badge | Meaning | Action required |
|-------|---------|-----------------|
| ✓ EXISTING | Exact name match found | None — auto-confirmed |
| ⚠ AMBIGUOUS | Name similar to existing person(s) | Choose correct person or create new |
| 🆕 NEW PERSON | No match in existing data | Verify and assign organization |

---

### Configuration (sidebar)

- **Fuzzy threshold**: 0.85 recommended. Lower = more fuzzy matches surfaced.
- **MUV patterns**: Customize to catch transliteration variants or new sub-units.
- **Multi-org**: Allow assigning one author to multiple organizational units.

---

### Batch Workflow (for library teams)

1. Run processing → download **Review Excel** from Tab 3
2. Share Excel with curators to fill in `OrganizationID` and set `APPROVED = YES`
3. Return to the app → Tab 3 → upload filled Excel → download merged CSV
4. Import merged CSV into WoS My Organization

---

### CLI Alternative

```bash
# Interactive mode
python cli.py input100.csv --mode interactive

# Batch mode (generates review Excel)
python cli.py input100.csv --mode batch

# Re-import filled review Excel
python cli.py input100.csv --mode batch --reimport output/review_*.xlsx
```

---

### Output Files

| File | Description |
|------|-------------|
| `upload_ready_*.csv` | Import this into WoS My Organization |
| `review_*.xlsx` | Review sheet for human curation (batch mode) |
| `audit_*.json` | Full audit trail of all decisions |
| `staging.db` | SQLite staging database for incremental runs |
""")
