"""
app.py — WoS MyOrg Affiliation Tool v2
UT-centric review: one publication at a time, all its MUV authors resolved
before locking and moving to the next.
Medical University of Varna · Research Information Systems
"""

from __future__ import annotations

import csv
import io
import re
from collections import defaultdict
from datetime import datetime
from typing import Any

import pandas as pd
import streamlit as st

from core import (
    load_config,
    build_person_index,
    parse_org_hierarchy,
    parse_wos_csv,
    extract_muv_author_pairs,
    batch_process,
    build_upload_csv,
    build_audit_json,
    normalize_name,
    name_similarity,
)

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="WoS → MyOrg  v2",
    page_icon="🔬",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;500;600&display=swap');

html, body, [class*="css"] { font-family: 'IBM Plex Sans', sans-serif; }

/* ── Header ── */
.app-header {
    background: #0f1923;
    color: #e8f4f8;
    padding: 1.4rem 2rem 1.2rem;
    border-bottom: 3px solid #1a9dc8;
    margin: -1rem -1rem 1.5rem;
    display: flex; align-items: baseline; gap: 1rem;
}
.app-header h1 { font-size: 1.4rem; font-weight: 600; margin: 0; letter-spacing: .02em; color: #fff; }
.app-header .sub { font-family: 'IBM Plex Mono', monospace; font-size: .75rem; color: #7ec8e3; }

/* ── UT card ── */
.ut-card {
    background: #0f1923;
    border: 1px solid #1a4a6b;
    border-left: 4px solid #1a9dc8;
    border-radius: 6px;
    padding: 1rem 1.4rem;
    margin-bottom: 1.2rem;
    font-family: 'IBM Plex Mono', monospace;
}
.ut-card .ut-id   { font-size: 1.05rem; font-weight: 600; color: #7ec8e3; letter-spacing:.03em; }
.ut-card .ut-meta { font-size: .78rem; color: #8aabb8; margin-top: .3rem; }

/* ── Author row ── */
.author-row {
    border: 1px solid #e0e8ef;
    border-radius: 6px;
    padding: .85rem 1rem;
    margin-bottom: .7rem;
    background: #fafcfe;
}
.author-row.locked   { background: #f0faf4; border-color: #27ae60; }
.author-row.skipped  { background: #f8f8f8; border-color: #bbb; opacity: .7; }
.author-row.rejected { background: #fff5f5; border-color: #e74c3c; }

/* ── Badge ── */
.badge {
    display: inline-block; padding: .15rem .55rem; border-radius: 3px;
    font-size: .72rem; font-weight: 600; letter-spacing: .04em;
    font-family: 'IBM Plex Mono', monospace; margin-right: .4rem;
}
.badge-exact    { background:#d4edda; color:#155724; }
.badge-initial  { background:#fff3cd; color:#856404; }
.badge-fuzzy    { background:#d1ecf1; color:#0c5460; }
.badge-new      { background:#e8d5f5; color:#5a1f8a; }
.badge-skip     { background:#e2e3e5; color:#383d41; }
.badge-dup      { background:#f8d7da; color:#721c24; }

/* ── Progress bar ── */
.prog-bar-wrap { background:#e0e8ef; border-radius:4px; height:8px; margin:.5rem 0 1rem; }
.prog-bar-fill { background: linear-gradient(90deg,#1a9dc8,#27ae60); border-radius:4px; height:8px; transition: width .3s; }

/* ── Nav buttons ── */
.stButton>button {
    font-family: 'IBM Plex Sans', sans-serif;
    font-weight: 500; border-radius: 4px;
}

/* ── Section heading ── */
.sec-head {
    font-size: .7rem; font-weight: 600; letter-spacing: .1em;
    color: #1a9dc8; text-transform: uppercase; margin: 1.2rem 0 .5rem;
    border-bottom: 1px solid #d0e8f0; padding-bottom: .3rem;
}

/* ── Affil chip ── */
.chip {
    display:inline-block; background:#e8f4f8; color:#0c5460;
    border-radius:3px; padding:.1rem .45rem; font-size:.72rem;
    margin:.1rem; font-family:'IBM Plex Mono',monospace;
    border:1px solid #b8dde8;
}

/* ── Locked UT indicator ── */
.locked-ut {
    display:inline-flex; align-items:center; gap:.5rem;
    background:#f0faf4; border:1px solid #27ae60; border-radius:4px;
    padding:.35rem .8rem; font-size:.82rem; color:#155724;
    font-weight:500; margin-bottom:.5rem;
}

/* ── Metric grid ── */
.metric-grid { display:flex; gap:.8rem; flex-wrap:wrap; margin:.8rem 0 1.2rem; }
.metric-card {
    background:#fff; border:1px solid #d0e8f0; border-radius:6px;
    padding:.7rem 1.1rem; min-width:110px; text-align:center;
}
.metric-card .num { font-size:1.6rem; font-weight:700; font-family:'IBM Plex Mono',monospace; color:#0f1923; }
.metric-card .num-blue  { color:#1a9dc8; }
.metric-card .num-green { color:#27ae60; }
.metric-card .num-amber { color:#e67e22; }
.metric-card .num-red   { color:#e74c3c; }
.metric-card .lbl { font-size:.68rem; color:#7a8fa0; text-transform:uppercase; letter-spacing:.06em; margin-top:.15rem; }
</style>
""", unsafe_allow_html=True)

# ── Header ────────────────────────────────────────────────────────────────────
st.markdown("""
<div class="app-header">
  <h1>🔬 WoS → MyOrg</h1>
  <span class="sub">v2 · UT-centric review · Medical University of Varna</span>
</div>
""", unsafe_allow_html=True)

# ── Session state defaults ────────────────────────────────────────────────────
def _init():
    defaults = {
        "processed":       False,
        "ut_order":        [],       # ordered list of UT strings to review
        "ut_index":        0,        # current UT cursor
        "ut_locked":       {},       # ut → True when confirmed
        "author_decs":     {},       # (norm, ut) → decision dict
        "output_rows":     [],
        "rejected_rows":   [],
        "finalized":       False,
        "batch_result":    None,
        "person_index":    [],
        "existing_pairs":  set(),
        "orgs":            [],
        "cfg":             {},
        "source_file":     "",
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v
_init()


# ── Helpers ───────────────────────────────────────────────────────────────────

def _safe_key(*parts: str) -> str:
    raw = "_".join(str(p) for p in parts)
    safe = re.sub(r"[^a-z0-9]", "_", raw.lower())
    return re.sub(r"_+", "_", safe).strip("_")

def search_persons(query: str, person_index: list, max_results: int = 8) -> list:
    if not query or len(query) < 2:
        return []
    q = normalize_name(query)
    q_parts = q.split()
    results = []
    for p in person_index:
        name = p["NormName"]
        score = name_similarity(q, name)
        if any(part in name for part in q_parts if len(part) > 2):
            score = max(score, 0.45)
        if score >= 0.28:
            results.append((score, p))
    results.sort(key=lambda x: -x[0])
    return results[:max_results]

def org_label(oid: str, org_map: dict) -> str:
    for lbl, v in org_map.items():
        if v == oid:
            return lbl
    return oid

def build_org_map(orgs: list) -> tuple[dict, list]:
    m = {f"[{o['OrganizationID']}] {o['OrganizationName']}": o["OrganizationID"] for o in orgs}
    labels = ["— none / skip —"] + list(m.keys())
    return m, labels

def _ut_status(ut: str) -> str:
    """Return 'locked', 'skip' (all dupes), or 'pending'."""
    if st.session_state.ut_locked.get(ut):
        return "locked"
    result = st.session_state.batch_result
    if result is None:
        return "pending"
    # Check if all pairs for this UT are already_uploaded
    au = [r for r in result["already_uploaded"] if r.get("UT") == ut]
    nr = [r for r in result["needs_review"]     if r.get("UT") == ut]
    cf = [r for r in result["confirmed"]         if r.get("UT") == ut]
    if not nr and not cf:
        return "skip"
    return "pending"

def _ut_needs_attention(ut: str) -> list:
    """Rows for this UT that need user input (needs_review only)."""
    if st.session_state.batch_result is None:
        return []
    return [r for r in st.session_state.batch_result["needs_review"] if r.get("UT") == ut]

def _ut_auto_confirmed(ut: str) -> list:
    if st.session_state.batch_result is None:
        return []
    return [r for r in st.session_state.batch_result["confirmed"] if r.get("UT") == ut]

def _ut_already_uploaded(ut: str) -> list:
    if st.session_state.batch_result is None:
        return []
    return [r for r in st.session_state.batch_result["already_uploaded"] if r.get("UT") == ut]

def _all_authors_decided(ut: str) -> bool:
    """True when every needs_review row for this UT has a decision."""
    rows = _ut_needs_attention(ut)
    for r in rows:
        key = (normalize_name(r["AuthorFullName"]), ut)
        dec = st.session_state.author_decs.get(key)
        if dec is None or not dec.get("decided", False):
            return False
    return True

def _get_dec(norm: str, ut: str) -> dict:
    return st.session_state.author_decs.get((norm, ut), {})

def _set_dec(norm: str, ut: str, dec: dict):
    st.session_state.author_decs[(norm, ut)] = dec


# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_load, tab_review, tab_export = st.tabs([
    "📂 1 · Load Files",
    "🔍 2 · Review by Publication",
    "⬇️ 3 · Export",
])

# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — LOAD
# ══════════════════════════════════════════════════════════════════════════════
with tab_load:
    st.markdown('<div class="sec-head">Upload files</div>', unsafe_allow_html=True)

    c1, c2, c3 = st.columns(3)
    with c1:
        wos_file = st.file_uploader("WoS Export CSV", type=["csv","txt"], key="wos_up",
            help="Web of Science full-record export. Must contain AU, AF, C1, C3, UT fields.")
    with c2:
        res_file = st.file_uploader("ResearcherAndDocument.csv", type=["csv"], key="res_up",
            help="Current MyOrg researcher roster.")
    with c3:
        org_file = st.file_uploader("OrganizationHierarchy.csv", type=["csv"], key="org_up",
            help="Organisation hierarchy for affiliation picker.")

    if wos_file and res_file and org_file:
        with st.expander("Preview uploads", expanded=False):
            p1, p2 = st.columns(2)
            with p1:
                st.caption("WoS (first 5 rows)")
                st.dataframe(pd.read_csv(io.BytesIO(wos_file.read()), nrows=5,
                             encoding="utf-8-sig"), use_container_width=True)
                wos_file.seek(0)
            with p2:
                st.caption("ResearcherAndDocument (first 5 rows)")
                st.dataframe(pd.read_csv(io.BytesIO(res_file.read()), nrows=5,
                             encoding="utf-8-sig"), use_container_width=True)
                res_file.seek(0)

        if st.button("⚙️  Process files", type="primary", use_container_width=True):
            with st.spinner("Parsing and matching…"):
                wos_content = wos_file.read().decode("utf-8-sig")
                res_content = res_file.read().decode("utf-8-sig")
                org_content = org_file.read().decode("utf-8-sig")

                cfg = load_config("config.json")
                person_index, max_pid, existing_pairs = build_person_index(res_content)
                orgs  = parse_org_hierarchy(org_content)
                records   = parse_wos_csv(wos_content)
                muv_pairs = extract_muv_author_pairs(records, cfg)

                result = batch_process(
                    muv_pairs, person_index, orgs, cfg,
                    start_pid=max_pid + 1,
                    researcher_csv_content=res_content,
                    existing_pairs=existing_pairs,
                )

                # Build UT order: UTs with needs_review or confirmed first (need attention),
                # then all-duplicate UTs last (auto-skipped)
                all_uts_in_result = sorted({
                    r.get("UT", "") for r in
                    result["confirmed"] + result["needs_review"] + result["already_uploaded"]
                    if r.get("UT")
                })

                def ut_sort_key(ut):
                    has_review = any(r["UT"] == ut for r in result["needs_review"])
                    has_conf   = any(r["UT"] == ut for r in result["confirmed"])
                    return (0 if (has_review or has_conf) else 1, ut)

                ut_order = sorted(all_uts_in_result, key=ut_sort_key)

                st.session_state.update({
                    "processed":      True,
                    "batch_result":   result,
                    "person_index":   person_index,
                    "existing_pairs": existing_pairs,
                    "orgs":           orgs,
                    "cfg":            cfg,
                    "ut_order":       ut_order,
                    "ut_index":       0,
                    "ut_locked":      {},
                    "author_decs":    {},
                    "output_rows":    [],
                    "rejected_rows":  [],
                    "finalized":      False,
                    "source_file":    wos_file.name,
                    "max_pid":        max_pid,
                })
            st.success(f"✅ Processed {len(records)} records · {len(muv_pairs)} MUV pairs · "
                       f"{len(ut_order)} UTs to review.")
            st.info("➡️ Go to **Tab 2** to review by publication.")
    else:
        st.info("Upload all three files to begin.")

    if st.session_state.processed:
        result = st.session_state.batch_result
        au_all = result["already_uploaded"]
        n_skip = len({r["UT"] for r in au_all
                      if not any(r2["UT"] == r["UT"] for r2 in result["needs_review"] + result["confirmed"])})
        n_dup_prob = len([r for r in au_all if r.get("match_type") == "probable_duplicate"])

        st.markdown('<div class="sec-head">Processing summary</div>', unsafe_allow_html=True)
        st.markdown(f"""
<div class="metric-grid">
  <div class="metric-card"><div class="num num-blue">{len(st.session_state.ut_order)}</div><div class="lbl">UTs to review</div></div>
  <div class="metric-card"><div class="num num-green">{len(result['confirmed'])}</div><div class="lbl">Auto-confirmed</div></div>
  <div class="metric-card"><div class="num num-amber">{len(result['needs_review'])}</div><div class="lbl">Needs decision</div></div>
  <div class="metric-card"><div class="num">{len(result['already_uploaded'])}</div><div class="lbl">Already in MyOrg</div></div>
  <div class="metric-card"><div class="num num-red">{n_dup_prob}</div><div class="lbl">Prob. duplicates</div></div>
  <div class="metric-card"><div class="num">{n_skip}</div><div class="lbl">All-dup UTs (auto-skip)</div></div>
</div>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — UT-CENTRIC REVIEW
# ══════════════════════════════════════════════════════════════════════════════
with tab_review:
    if not st.session_state.processed:
        st.info("⬅️ Load and process files in Tab 1 first.")
        st.stop()

    result       = st.session_state.batch_result
    person_index = st.session_state.person_index
    orgs         = st.session_state.orgs
    cfg          = st.session_state.cfg
    ut_order     = st.session_state.ut_order
    org_map, org_labels = build_org_map(orgs)

    if not ut_order:
        st.success("Nothing to review.")
        st.stop()

    # ── Progress bar ─────────────────────────────────────────────────────────
    def _ut_is_done(ut: str) -> bool:
        """True when nothing more is needed for this UT."""
        if st.session_state.ut_locked.get(ut):
            return True
        if _ut_status(ut) == "skip":
            return True
        # UTs with ONLY auto-confirmed rows (no needs_review) need no user action
        if not _ut_needs_attention(ut):
            return True
        return False

    n_done = sum(1 for ut in ut_order if _ut_is_done(ut))
    n_need = sum(1 for ut in ut_order if _ut_needs_attention(ut))
    pct = int(100 * n_done / len(ut_order)) if ut_order else 100

    st.markdown(f"""
<div style="display:flex;justify-content:space-between;font-size:.8rem;color:#5a7080;">
  <span>Progress</span>
  <span><b>{n_done}</b> / {len(ut_order)} done &nbsp;·&nbsp; <b>{n_need}</b> need decisions</span>
</div>
<div class="prog-bar-wrap"><div class="prog-bar-fill" style="width:{pct}%"></div></div>
""", unsafe_allow_html=True)

    # ── UT navigation ─────────────────────────────────────────────────────────
    idx = st.session_state.ut_index
    idx = max(0, min(idx, len(ut_order) - 1))

    nav_l, nav_c, nav_r = st.columns([1, 4, 1])
    with nav_l:
        if st.button("◀ Prev", use_container_width=True, disabled=(idx == 0)):
            st.session_state.ut_index = max(0, idx - 1)
            st.rerun()
    with nav_r:
        if st.button("Next ▶", use_container_width=True, disabled=(idx >= len(ut_order) - 1)):
            st.session_state.ut_index = min(len(ut_order) - 1, idx + 1)
            st.rerun()
    with nav_c:
        # Jump-to selectbox — NO persistent key so label changes don't
        # cause mismatches when a UT is locked/unlocked mid-session.
        ut_display = [
            f"{'✅' if _ut_is_done(u) else '⏳'}  {u}"
            for u in ut_order
        ]
        jump = st.selectbox(
            "Jump to publication",
            ut_display,
            index=idx,
            label_visibility="collapsed",
        )
        jumped_idx = ut_display.index(jump)
        if jumped_idx != idx:
            st.session_state.ut_index = jumped_idx
            st.rerun()

    ut = ut_order[idx]
    status = _ut_status(ut)

    # ── UT header card ────────────────────────────────────────────────────────
    auto_rows  = _ut_auto_confirmed(ut)
    rev_rows   = _ut_needs_attention(ut)
    dup_rows   = _ut_already_uploaded(ut)

    _card_badge = ""
    if status == "locked":
        _card_badge = "<span style='color:#27ae60;font-weight:600;margin-left:.8rem;'>✅ LOCKED</span>"
    elif status == "skip":
        _card_badge = "<span style='color:#888;margin-left:.8rem;'>⏭ all duplicates — auto-skipped</span>"
    elif not rev_rows:
        _card_badge = "<span style='color:#27ae60;margin-left:.8rem;'>✅ auto-done (no decisions needed)</span>"

    st.markdown(f"""
<div class="ut-card">
  <div class="ut-id">{ut}</div>
  <div class="ut-meta">
    <b>{len(auto_rows)}</b> auto-confirmed &nbsp;·&nbsp;
    <b>{len(rev_rows)}</b> need decision &nbsp;·&nbsp;
    <b>{len(dup_rows)}</b> already in MyOrg
    {_card_badge}
  </div>
</div>
""", unsafe_allow_html=True)

    # ── Already-uploaded rows (collapsed summary) ─────────────────────────────
    if dup_rows:
        with st.expander(f"⏭  {len(dup_rows)} already in MyOrg (skipped)", expanded=False):
            for r in dup_rows:
                mt = r.get("match_type", "")
                badge = "badge-dup" if mt == "probable_duplicate" else "badge-skip"
                label = "PROB. DUP" if mt == "probable_duplicate" else "ALREADY IN"
                score = f" ({r['match_score']:.2f})" if mt == "probable_duplicate" else ""
                st.markdown(
                    f'<span class="badge {badge}">{label}{score}</span> '
                    f'<b>{r.get("author_full", r.get("AuthorFullName",""))}</b>'
                    f' → {r.get("AuthorFullName","")} [{r.get("PersonID","")}]'
                    f' <span style="font-size:.75rem;color:#888;">{r.get("Reason","")}</span>',
                    unsafe_allow_html=True,
                )

    # ── Auto-confirmed rows ───────────────────────────────────────────────────
    if auto_rows:
        with st.expander(f"✅  {len(auto_rows)} auto-confirmed (exact match)", expanded=False):
            for r in auto_rows:
                st.markdown(
                    f'<span class="badge badge-exact">EXACT</span> '
                    f'<b>{r.get("author_full", r.get("AuthorFullName",""))}</b>'
                    f' → {r.get("AuthorFullName","")} [{r.get("PersonID","")}]'
                    f' <span class="chip">{r.get("OrganizationID","")}</span>',
                    unsafe_allow_html=True,
                )

    # ── Rows needing decision ─────────────────────────────────────────────────
    if status == "locked":
        st.markdown('<div class="locked-ut">🔒 Publication confirmed — all authors resolved</div>',
                    unsafe_allow_html=True)

        # Show a summary of every decision made for this UT
        if rev_rows:
            with st.expander(f"🗒 {len(rev_rows)} resolved author(s) — click to review", expanded=False):
                for r in rev_rows:
                    raw_a = r.get("AuthorFullName", r.get("author_full", ""))
                    norm_r = normalize_name(raw_a)
                    dec_r  = st.session_state.author_decs.get((norm_r, ut), {})
                    action = dec_r.get("action", "approve")
                    mt_r   = dec_r.get("match_type", r.get("match_type", "new"))
                    badge_map_r = {
                        "initial_expansion": ("badge-initial", "INITIAL"),
                        "fuzzy":             ("badge-fuzzy",   "FUZZY"),
                        "new":               ("badge-new",     "NEW"),
                        "resolved":          ("badge-exact",   "RESOLVED"),
                    }
                    bcls_r, blbl_r = badge_map_r.get(mt_r, ("badge-new", mt_r.upper()))
                    if action == "reject":
                        st.markdown(
                            f'<span class="badge badge-dup">REJECTED</span> '                            f'<b>{raw_a}</b>',
                            unsafe_allow_html=True,
                        )
                    else:
                        org_ids_r = [o for o in dec_r.get("org_ids", []) if o]
                        org_str_r = " ".join(f'<span class="chip">{o}</span>' for o in org_ids_r) or "—"
                        st.markdown(
                            f'<span class="badge {bcls_r}">{blbl_r}</span> '                            f'<b>{raw_a}</b> → '                            f'{dec_r.get("resolved_name", raw_a)} '                            f'[{dec_r.get("resolved_pid", "")}] '                            f'{org_str_r}',
                            unsafe_allow_html=True,
                        )

        # Unlock must stay on this UT — do NOT advance index
        if st.button("🔓 Unlock to re-edit", key=f"unlock_{ut}"):
            st.session_state.ut_locked[ut] = False
            # Stay on this UT — explicitly write back the current index
            st.session_state.ut_index = idx
            st.rerun()

    elif status == "skip":
        st.success("All authors for this publication are already in MyOrg — nothing to do.")

    else:
        # ── Author decision cards ──────────────────────────────────────────────
        if rev_rows:
            st.markdown('<div class="sec-head">Authors needing a decision</div>',
                        unsafe_allow_html=True)

        all_decided = True  # track whether we can enable Lock button

        for r in rev_rows:
            raw_author = r.get("AuthorFullName", r.get("author_full", ""))
            norm       = normalize_name(raw_author)
            mt         = r.get("match_type", "new")
            dec        = _get_dec(norm, ut)

            if not dec:
                dec = {
                    "decided":       False,
                    "action":        "approve",   # approve | reject
                    "resolved_pid":  r.get("suggested_pid", ""),
                    "resolved_name": r.get("suggested_name", raw_author),
                    "org_ids":       r.get("suggested_org_ids") or [""],
                    "match_type":    mt,
                    "_search":       "",
                    "_search_ovr":   "",
                }
                _set_dec(norm, ut, dec)

            decided = dec.get("decided", False)
            if not decided:
                all_decided = False

            # Badge
            badge_map = {
                "initial_expansion": ("badge-initial", "INITIAL"),
                "fuzzy":             ("badge-fuzzy",   "FUZZY"),
                "new":               ("badge-new",     "NEW"),
            }
            bcls, blbl = badge_map.get(mt, ("badge-new", mt.upper()))

            # Affil chips
            chips = " ".join(
                f'<span class="chip">{a}</span>'
                for a in r.get("muv_affils", [r.get("RawAffil", "")])[:3]
            )

            # Card styling
            card_cls = "author-row"
            if decided:
                if dec.get("action") == "reject":
                    card_cls += " rejected"
                else:
                    card_cls += " locked"

            st.markdown(f"""
<div class="{card_cls}">
  <span class="badge {bcls}">{blbl}</span>
  <b>{raw_author}</b>
  <span style="font-size:.75rem;color:#7a8fa0;margin-left:.5rem;">{chips}</span>
</div>
""", unsafe_allow_html=True)

            with st.container():
                left, right = st.columns([3, 2])

                with left:
                    # ── Identity picker ────────────────────────────────────────
                    cands = r.get("candidates", [])

                    if mt in ("fuzzy", "initial_expansion") and cands:
                        # Algorithm candidates + search override
                        cand_labels = [
                            f"[{p['PersonID']}] {p['AuthorFullName']}  ({s:.2f})"
                            for s, p, _ in cands
                        ]
                        cand_labels.append("➕ Create as NEW PERSON")

                        saved_choice = dec.get("_cand_choice", cand_labels[0])
                        def_idx = cand_labels.index(saved_choice) if saved_choice in cand_labels else 0

                        choice = st.selectbox(
                            f"Identity for **{raw_author}**",
                            cand_labels,
                            index=def_idx,
                            key=_safe_key("cand", norm, ut),
                            disabled=decided,
                        )
                        dec["_cand_choice"] = choice

                        if not dec.get("_override_pid"):
                            if "NEW PERSON" in choice:
                                dec["resolved_pid"]  = r.get("suggested_pid", "")
                                dec["resolved_name"] = raw_author
                                dec["match_type"]    = "new"
                                dec["org_ids"]       = [""]
                            else:
                                idx_c = cand_labels.index(choice)
                                _, cp, _ = cands[idx_c]
                                dec["resolved_pid"]  = cp["PersonID"]
                                dec["resolved_name"] = cp["AuthorFullName"]
                                dec["match_type"]    = "resolved"
                                dec["org_ids"] = cp.get("OrganizationIDs") or (
                                    [cp["OrganizationID"]] if cp.get("OrganizationID") else [""])

                    else:
                        # New person — search picker
                        sq = st.text_input(
                            f"Search existing for **{raw_author}**",
                            value=dec.get("_search", raw_author),
                            key=_safe_key("search", norm, ut),
                            disabled=decided,
                        )
                        dec["_search"] = sq

                        hits = search_persons(sq, person_index)
                        NEW_LBL = f"➕ Create as NEW PERSON  (ID {r.get('suggested_pid','')})"
                        opts = [NEW_LBL] + [
                            f"[{hp['PersonID']}] {hp['AuthorFullName']}  ·  {int(hs*100)}%"
                            for hs, hp in hits
                        ]
                        hit_map = {
                            f"[{hp['PersonID']}] {hp['AuthorFullName']}  ·  {int(hs*100)}%": hp
                            for hs, hp in hits
                        }
                        saved_s = dec.get("_search_choice", NEW_LBL)
                        s_def   = opts.index(saved_s) if saved_s in opts else 0

                        sch = st.selectbox(
                            f"Identity for **{raw_author}**",
                            opts,
                            index=s_def,
                            key=_safe_key("search_pick", norm, ut),
                            disabled=decided,
                        )
                        dec["_search_choice"] = sch

                        if not dec.get("_override_pid"):
                            if sch == NEW_LBL:
                                dec["resolved_pid"]  = r.get("suggested_pid", "")
                                dec["resolved_name"] = raw_author
                                dec["match_type"]    = "new"
                                dec["org_ids"]       = [""]
                            else:
                                hp = hit_map[sch]
                                dec["resolved_pid"]  = hp["PersonID"]
                                dec["resolved_name"] = hp["AuthorFullName"]
                                dec["match_type"]    = "resolved"
                                dec["org_ids"] = hp.get("OrganizationIDs") or (
                                    [hp["OrganizationID"]] if hp.get("OrganizationID") else [""])

                    # ── Override search (for fuzzy/initial) ───────────────────
                    if mt in ("fuzzy", "initial_expansion"):
                        ovr_active = bool(dec.get("_override_pid"))
                        ovr_lbl = (
                            f"✏️ Override: {dec.get('resolved_name','')} [{dec.get('resolved_pid','')}]"
                            if ovr_active else "🔍 Not right? Search full list"
                        )
                        with st.expander(ovr_lbl, expanded=ovr_active):
                            if ovr_active and not decided:
                                if st.button("✖ Clear override",
                                             key=_safe_key("clr_ovr", norm, ut)):
                                    for k in ("_override_pid","_override_query","_override_choice"):
                                        dec.pop(k, None)
                                    _set_dec(norm, ut, dec)
                                    st.rerun()

                            ovq = st.text_input(
                                "Search",
                                value=dec.get("_override_query", ""),
                                key=_safe_key("ovr_q", norm, ut),
                                disabled=decided,
                            )
                            dec["_override_query"] = ovq

                            if ovq:
                                oh = search_persons(ovq, person_index)
                                if oh:
                                    NEW_OVR = f"➕ Create as NEW PERSON  (ID {r.get('suggested_pid','')})"
                                    o_opts  = [NEW_OVR] + [
                                        f"[{hp['PersonID']}] {hp['AuthorFullName']}  ·  {int(hs*100)}%"
                                        for hs, hp in oh
                                    ]
                                    o_map   = {
                                        f"[{hp['PersonID']}] {hp['AuthorFullName']}  ·  {int(hs*100)}%": hp
                                        for hs, hp in oh
                                    }
                                    saved_o = dec.get("_override_choice", NEW_OVR)
                                    o_def   = o_opts.index(saved_o) if saved_o in o_opts else 0
                                    oc = st.selectbox(
                                        "Select",
                                        o_opts,
                                        index=o_def,
                                        key=_safe_key("ovr_pick", norm, ut),
                                        disabled=decided,
                                    )
                                    dec["_override_choice"] = oc
                                    if oc != NEW_OVR:
                                        op = o_map[oc]
                                        dec["resolved_pid"]    = op["PersonID"]
                                        dec["resolved_name"]   = op["AuthorFullName"]
                                        dec["match_type"]      = "resolved"
                                        dec["_override_pid"]   = op["PersonID"]
                                        new_org = op.get("OrganizationIDs") or (
                                            [op["OrganizationID"]] if op.get("OrganizationID") else [])
                                        dec["org_ids"] = new_org or [""]
                                        # push to multiselect key
                                        ok = _safe_key("orgs", norm, ut)
                                        valid = [org_label(o, org_map) for o in new_org if o]
                                        valid = [l for l in valid if l in org_map]
                                        if valid:
                                            st.session_state[ok] = valid
                                    else:
                                        dec["resolved_pid"]  = r.get("suggested_pid", "")
                                        dec["resolved_name"] = raw_author
                                        dec["match_type"]    = "new"
                                        dec["org_ids"]       = [""]
                                        dec.pop("_override_pid", None)
                                else:
                                    st.caption("No matches found.")

                    # ── Show who will be used ──────────────────────────────────
                    if dec.get("resolved_pid") and dec.get("resolved_name"):
                        if dec["match_type"] == "new":
                            st.caption(f"📋 Will be created as **{dec['resolved_name']}** (new ID {dec['resolved_pid']})")
                        else:
                            st.caption(f"✔ Resolves to **{dec['resolved_name']}** (ID {dec['resolved_pid']})")
                            # Immediate duplicate warning
                            ep = st.session_state.existing_pairs
                            for it in [r]:
                                if (dec["resolved_pid"], it.get("UT","")) in ep:
                                    st.warning(f"⚠️ Already in MyOrg — will be skipped on save.")

                with right:
                    # ── Org picker ────────────────────────────────────────────
                    ok = _safe_key("orgs", norm, ut)
                    # Build default list
                    default_labels = [
                        org_label(o, org_map)
                        for o in dec.get("org_ids", [""])
                        if o and org_label(o, org_map) in org_map
                    ]
                    sel_orgs = st.multiselect(
                        "Organisation(s)",
                        options=list(org_map.keys()),
                        default=default_labels if ok not in st.session_state else None,
                        key=ok,
                        disabled=decided,
                    )
                    dec["org_ids"] = [org_map[l] for l in sel_orgs] or [""]

                    # ── Approve / Reject ──────────────────────────────────────
                    st.markdown("")
                    a_col, r_col = st.columns(2)
                    with a_col:
                        ap_key = _safe_key("approve", norm, ut)
                        if st.button(
                            "✅ Approve" if not decided or dec.get("action") == "reject" else "✅ Approved",
                            key=ap_key,
                            use_container_width=True,
                            type="primary" if not decided else "secondary",
                            disabled=(decided and dec.get("action") == "approve"),
                        ):
                            dec["decided"] = True
                            dec["action"]  = "approve"
                            _set_dec(norm, ut, dec)
                            st.rerun()

                    with r_col:
                        rj_key = _safe_key("reject", norm, ut)
                        if st.button(
                            "❌ Reject" if not decided or dec.get("action") == "approve" else "❌ Rejected",
                            key=rj_key,
                            use_container_width=True,
                            disabled=(decided and dec.get("action") == "reject"),
                        ):
                            dec["decided"] = True
                            dec["action"]  = "reject"
                            _set_dec(norm, ut, dec)
                            st.rerun()

                    if decided:
                        if st.button("✏️ Undo", key=_safe_key("undo", norm, ut),
                                     use_container_width=True):
                            dec["decided"] = False
                            _set_dec(norm, ut, dec)
                            st.rerun()

            _set_dec(norm, ut, dec)
            st.markdown("<hr style='margin:.5rem 0;border-color:#e0e8ef;'>", unsafe_allow_html=True)

        # ── Lock button ───────────────────────────────────────────────────────
        st.markdown("")
        can_lock = _all_authors_decided(ut) or not rev_rows

        lock_help = "" if can_lock else "Decide all authors above before locking."
        if st.button(
            "🔒 Confirm & Lock Publication",
            type="primary",
            use_container_width=True,
            disabled=not can_lock,
            help=lock_help,
            key=f"lock_{ut}",
        ):
            st.session_state.ut_locked[ut] = True
            # Advance to next unlocked UT if possible
            for i in range(idx + 1, len(ut_order)):
                next_ut = ut_order[i]
                if not st.session_state.ut_locked.get(next_ut) and _ut_status(next_ut) != "skip":
                    st.session_state.ut_index = i
                    break
            else:
                st.session_state.ut_index = min(idx + 1, len(ut_order) - 1)
            st.rerun()

    # ── Sidebar mini-map ──────────────────────────────────────────────────────
    with st.sidebar:
        st.markdown("### Publication list")
        for i, u in enumerate(ut_order):
            icon = "✅" if _ut_is_done(u) else ("⏳" if _ut_needs_attention(u) else "—")
            if st.button(f"{icon} {u}", key=f"sb_{i}", use_container_width=True):
                st.session_state.ut_index = i
                st.rerun()


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — EXPORT
# ══════════════════════════════════════════════════════════════════════════════
with tab_export:
    if not st.session_state.processed:
        st.info("⬅️ Complete Tabs 1 and 2 first.")
        st.stop()

    result       = st.session_state.batch_result
    ut_order     = st.session_state.ut_order
    existing_pairs = st.session_state.existing_pairs

    n_locked = sum(1 for ut in ut_order
                   if st.session_state.ut_locked.get(ut) or _ut_status(ut) == "skip")
    n_total  = len(ut_order)
    n_pending = n_total - n_locked

    if n_pending > 0:
        st.warning(f"⚠️ **{n_pending} publication(s) not yet locked.** "
                   f"You can still export — unlocked publications will be included as-is, "
                   f"with unapproved authors omitted.")

    if st.button("⚙️ Build export", type="primary", use_container_width=True):
        output_rows   = []
        rejected_rows = []
        seen: set[tuple] = set()

        # 1 — Auto-confirmed exact rows
        for r in result["confirmed"]:
            k = (r["PersonID"], r.get("UT",""), r.get("OrganizationID",""))
            if k not in seen:
                seen.add(k)
                output_rows.append(r)

        # 2 — User decisions
        for r in result["needs_review"]:
            raw_author = r.get("AuthorFullName", r.get("author_full",""))
            norm = normalize_name(raw_author)
            ut   = r.get("UT","")
            dec  = st.session_state.author_decs.get((norm, ut))

            if not dec or dec.get("action") == "reject" or not dec.get("decided"):
                rejected_rows.append({
                    "AuthorFullName": raw_author, "UT": ut,
                    "Reason": "Rejected" if (dec and dec.get("action") == "reject") else "Not decided",
                })
                continue

            pid           = str(dec.get("resolved_pid","")).strip()
            resolved_name = dec.get("resolved_name", raw_author)
            org_ids       = [o for o in dec.get("org_ids",[""])  if o] or [""]

            # Check against existing_pairs (resolved-to-existing may already be there)
            if pid and (pid, ut) in existing_pairs:
                rejected_rows.append({
                    "AuthorFullName": resolved_name, "UT": ut,
                    "Reason": "Already in MyOrg (resolved match)",
                })
                continue

            for oid in org_ids:
                k = (pid, ut, oid)
                if k in seen:
                    rejected_rows.append({"AuthorFullName": resolved_name, "UT": ut, "Reason": "Duplicate"})
                    continue
                seen.add(k)
                output_rows.append({
                    "PersonID": pid, "AuthorFullName": resolved_name,
                    "UT": ut, "OrganizationID": oid,
                    "match_type": dec.get("match_type",""),
                })

        st.session_state.output_rows   = output_rows
        st.session_state.rejected_rows = rejected_rows
        st.session_state.finalized     = True
        st.success(f"✅ {len(output_rows)} rows ready · {len(rejected_rows)} excluded.")

    if st.session_state.finalized:
        output_rows   = st.session_state.output_rows
        rejected_rows = st.session_state.rejected_rows
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        src = st.session_state.source_file

        c1, c2 = st.columns(2)

        with c1:
            st.markdown("#### Upload-ready CSV")
            csv_bytes = build_upload_csv(output_rows, src).encode("utf-8")
            st.download_button(
                "⬇️ Download upload_ready.csv",
                data=csv_bytes,
                file_name=f"upload_ready_{ts}.csv",
                mime="text/csv",
                use_container_width=True,
            )
            st.dataframe(
                pd.DataFrame(output_rows)[["PersonID","AuthorFullName","UT","OrganizationID"]]
                if output_rows else pd.DataFrame(),
                use_container_width=True, height=300,
            )

        with c2:
            st.markdown("#### Excluded rows")
            if rejected_rows:
                st.download_button(
                    "⬇️ Download excluded.csv",
                    data=pd.DataFrame(rejected_rows).to_csv(index=False).encode("utf-8"),
                    file_name=f"excluded_{ts}.csv",
                    mime="text/csv",
                    use_container_width=True,
                )
                st.dataframe(pd.DataFrame(rejected_rows), use_container_width=True, height=300)
            else:
                st.success("No excluded rows.")

        # New persons CSV
        new_pids = sorted({
            r["PersonID"] for r in output_rows
            if r.get("match_type") == "new"
        })
        if new_pids:
            st.markdown("#### New persons to register")
            st.caption("These PersonIDs are new — add them to ResearcherAndDocument.csv before the next run.")
            new_person_names = {
                dec["resolved_pid"]: dec["resolved_name"]
                for (norm_k, ut_k), dec in st.session_state.author_decs.items()
                if dec.get("match_type") == "new" and dec.get("action") == "approve"
            }
            new_rows = [{"PersonID": pid, "SuggestedName": new_person_names.get(pid, "")}
                        for pid in new_pids]
            st.dataframe(pd.DataFrame(new_rows), use_container_width=True)
            st.download_button(
                "⬇️ Download new_persons.csv",
                data=pd.DataFrame(new_rows).to_csv(index=False).encode("utf-8"),
                file_name=f"new_persons_{ts}.csv",
                mime="text/csv",
                use_container_width=True,
            )
