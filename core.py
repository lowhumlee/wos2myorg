"""
core.py — WoS MUV Affiliation Ingestion Tool
Core processing engine shared by CLI and Streamlit GUI.
Medical University of Varna · Research Information Systems
"""

from __future__ import annotations

import csv
import difflib
import io
import json
import logging
import os
import re
import sqlite3
import unicodedata
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any

logger = logging.getLogger("wos_muv.core")

# ─── Default Configuration ────────────────────────────────────────────────────

DEFAULT_CONFIG: dict = {
    "muv_affiliation_patterns": [
        "medical university varna",
        "med univ varna",
        "mu varna",
        "medical university of varna",
        "муварна",
        "медицинскиуниверситетварна",
    ],
    "fuzzy_threshold": 0.85,
    "interactive_mode": True,
    "allow_multi_org": True,
    "new_person_id_start": 9000,
    "output_dir": "output",
    "db_path": "staging.db",
}


def load_config(path: str = "config.json") -> dict:
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            cfg = json.load(f)
        return {**DEFAULT_CONFIG, **cfg}
    return DEFAULT_CONFIG.copy()


# ─── Name Normalization ───────────────────────────────────────────────────────

def strip_diacritics(text: str) -> str:
    if not text: return ""
    return "".join(
        c for c in unicodedata.normalize("NFD", text)
        if unicodedata.category(c) != "Mn"
    )

def normalize_name(name: str) -> str:
    """Basic normalization: lower, no diacritics, only alphanumeric."""
    if not name: return ""
    s = strip_diacritics(name).lower()
    s = re.sub(r'[^a-z0-9\s,]', '', s)
    return " ".join(s.split())

def get_initials_key(name: str) -> str:
    """
    Converts 'Lazarov, Nikola R.' or 'Lazarov, N.R.' into 'lazarov n r'.
    Used for strict matching when full names are unavailable.
    """
    norm = normalize_name(name)
    if ',' not in norm:
        return norm
    surname, given = norm.split(',', 1)
    # Extract just the first letter of each part of the given name
    initials = " ".join([part[0] for part in given.split() if part])
    return f"{surname.strip()} {initials}".strip()

def name_similarity(a: str, b: str) -> float:
    """Fuzzy similarity between two strings."""
    return difflib.SequenceMatcher(None, a, b).ratio()


# ─── Data Parsing ─────────────────────────────────────────────────────────────

def build_person_index(csv_content: str) -> tuple[List[Dict], int]:
    """
    Parses ResearcherAndDocument.csv.
    Returns: (list of unique persons, max PersonID found).
    """
    persons = {}
    max_pid = 0
    f = io.StringIO(csv_content.strip())
    reader = csv.DictReader(f)
    for row in reader:
        pid_str = row.get("PersonID")
        if not pid_str:
            continue
            
        try:
            pid_int = int(pid_str)
            if pid_int > max_pid:
                max_pid = pid_int
        except ValueError:
            pass

        if pid_str in persons:
            continue
        
        full_name = f"{row.get('LastName', '')}, {row.get('FirstName', '')}"
        norm = normalize_name(full_name)
        
        persons[pid_str] = {
            "PersonID": pid_str,
            "FullName": full_name,
            "NormName": norm,
            "InitialsKey": get_initials_key(full_name)
        }
    return list(persons.values()), max_pid

def parse_org_hierarchy(csv_content: str) -> Dict[str, str]:
    """Returns mapping of ID -> Name."""
    orgs = {}
    f = io.StringIO(csv_content.strip())
    reader = csv.DictReader(f)
    for row in reader:
        oid = row.get("OrganizationID")
        oname = row.get("OrganizationName")
        if oid and oname:
            orgs[oid] = oname
    return orgs

def parse_wos_csv(csv_content: str) -> List[Dict]:
    """Parses WoS Export."""
    f = io.StringIO(csv_content.strip())
    # Detect tab vs comma
    sample = csv_content[:2000]
    dialect = 'excel-tab' if '\t' in sample else 'excel'
    reader = csv.DictReader(f, dialect=dialect)
    return [row for row in reader if row.get("UT")]


# ─── Extraction Logic ─────────────────────────────────────────────────────────

def extract_muv_author_pairs(wos_records: List[Dict], cfg: dict) -> List[Dict]:
    """
    Extracts (Author, Affiliation, UT) tuples where affiliation matches MUV patterns.
    """
    extracted = []
    patterns = [p.lower() for p in cfg.get("muv_affiliation_patterns", [])]

    for rec in wos_records:
        ut = rec.get("UT")
        c1 = rec.get("C1", "")
        if not c1: continue

        blocks = re.findall(r'\[(.*?)\]\s*([^\[]+)', c1)
        for authors_str, affil_str in blocks:
            affil_norm = normalize_name(affil_str)
            if any(p in affil_norm for p in patterns):
                authors = [a.strip() for a in authors_str.split(';')]
                for auth in authors:
                    extracted.append({
                        "AuthorName": auth,
                        "RawAffil": affil_str.strip(),
                        "UT": ut
                    })
    return extracted


# ─── Matching Engine ─────────────────────────────────────────────────────────

def match_person(author_name: str, person_index: List[Dict], threshold: float) -> Dict:
    norm_auth = normalize_name(author_name)
    auth_initials = get_initials_key(author_name)
    
    candidates = []

    for p in person_index:
        if p["NormName"] == norm_auth:
            return {"status": "EXACT", "match": p, "score": 1.0}
        
        if p["InitialsKey"] == auth_initials:
            candidates.append({"status": "AMBIGUOUS", "match": p, "score": 0.95})
            continue

        score = name_similarity(norm_auth, p["NormName"])
        if score >= threshold:
            candidates.append({"status": "AMBIGUOUS", "match": p, "score": score})

    if candidates:
        candidates.sort(key=lambda x: x["score"], reverse=True)
        return candidates[0]

    return {"status": "NEW", "match": None, "score": 0.0}


def group_new_authors(new_records: List[Dict]) -> List[Dict]:
    groups = defaultdict(list)
    for rec in new_records:
        ikey = get_initials_key(rec["AuthorName"])
        groups[ikey].append(rec)
    
    processed = []
    for ikey, items in groups.items():
        canonical_name = max([item["AuthorName"] for item in items], key=len)
        for item in items:
            item["GroupedName"] = canonical_name
            processed.append(item)
    return processed


# ─── Batch Processing ─────────────────────────────────────────────────────────

def batch_process(muv_pairs: List[Dict], person_index: List[Dict], orgs: Dict, cfg: dict, start_pid: int = 9000):
    """
    Processes extracted pairs against the person index.
    Signature matched to app.py expectations.
    """
    results = []
    new_authors_buffer = []

    for pair in muv_pairs:
        m = match_person(pair["AuthorName"], person_index, cfg.get("fuzzy_threshold", 0.85))
        res = {**pair, "Status": m["status"], "Match": m["match"], "Score": m["score"]}
        
        if m["status"] == "NEW":
            new_authors_buffer.append(res)
        else:
            results.append(res)

    if new_authors_buffer:
        grouped_new = group_new_authors(new_authors_buffer)
        results.extend(grouped_new)

    return results


# ─── Persistence & Helpers ────────────────────────────────────────────────────

class StagingDB:
    def __init__(self, db_path: str):
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self._create_tables()

    def _create_tables(self):
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS persons (
                PersonID TEXT PRIMARY KEY, FullName TEXT, NormName TEXT, IsNew INTEGER, Timestamp TEXT
            );
            CREATE TABLE IF NOT EXISTS affiliations (
                PersonID TEXT, UT TEXT, OrgID TEXT, RawAffil TEXT, SourceFile TEXT, Timestamp TEXT,
                PRIMARY KEY (PersonID, UT, OrgID)
            );
            CREATE TABLE IF NOT EXISTS decisions (
                PersonID TEXT, DecisionType TEXT, Detail TEXT, Timestamp TEXT
            );
        """)
        self.conn.commit()

# ... rest of persistence and export functions (build_upload_csv, build_review_excel)
# remain consistent with previous versions.

def build_upload_csv(affiliations: List[Dict]) -> str:
    output = io.StringIO()
    fieldnames = ["PersonID", "AuthorFullName", "UT", "OrganizationID", "SourceFile", "Timestamp"]
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for aff in affiliations:
        writer.writerow({
            "PersonID": aff.get("PersonID", ""),
            "AuthorFullName": aff.get("AuthorFullName", ""),
            "UT": aff.get("UT", ""),
            "OrganizationID": aff.get("OrgID", ""),
            "SourceFile": aff.get("SourceFile", "manual_entry"),
            "Timestamp": datetime.now().isoformat()
        })
    return output.getvalue()

def build_review_excel(results: List[Dict], org_hierarchy: Dict[str, str]):
    import openpyxl
    from openpyxl.styles import Font, PatternFill
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Author Review"
    headers = ["Status", "WoS Author", "Detected PersonID", "Existing Name", "Match Score", "UT", "Affiliation", "OrganizationID", "APPROVED"]
    ws.append(headers)
    for r in results:
        m = r.get("Match")
        ws.append([
            r["Status"], r.get("GroupedName", r["AuthorName"]),
            m["PersonID"] if m else "", m["FullName"] if m else "",
            r["Score"], r["UT"], r["RawAffil"], "", "YES" if r["Status"] == "EXACT" else "PENDING"
        ])
    out = io.BytesIO()
    wb.save(out)
    return out.getvalue()
