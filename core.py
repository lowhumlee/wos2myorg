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

def build_person_index(csv_content: str) -> List[Dict]:
    """Parses ResearcherAndDocument.csv and returns list of unique persons."""
    persons = {}
    f = io.StringIO(csv_content.strip())
    reader = csv.DictReader(f)
    for row in reader:
        pid = row.get("PersonID")
        if not pid or pid in persons: continue
        
        full_name = f"{row.get('LastName', '')}, {row.get('FirstName', '')}"
        norm = normalize_name(full_name)
        
        persons[pid] = {
            "PersonID": pid,
            "FullName": full_name,
            "NormName": norm,
            "InitialsKey": get_initials_key(full_name)
        }
    return list(persons.values())

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
    # WoS exports often have a Byte Order Mark or unusual quoting
    f = io.StringIO(csv_content.strip())
    reader = csv.DictReader(f, delimiter='\t' if '\t' in csv_content[:1000] else ',')
    return [row for row in reader if row.get("UT")]


# ─── Extraction Logic ─────────────────────────────────────────────────────────

def extract_muv_author_pairs(wos_records: List[Dict], patterns: List[str]) -> List[Dict]:
    """
    Extracts (Author, Affiliation, UT) tuples where affiliation matches MUV patterns.
    Handles the [Author] Affiliation format in C1.
    """
    extracted = []
    patterns = [p.lower() for p in patterns]

    for rec in wos_records:
        ut = rec.get("UT")
        c1 = rec.get("C1", "")
        if not c1: continue

        # Regex for [Author1; Author2] Affiliation
        blocks = re.findall(r'\[(.*?)\]\s*([^\[]+)', c1)
        
        for authors_str, affil_str in blocks:
            # Check if this affiliation is MUV
            affil_norm = normalize_name(affil_str)
            is_muv = any(p in affil_norm for p in patterns)
            
            if is_muv:
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
    """
    Tiered Matcher:
    1. Exact full-name match
    2. Initials-key match (e.g., 'Lazarov, N. R.' == 'Lazarov, Nikola R.')
    3. Fuzzy match
    """
    norm_auth = normalize_name(author_name)
    auth_initials = get_initials_key(author_name)
    
    best_match = None
    best_score = 0.0
    candidates = []

    for p in person_index:
        # 1. Exact Match
        if p["NormName"] == norm_auth:
            return {"status": "EXACT", "match": p, "score": 1.0}
        
        # 2. Initials Match (High Confidence)
        # Check if the surname and initials match exactly
        if p["InitialsKey"] == auth_initials:
            # We don't return immediately because there might be multiple people with same initials
            candidates.append({"status": "AMBIGUOUS", "match": p, "score": 0.95})
            continue

        # 3. Fuzzy Candidate
        score = name_similarity(norm_auth, p["NormName"])
        if score >= threshold:
            candidates.append({"status": "AMBIGUOUS", "match": p, "score": score})

    if candidates:
        # Sort by score descending
        candidates.sort(key=lambda x: x["score"], reverse=True)
        # If the top candidate is a very strong initials match, promote it to status 'AMBIGUOUS' 
        # but keep it as the primary choice.
        return candidates[0]

    return {"status": "NEW", "match": None, "score": 0.0}


def group_new_authors(new_records: List[Dict]) -> List[Dict]:
    """
    Groups 'NEW' authors that likely represent the same person.
    e.g., 'Lazarov, N. R.' and 'Lazarov, N.' will be merged into one staging identity.
    """
    groups = defaultdict(list)
    for rec in new_records:
        # Group by the initials-key to find variants of the same person
        ikey = get_initials_key(rec["AuthorName"])
        groups[ikey].append(rec)
    
    processed = []
    for ikey, items in groups.items():
        # Choose the longest name as the canonical one
        canonical_name = max([item["AuthorName"] for item in items], key=len)
        for item in items:
            item["GroupedName"] = canonical_name
            processed.append(item)
    return processed


# ─── Batch Processing ─────────────────────────────────────────────────────────

def batch_process(wos_content: str, researcher_content: str, cfg: dict):
    person_index = build_person_index(researcher_content)
    wos_records = parse_wos_csv(wos_content)
    muv_pairs = extract_muv_author_pairs(wos_records, cfg["muv_affiliation_patterns"])
    
    results = []
    new_authors_buffer = []

    for pair in muv_pairs:
        m = match_person(pair["AuthorName"], person_index, cfg["fuzzy_threshold"])
        res = {**pair, "Status": m["status"], "Match": m["match"], "Score": m["score"]}
        
        if m["status"] == "NEW":
            new_authors_buffer.append(res)
        else:
            results.append(res)

    # Apply grouping to new authors
    if new_authors_buffer:
        grouped_new = group_new_authors(new_authors_buffer)
        results.extend(grouped_new)

    return results


# ─── Persistence ─────────────────────────────────────────────────────────────

class StagingDB:
    def __init__(self, db_path: str):
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self._create_tables()

    def _create_tables(self):
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS persons (
                PersonID TEXT PRIMARY KEY,
                FullName TEXT,
                NormName TEXT,
                IsNew INTEGER,
                Timestamp TEXT
            );
            CREATE TABLE IF NOT EXISTS affiliations (
                PersonID TEXT,
                UT TEXT,
                OrgID TEXT,
                RawAffil TEXT,
                SourceFile TEXT,
                Timestamp TEXT,
                PRIMARY KEY (PersonID, UT, OrgID)
            );
            CREATE TABLE IF NOT EXISTS decisions (
                PersonID TEXT,
                DecisionType TEXT,
                Detail TEXT,
                Timestamp TEXT
            );
            CREATE TABLE IF NOT EXISTS rejected (
                AuthorName TEXT,
                UT TEXT,
                Reason TEXT,
                Timestamp TEXT
            );
        """)
        self.conn.commit()

    def upsert_person(self, pid: str, full_name: str, norm: str, is_new: bool = True):
        self.conn.execute(
            "INSERT OR IGNORE INTO persons VALUES (?,?,?,?,?)",
            (pid, full_name, norm, int(is_new), datetime.now().isoformat(timespec="seconds"))
        )
        self.conn.commit()

    def add_affiliation(self, pid: str, ut: str, org_id: str, raw_affil: str, source: str):
        try:
            self.conn.execute(
                "INSERT OR IGNORE INTO affiliations(PersonID,UT,OrgID,RawAffil,SourceFile,Timestamp) VALUES(?,?,?,?,?,?)\n",
                (pid, ut, org_id, raw_affil, source, datetime.now().isoformat(timespec="seconds"))
            )
            self.conn.commit()
        except Exception as e:
            logger.warning("DB affiliation insert error: %s", e)

    def log_decision(self, pid: str, dtype: str, detail: str):
        self.conn.execute(
            "INSERT INTO decisions(PersonID,DecisionType,Detail,Timestamp) VALUES(?,?,?,?)",
            (pid, dtype, detail, datetime.now().isoformat(timespec="seconds"))
        )
        self.conn.commit()

    def log_rejected(self, name: str, ut: str, reason: str):
        self.conn.execute(
            "INSERT INTO rejected(AuthorName,UT,Reason,Timestamp) VALUES(?,?,?,?)",
            (name, ut, reason, datetime.now().isoformat(timespec="seconds"))
        )
        self.conn.commit()


# ─── Export Formatters ────────────────────────────────────────────────────────

def build_upload_csv(affiliations: List[Dict]) -> str:
    """Generates the final MyOrg CSV."""
    output = io.StringIO()
    # Canonical MyOrg columns
    fieldnames = ["PersonID", "AuthorFullName", "UT", "OrganizationID", "SourceFile", "Timestamp"]
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for aff in affiliations:
        writer.writerow({
            "PersonID": aff["PersonID"],
            "AuthorFullName": aff.get("AuthorFullName", ""),
            "UT": aff["UT"],
            "OrganizationID": aff["OrgID"],
            "SourceFile": aff.get("SourceFile", "manual_entry"),
            "Timestamp": aff.get("Timestamp", datetime.now().isoformat())
        })
    return output.getvalue()


def build_audit_json(summary: dict, new_persons: list) -> str:
    data = {
        "generated_at": datetime.now().isoformat(),
        "summary": summary,
        "new_persons": new_persons
    }
    return json.dumps(data, indent=2, ensure_ascii=False)


def build_review_excel(results: List[Dict], org_hierarchy: Dict[str, str]):
    """Creates a review spreadsheet for manual curation."""
    import openpyxl
    from openpyxl.styles import Font, PatternFill

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Author Review"

    headers = ["Status", "WoS Author", "Detected PersonID", "Existing Name", "Match Score", "UT", "Affiliation", "OrganizationID", "APPROVED"]
    ws.append(headers)

    # Style header
    for cell in ws[1]:
        cell.font = Font(bold=True, color="FFFFFF")
        cell.fill = PatternFill(start_color="4F81BD", end_color="4F81BD", fill_type="solid")

    for r in results:
        match_obj = r.get("Match")
        status = r["Status"]
        # Use GroupedName for NEW authors to show consistency
        wos_name = r.get("GroupedName", r["AuthorName"])
        
        row = [
            status,
            wos_name,
            match_obj["PersonID"] if match_obj else "",
            match_obj["FullName"] if match_obj else "",
            r["Score"],
            r["UT"],
            r["RawAffil"],
            "", # Manual OrgID entry
            "YES" if status == "EXACT" else "PENDING"
        ]
        ws.append(row)

    output = io.BytesIO()
    wb.save(output)
    return output.getvalue()
