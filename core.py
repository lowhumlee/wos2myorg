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
        
        first_name = row.get('FirstName', '')
        last_name = row.get('LastName', '')
        full_name = f"{last_name}, {first_name}"
        norm = normalize_name(full_name)
        
        # Analyze name parts for better matching
        norm_last, norm_first = last_name.lower().strip(), first_name.lower().strip()
        norm_last = re.sub(r'[^a-z0-9\s]', '', strip_diacritics(norm_last))
        norm_first = re.sub(r'[^a-z0-9\s]', '', strip_diacritics(norm_first))
        
        # Is it initials only? (e.g., "N. R.")
        is_init = all(len(p) == 1 for p in norm_first.split())
        initials = "".join([p[0] for p in norm_first.split() if p])

        persons[pid_str] = {
            "PersonID": pid_str,
            "AuthorFullName": full_name, # Standard field name for the app
            "FullName": full_name,
            "NormName": norm,
            "Surname": norm_last,
            "GivenName": norm_first,
            "Initials": initials,
            "IsInitialsOnly": is_init,
            "InitialsKey": get_initials_key(full_name),
            "OrganizationID": row.get("OrganizationID", ""),
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
                        "author_full": auth, # Required by app.py and cli.py
                        "AuthorName": auth,
                        "RawAffil": affil_str.strip(),
                        "muv_affils": [affil_str.strip()], # Required by app.py
                        "ut": ut, # Required by app.py and cli.py
                        "UT": ut,
                    })
    return extracted


# ─── Matching Engine ─────────────────────────────────────────────────────────

def match_person(author_name: str, person_index: List[Dict], threshold: float | dict) -> tuple[str, list]:
    """
    Improved author matching logic with initial containment and strict rules.
    Returns (match_type, candidates_list).
    """
    if isinstance(threshold, dict):
        threshold = float(threshold.get("fuzzy_threshold", 0.85))

    norm_auth = normalize_name(author_name)
    if ',' not in norm_auth:
        # Fallback for names without comma
        for p in person_index:
            if p["NormName"] == norm_auth:
                return "exact", [(1.0, p, 1.0)]
        return "new", []

    auth_sur, auth_given = norm_auth.split(',', 1)
    auth_sur = auth_sur.strip()
    auth_given = auth_given.strip()
    
    auth_initials = "".join([p[0] for p in auth_given.split() if p])
    auth_is_init = all(len(p) == 1 for p in auth_given.split())
    
    candidates = []

    for p in person_index:
        # Exact string match is always top priority
        if p["NormName"] == norm_auth:
            return "exact", [(1.0, p, 1.0)]
        
        # Must have same surname
        if p["Surname"] != auth_sur:
            continue
            
        # First initial must match
        if not p["Initials"] or not auth_initials or p["Initials"][0] != auth_initials[0]:
            continue

        # Rule 1: Initial-Only Matching Rule
        # If WoS author is initials-only, only match against registry entries that are also initials-only.
        if auth_is_init and not p["IsInitialsOnly"]:
            continue
            
        # Rule 2: Initial Containment Logic
        # Allow grouping if one set of initials is a prefix of the other (e.g., N vs NR)
        if auth_initials.startswith(p["Initials"]) or p["Initials"].startswith(auth_initials):
            # We give a high score for initials match
            score = 0.95 if auth_initials == p["Initials"] else 0.90
            candidates.append((score, p, score))
            continue

        # Fallback to fuzzy similarity for full names (if neither is initials-only)
        if not auth_is_init and not p["IsInitialsOnly"]:
            score = name_similarity(norm_auth, p["NormName"])
            if score >= threshold:
                candidates.append((score, p, score))

    if candidates:
        candidates.sort(key=lambda x: x[0], reverse=True)
        return "fuzzy", candidates

    return "new", []


def group_new_authors(new_records: List[Dict]) -> List[Dict]:
    """
    Groups new authors that are variants of each other before insertion.
    Rule 3: Prefer variant with most complete initials.
    """
    # Sort by name length descending so we find more complete names first
    sorted_records = sorted(new_records, key=lambda x: len(x.get("author_full", "")), reverse=True)
    
    canonical_map = {} # norm_surname -> list of (canonical_name, initials)
    
    processed = []
    for rec in sorted_records:
        name = rec.get("author_full", "")
        norm = normalize_name(name)
        if ',' not in norm:
            rec["GroupedName"] = name
            processed.append(rec)
            continue
            
        sur, given = norm.split(',', 1)
        sur = sur.strip()
        given = given.strip()
        initials = "".join([p[0] for p in given.split() if p])
        
        found_canonical = None
        if sur in canonical_map:
            for canon_name, canon_initials in canonical_map[sur]:
                # If initials match or contain each other and first initial is same
                if initials and canon_initials and initials[0] == canon_initials[0]:
                    if initials.startswith(canon_initials) or canon_initials.startswith(initials):
                        found_canonical = canon_name
                        break
        
        if found_canonical:
            rec["GroupedName"] = found_canonical
        else:
            rec["GroupedName"] = name
            if sur not in canonical_map:
                canonical_map[sur] = []
            canonical_map[sur].append((name, initials))
            
        processed.append(rec)
        
    return processed


# ─── Batch Processing ─────────────────────────────────────────────────────────

def batch_process(muv_pairs: List[Dict], person_index: List[Dict], orgs: Dict, cfg: dict, start_pid: int = 9000):
    """
    Processes extracted pairs against the person index.
    Returns a dict with 'confirmed', 'needs_review', and 'new_persons'.
    """
    confirmed = []
    needs_review = []
    new_persons_staged = {} # norm -> entry
    
    pid_counter = start_pid
    threshold = float(cfg.get("fuzzy_threshold", 0.85))

    # Group by author first to apply grouping logic
    author_groups = defaultdict(list)
    for pair in muv_pairs:
        norm = normalize_name(pair["author_full"])
        author_groups[norm].append(pair)

    for norm, pairs in author_groups.items():
        author_full = pairs[0]["author_full"]
        match_type, candidates = match_person(author_full, person_index, threshold)
        
        if match_type == "exact":
            p = candidates[0][1]
            for pair in pairs:
                confirmed.append({
                    **pair,
                    "match_type": "exact",
                    "PersonID": p["PersonID"],
                    "AuthorFullName": p["AuthorFullName"],
                    "OrganizationID": p.get("OrganizationID", ""),
                })
        elif match_type == "fuzzy":
            for pair in pairs:
                needs_review.append({
                    **pair,
                    "match_type": "fuzzy",
                    "norm": norm,
                    "candidates": candidates,
                    "suggested_pid": candidates[0][1]["PersonID"],
                    "suggested_name": candidates[0][1]["AuthorFullName"],
                    "AuthorFullName": author_full,
                })
        else:
            # Check if this "new" author can be grouped with another new author
            # For batch process, we'll simplify: if we haven't seen a variant, it's a new canonical
            found_canonical = None
            sur, given = author_full.lower().split(',', 1) if ',' in author_full else (author_full.lower(), "")
            sur = re.sub(r'[^a-z0-9\s]', '', strip_diacritics(sur)).strip()
            initials = "".join([p[0] for p in given.split() if p])
            
            for staged_norm, staged_entry in new_persons_staged.items():
                s_sur = staged_entry["surname"]
                s_init = staged_entry["initials"]
                if sur == s_sur and initials and s_init and initials[0] == s_init[0]:
                    if initials.startswith(s_init) or s_init.startswith(initials):
                        found_canonical = staged_entry
                        # If current name is longer/more complete, update canonical
                        if len(author_full) > len(staged_entry["AuthorFullName"]):
                            staged_entry["AuthorFullName"] = author_full
                            staged_entry["initials"] = initials
                        break
            
            if found_canonical:
                pid = found_canonical["PersonID"]
                resolved_name = found_canonical["AuthorFullName"]
            else:
                pid = str(pid_counter)
                pid_counter += 1
                resolved_name = author_full
                new_persons_staged[norm] = {
                    "PersonID": pid, 
                    "AuthorFullName": author_full,
                    "surname": sur,
                    "initials": initials
                }
            
            for pair in pairs:
                needs_review.append({
                    **pair,
                    "match_type": "new",
                    "norm": norm,
                    "PersonID": pid,
                    "AuthorFullName": resolved_name,
                    "suggested_pid": pid,
                    "suggested_name": resolved_name,
                })

    return {
        "confirmed": confirmed,
        "needs_review": needs_review,
        "new_persons": list(new_persons_staged.values())
    }


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
            CREATE TABLE IF NOT EXISTS rejected (
                AuthorFullName TEXT, UT TEXT, Reason TEXT, Timestamp TEXT
            );
        """)
        self.conn.commit()

    def upsert_person(self, pid: str, full_name: str, norm: str, is_new: bool = True):
        self.conn.execute(
            "INSERT OR IGNORE INTO persons VALUES (?,?,?,?,?)",
            (pid, full_name, norm, int(is_new), datetime.now().isoformat(timespec="seconds"))
        )
        self.conn.commit()
    
    def log_decision(self, pid: str, dtype: str, detail: str):
        self.conn.execute(
            "INSERT INTO decisions VALUES (?,?,?,?)",
            (pid, dtype, detail, datetime.now().isoformat(timespec="seconds"))
        )
        self.conn.commit()

    def log_rejected(self, author: str, ut: str, reason: str):
        self.conn.execute(
            "INSERT INTO rejected VALUES (?,?,?,?)",
            (author, ut, reason, datetime.now().isoformat(timespec="seconds"))
        )
        self.conn.commit()

# ─── Export Formatters ────────────────────────────────────────────────────────

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

def build_audit_json(summary: dict, new_persons: list) -> str:
    """Generates the audit JSON structure for export."""
    data = {
        "generated_at": datetime.now().isoformat(),
        "summary": summary,
        "new_persons": new_persons
    }
    return json.dumps(data, indent=2, ensure_ascii=False)

def build_review_excel(results: List[Dict] | dict, org_hierarchy: Dict[str, str] = None):
    import openpyxl
    from openpyxl.styles import Font, PatternFill
    
    # Handle both list and dict inputs
    if isinstance(results, dict):
        # Flatten the dict result into a list of all items
        all_items = results.get("confirmed", []) + results.get("needs_review", [])
    else:
        all_items = results

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Author Review"
    headers = ["Status", "WoS Author", "Detected PersonID", "Existing Name", "Match Score", "UT", "Affiliation", "OrganizationID", "APPROVED"]
    ws.append(headers)
    
    for r in all_items:
        # Check for candidates or direct match
        m_pid = r.get("PersonID", "")
        m_name = r.get("AuthorFullName", "")
        score = r.get("Score", 1.0 if r.get("match_type") == "exact" else 0.0)
        
        ws.append([
            r.get("match_type", "UNKNOWN").upper(), 
            r.get("author_full", r.get("AuthorName", "")),
            m_pid, m_name,
            score, r.get("UT", r.get("ut", "")), 
            "; ".join(r.get("muv_affils", [])) if isinstance(r.get("muv_affils"), list) else r.get("RawAffil", ""),
            r.get("OrganizationID", ""), 
            "YES" if r.get("match_type") == "exact" else "PENDING"
        ])
    out = io.BytesIO()
    wb.save(out)
    return out.getvalue()
