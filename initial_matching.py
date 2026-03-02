"""
initial_matching.py — Initial-aware author name matching for wos2myorg
=======================================================================
Augments the existing fuzzy matching in core.py with two new capabilities:

1. INITIAL EXPANSION MATCHING
   WoS AF names often contain only initials (e.g. "Lazarov, N." or "Lazarov, N. R.").
   This module checks whether such a name is consistent with a full name in the
   ResearcherAndDocument master list (e.g. "Lazarov, Nikolay" or "Lazarov, Nikolay R.")
   by verifying that every provided initial matches the corresponding word in the full name.

2. INITIAL-PREFIX GROUPING
   When multiple WoS-sourced author variants share the same surname and compatible initials
   (e.g. "Lazarov, N. R." and "Lazarov, N."; "Velyanov, V." and "Velyanov, V. V."),
   they are grouped together so that a match found for one propagates to all siblings.

Usage inside core.py
---------------------
Replace (or augment) the section that classifies a detected author as
"exact match", "fuzzy match", or "new person" with calls to:

    from initial_matching import InitialAwareMatcher

    matcher = InitialAwareMatcher(researcher_df)      # build once

    result = matcher.match(wos_name)
    # result.kind  → "exact" | "initial_expansion" | "fuzzy" | "new"
    # result.candidates → list[CandidateMatch]  (ranked best-first)
    # result.group_key  → str  (surname + sorted initials; for grouping siblings)

Integration points
------------------
* In cli.py interactive mode: treat "initial_expansion" like "fuzzy" — present
  candidate(s) to the operator for confirmation.
* In cli.py batch mode: treat "initial_expansion" like "fuzzy" — export to
  review Excel rather than auto-confirming.
* Use group_key to merge rows in the review Excel so siblings appear together,
  reducing visual clutter and enabling one-click resolution for the whole group.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from typing import List, Optional

try:
    from rapidfuzz import fuzz as _rf_fuzz
    _RAPIDFUZZ = True
except ImportError:
    _RAPIDFUZZ = False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _strip_diacritics(text: str) -> str:
    """Remove diacritics; keep ASCII letters, digits, spaces, commas, hyphens."""
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def _normalize(name: str) -> str:
    """Lower-case, strip diacritics, collapse whitespace, remove punctuation
    except commas (used as surname/firstname separator)."""
    name = _strip_diacritics(name).lower()
    name = re.sub(r"[^a-z0-9, ]", " ", name)
    return re.sub(r"\s+", " ", name).strip()


def _similarity(a: str, b: str) -> float:
    if _RAPIDFUZZ:
        return _rf_fuzz.ratio(a, b) / 100.0
    return SequenceMatcher(None, a, b).ratio()


def _parse_name(raw: str) -> tuple[str, list[str]]:
    """
    Parse a name in "Surname, Firstnames/Initials" format.

    Returns
    -------
    surname : str  (lower-case, diacritics stripped)
    parts   : list[str]  individual first-name tokens, lower-case
              Each token is either a full word ("nikolay") or a single letter ("n").

    Examples
    --------
    "Lazarov, N."          → ("lazarov", ["n"])
    "Lazarov, N. R."       → ("lazarov", ["n", "r"])
    "Lazarov, Nikolay"     → ("lazarov", ["nikolay"])
    "Lazarov, Nikolay R."  → ("lazarov", ["nikolay", "r"])
    "Velyanov, Viktor V."  → ("velyanov", ["viktor", "v"])
    """
    norm = _normalize(raw)
    if "," in norm:
        surname_part, given_part = norm.split(",", 1)
    else:
        # Fallback: last token is surname
        tokens = norm.split()
        surname_part = tokens[-1] if tokens else norm
        given_part = " ".join(tokens[:-1])

    surname = surname_part.strip()
    # Split given part into individual tokens (handles "n r", "nikolay r", etc.)
    parts = [t.strip(".") for t in given_part.split() if t.strip(".")]
    return surname, parts


def _is_initial(token: str) -> bool:
    """True if token is a single letter (i.e. an initial)."""
    return len(token) == 1 and token.isalpha()


def _initials_compatible(wos_parts: list[str], master_parts: list[str]) -> bool:
    """
    Check whether the WoS name parts are compatible with the master name parts.

    Rules
    -----
    * The WoS name may have *fewer* parts than the master (extra trailing parts
      in master are allowed — e.g. WoS "N." is compatible with master "Nikolay R.").
    * For each WoS part:
        - If the WoS token is an initial → the corresponding master token must
          start with that initial.
        - If the WoS token is a full name → either it equals the master token,
          OR the master token is an initial of it.
    * The master may NOT have fewer parts than the WoS (e.g. WoS "N. R." is NOT
      compatible with master "Nikolay" because the second initial has no match).

    Examples
    --------
    wos ["n"]          master ["nikolay"]          → True
    wos ["n"]          master ["nikolay", "r"]     → True  (extra master part OK)
    wos ["n", "r"]     master ["nikolay"]          → False (master too short)
    wos ["n", "r"]     master ["nikolay", "r"]     → True
    wos ["n", "r"]     master ["nikolay", "rumen"] → True
    wos ["viktor", "v"] master ["viktor", "v"]     → True
    wos ["viktor", "v"] master ["viktor", "velikov"]→ True
    wos ["v"]          master ["viktor", "v"]      → True
    """
    if len(master_parts) < len(wos_parts):
        return False

    for i, wt in enumerate(wos_parts):
        mt = master_parts[i]
        if _is_initial(wt):
            if not mt.startswith(wt):
                return False
        else:
            # WoS has a full word
            if _is_initial(mt):
                # master has only an initial — must match first letter of WoS word
                if mt != wt[0]:
                    return False
            else:
                if wt != mt:
                    return False
    return True


def _group_key(surname: str, parts: list[str]) -> str:
    """
    Build a canonical grouping key from surname + initials only.

    All parts are reduced to their first letter so that:
      "lazarov n r"  and  "lazarov n"  and  "lazarov nikolay r"
    all map to the same key:  "lazarov|n|r"  /  "lazarov|n"

    The key is: surname + "|" + "|".join(first_letter of each part).
    A shorter WoS name (fewer parts) produces a *prefix* of a longer key.
    Grouping logic: two names are siblings if one key is a prefix of the other
    (see InitialAwareMatcher.build_sibling_groups).
    """
    initials = [p[0] for p in parts if p]
    return surname + "|" + "|".join(initials)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class CandidateMatch:
    person_id: str
    full_name: str          # as stored in ResearcherAndDocument
    score: float            # 0.0–1.0  (1.0 = exact / perfect initial match)
    match_type: str         # "exact" | "initial_expansion" | "fuzzy"


@dataclass
class MatchResult:
    wos_name: str
    kind: str               # "exact" | "initial_expansion" | "fuzzy" | "new"
    candidates: List[CandidateMatch] = field(default_factory=list)
    group_key: str = ""     # for sibling grouping


# ---------------------------------------------------------------------------
# Core matcher
# ---------------------------------------------------------------------------

class InitialAwareMatcher:
    """
    Drop-in augmentation for the existing name-matching logic in core.py.

    Parameters
    ----------
    researcher_df : pandas.DataFrame
        The loaded ResearcherAndDocument.csv.  Must have at minimum columns
        "PersonID", "FirstName", "LastName".

    fuzzy_threshold : float
        Minimum SequenceMatcher ratio to trigger fuzzy review (default 0.85).

    exact_threshold : float
        Ratio at or above which a match is considered exact (default 1.0).
    """

    def __init__(self, researcher_df, fuzzy_threshold: float = 0.85,
                 exact_threshold: float = 1.0):
        self.fuzzy_threshold = fuzzy_threshold
        self.exact_threshold = exact_threshold
        self._master: list[dict] = []
        self._build_master(researcher_df)

    # ------------------------------------------------------------------
    # Build internal master list
    # ------------------------------------------------------------------

    def _build_master(self, df) -> None:
        """Pre-process all ResearcherAndDocument rows into parsed form."""
        seen_ids: set[str] = set()
        for _, row in df.iterrows():
            pid = str(row.get("PersonID", "")).strip()
            if not pid or pid in seen_ids:
                continue
            seen_ids.add(pid)

            first = str(row.get("FirstName", "")).strip()
            last  = str(row.get("LastName",  "")).strip()

            if not last:
                continue  # skip malformed rows

            # Build the canonical "Surname, Firstname" string
            if first:
                canonical = f"{last}, {first}"
            else:
                canonical = last

            surname, parts = _parse_name(canonical)

            self._master.append({
                "person_id": pid,
                "canonical": canonical,
                "norm": _normalize(canonical),
                "surname": surname,
                "parts": parts,
            })

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def match(self, wos_name: str) -> MatchResult:
        """
        Match a single WoS author name against the master list.

        Returns a MatchResult with:
          - kind: best classification
          - candidates: all plausible matches ranked by score
          - group_key: for sibling grouping
        """
        norm_wos = _normalize(wos_name)
        wos_surname, wos_parts = _parse_name(wos_name)
        gk = _group_key(wos_surname, wos_parts)

        candidates: list[CandidateMatch] = []

        for master in self._master:
            # Quick surname check first (cheap filter)
            if master["surname"] != wos_surname:
                continue

            # --- 1. Exact string match ---
            if norm_wos == master["norm"]:
                candidates.append(CandidateMatch(
                    person_id=master["person_id"],
                    full_name=master["canonical"],
                    score=1.0,
                    match_type="exact",
                ))
                continue

            # --- 2. Initial expansion match ---
            # Only attempt when at least one side has initials
            if wos_parts and master["parts"]:
                if _initials_compatible(wos_parts, master["parts"]):
                    # Score: reward having more parts matched (longer master = more specific)
                    # Base score: 0.95 minus a small penalty per unmatched trailing master part
                    extra = len(master["parts"]) - len(wos_parts)
                    score = max(0.90, 0.98 - 0.01 * extra)
                    candidates.append(CandidateMatch(
                        person_id=master["person_id"],
                        full_name=master["canonical"],
                        score=score,
                        match_type="initial_expansion",
                    ))
                    continue

                # Also check the reverse: master has initials, WoS has full name
                # (less common but possible)
                if _initials_compatible(master["parts"], wos_parts):
                    candidates.append(CandidateMatch(
                        person_id=master["person_id"],
                        full_name=master["canonical"],
                        score=0.92,
                        match_type="initial_expansion",
                    ))
                    continue

            # --- 3. Fuzzy match ---
            score = _similarity(norm_wos, master["norm"])
            if score >= self.fuzzy_threshold:
                candidates.append(CandidateMatch(
                    person_id=master["person_id"],
                    full_name=master["canonical"],
                    score=score,
                    match_type="fuzzy",
                ))

        # Sort best-first
        candidates.sort(key=lambda c: c.score, reverse=True)

        # Determine overall kind
        if not candidates:
            kind = "new"
        elif candidates[0].match_type == "exact":
            kind = "exact"
        elif candidates[0].match_type == "initial_expansion":
            kind = "initial_expansion"
        else:
            kind = "fuzzy"

        return MatchResult(wos_name=wos_name, kind=kind,
                           candidates=candidates, group_key=gk)

    def build_sibling_groups(
        self, wos_names: list[str]
    ) -> dict[str, list[str]]:
        """
        Group a list of WoS author name strings by compatible initial prefix.

        Two names are siblings if one's group_key is a prefix of the other's
        (same surname, and shorter initial sequence is a prefix of longer one).

        Returns
        -------
        dict mapping canonical_group_key → list[wos_name]

        Example
        -------
        Input:  ["Lazarov, N. R.", "Lazarov, N.", "Velyanov, V.", "Velyanov, V. V."]
        Output: {
            "lazarov|n":   ["Lazarov, N. R.", "Lazarov, N."],
            "velyanov|v":  ["Velyanov, V.",   "Velyanov, V. V."],
        }
        (the *shortest* key in each cluster becomes the canonical group key)
        """
        keys: dict[str, str] = {}  # wos_name → group_key
        for name in wos_names:
            surname, parts = _parse_name(name)
            keys[name] = _group_key(surname, parts)

        # Cluster: two keys are siblings if one is a prefix of the other
        # (i.e. key_a == key_b[:len(key_a)])
        groups: dict[str, list[str]] = {}
        assigned: dict[str, str] = {}  # name → canonical_group_key

        for name, key in keys.items():
            # Find if this name belongs to an existing group
            merged_into: Optional[str] = None
            for canon_key in list(groups.keys()):
                # Are they compatible? (one is a prefix of the other)
                short, long_ = sorted([key, canon_key], key=len)
                if long_.startswith(short + "|") or long_ == short:
                    merged_into = canon_key
                    break

            if merged_into is None:
                # Start a new group with this key as canonical
                groups[key] = [name]
                assigned[name] = key
            else:
                groups[merged_into].append(name)
                assigned[name] = merged_into
                # If this name's key is shorter, it becomes the new canonical key
                if len(key) < len(merged_into):
                    groups[key] = groups.pop(merged_into)
                    # Update all previously assigned members
                    for n, k in assigned.items():
                        if k == merged_into:
                            assigned[n] = key

        return groups


# ---------------------------------------------------------------------------
# Integration helpers — call these from core.py / cli.py
# ---------------------------------------------------------------------------

def classify_wos_authors(
    wos_names: list[str],
    researcher_df,
    fuzzy_threshold: float = 0.85,
) -> dict[str, MatchResult]:
    """
    Convenience wrapper: match all WoS author names and return a dict.

    Parameters
    ----------
    wos_names       : list of raw name strings from WoS AF field
    researcher_df   : pandas DataFrame from ResearcherAndDocument.csv
    fuzzy_threshold : passed to InitialAwareMatcher

    Returns
    -------
    dict[wos_name → MatchResult]
    """
    matcher = InitialAwareMatcher(researcher_df, fuzzy_threshold)
    return {name: matcher.match(name) for name in wos_names}


def group_wos_authors(
    wos_names: list[str],
) -> dict[str, list[str]]:
    """
    Group WoS author names by initial-compatible surname clusters,
    without needing the researcher DataFrame.

    Useful for de-duplicating the "new authors" list before review.
    """
    # Build a throwaway matcher with an empty frame just to use build_sibling_groups
    import pandas as pd
    empty_df = pd.DataFrame(columns=["PersonID", "FirstName", "LastName"])
    matcher = InitialAwareMatcher(empty_df)
    return matcher.build_sibling_groups(wos_names)
