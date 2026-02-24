"""
tests_initial_matching.py — Unit tests for initial_matching.py
Run with:  python -m pytest tests_initial_matching.py -v
"""
import pytest
import pandas as pd
from initial_matching import (
    _parse_name,
    _initials_compatible,
    _group_key,
    InitialAwareMatcher,
    classify_wos_authors,
    group_wos_authors,
)


# ---------------------------------------------------------------------------
# _parse_name
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("raw,expected", [
    ("Lazarov, N.",          ("lazarov", ["n"])),
    ("Lazarov, N. R.",       ("lazarov", ["n", "r"])),
    ("Lazarov, Nikolay",     ("lazarov", ["nikolay"])),
    ("Lazarov, Nikolay R.",  ("lazarov", ["nikolay", "r"])),
    ("Velyanov, Viktor V.",  ("velyanov", ["viktor", "v"])),
    ("Velyanov, V.",         ("velyanov", ["v"])),
    ("Velyanov, V. V.",      ("velyanov", ["v", "v"])),
    ("Bratoeva, Kamelya",    ("bratoeva", ["kamelya"])),
    ("Tonchev, Anton B.",    ("tonchev", ["anton", "b"])),
])
def test_parse_name(raw, expected):
    assert _parse_name(raw) == expected


# ---------------------------------------------------------------------------
# _initials_compatible
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("wos_parts,master_parts,expected", [
    # Basic initial expansion
    (["n"],         ["nikolay"],           True),
    (["n"],         ["nikolay", "r"],      True),   # extra master part OK
    (["n", "r"],    ["nikolay"],           False),  # master too short
    (["n", "r"],    ["nikolay", "r"],      True),
    (["n", "r"],    ["nikolay", "rumen"],  True),
    # Full name vs initial
    (["viktor"],    ["v"],                 True),   # master has only initial
    (["viktor", "v"], ["viktor", "v"],     True),
    (["v"],         ["viktor", "v"],       True),
    # Mismatch
    (["n"],         ["maria"],             False),
    (["n", "r"],    ["nikolay", "s"],      False),
    # Exact full names
    (["nikolay"],   ["nikolay"],           True),
    (["nikolay"],   ["nikolay", "r"],      True),
])
def test_initials_compatible(wos_parts, master_parts, expected):
    assert _initials_compatible(wos_parts, master_parts) == expected


# ---------------------------------------------------------------------------
# _group_key
# ---------------------------------------------------------------------------

def test_group_key_same_for_prefix():
    # "Lazarov, N." and "Lazarov, N. R." should have related keys
    _, p1 = _parse_name("Lazarov, N.")
    _, p2 = _parse_name("Lazarov, N. R.")
    k1 = _group_key("lazarov", p1)  # "lazarov|n"
    k2 = _group_key("lazarov", p2)  # "lazarov|n|r"
    assert k2.startswith(k1 + "|") or k1 == k2


# ---------------------------------------------------------------------------
# InitialAwareMatcher
# ---------------------------------------------------------------------------

@pytest.fixture
def researcher_df():
    return pd.DataFrame([
        {"PersonID": "2570", "FirstName": "Nikolay R.", "LastName": "Lazarov"},
        {"PersonID": "2548", "FirstName": "Viktor V.",  "LastName": "Velyanov"},
        {"PersonID": "2161", "FirstName": "Kameliya",   "LastName": "Bratoeva"},
        {"PersonID": "2406", "FirstName": "Anton B.",   "LastName": "Tonchev"},
        {"PersonID": "2556", "FirstName": "Nikolay T.", "LastName": "Evtimov"},
        {"PersonID": "9001", "FirstName": "Maria",      "LastName": "Georgieva"},
    ])


class TestInitialExpansion:
    def test_lazarov_n_matches_nikolay_r(self, researcher_df):
        matcher = InitialAwareMatcher(researcher_df)
        result = matcher.match("Lazarov, N.")
        assert result.kind == "initial_expansion"
        assert any(c.person_id == "2570" for c in result.candidates)

    def test_lazarov_n_r_matches_nikolay_r(self, researcher_df):
        matcher = InitialAwareMatcher(researcher_df)
        result = matcher.match("Lazarov, N. R.")
        assert result.kind in ("exact", "initial_expansion")
        assert any(c.person_id == "2570" for c in result.candidates)

    def test_velyanov_v_matches_viktor_v(self, researcher_df):
        matcher = InitialAwareMatcher(researcher_df)
        result = matcher.match("Velyanov, V.")
        assert result.kind == "initial_expansion"
        assert any(c.person_id == "2548" for c in result.candidates)

    def test_velyanov_v_v_matches_viktor_v(self, researcher_df):
        matcher = InitialAwareMatcher(researcher_df)
        result = matcher.match("Velyanov, V. V.")
        assert result.kind in ("exact", "initial_expansion")
        assert any(c.person_id == "2548" for c in result.candidates)

    def test_no_false_positive_different_surname(self, researcher_df):
        matcher = InitialAwareMatcher(researcher_df)
        result = matcher.match("Ivanov, N.")
        # "Ivanov" not in master → new
        assert result.kind == "new"
        assert result.candidates == []

    def test_initial_mismatch_not_matched(self, researcher_df):
        matcher = InitialAwareMatcher(researcher_df)
        result = matcher.match("Lazarov, M.")
        # "M" does not match "Nikolay" (starts with N)
        assert result.kind == "new"

    def test_exact_match_preferred(self, researcher_df):
        # If the exact normalized form exists, kind should be "exact"
        matcher = InitialAwareMatcher(researcher_df)
        result = matcher.match("Tonchev, Anton B.")
        assert result.kind == "exact"
        assert result.candidates[0].person_id == "2406"

    def test_fuzzy_fallback_bratoeva(self, researcher_df):
        # "Kamelya" vs "Kameliya" — not initial match, should be fuzzy
        matcher = InitialAwareMatcher(researcher_df)
        result = matcher.match("Bratoeva, Kamelya")
        assert result.kind == "fuzzy"
        assert any(c.person_id == "2161" for c in result.candidates)


class TestSiblingGrouping:
    def test_lazarov_siblings(self):
        names = ["Lazarov, N. R.", "Lazarov, N.", "Tonchev, A. B.", "Tonchev, A."]
        groups = group_wos_authors(names)
        # Both Lazarov variants should be in the same group
        lazarov_group = next(v for v in groups.values() if any("Lazarov" in n for n in v))
        lazarov_names = set(lazarov_group)
        assert "Lazarov, N. R." in lazarov_names
        assert "Lazarov, N." in lazarov_names

    def test_velyanov_siblings(self):
        names = ["Velyanov, V.", "Velyanov, V. V.", "Bratoeva, Kamelya"]
        groups = group_wos_authors(names)
        velyanov_group = next(v for v in groups.values() if any("Velyanov" in n for n in v))
        assert "Velyanov, V." in velyanov_group
        assert "Velyanov, V. V." in velyanov_group

    def test_different_surnames_not_grouped(self):
        names = ["Lazarov, N.", "Ivanov, N.", "Petrov, N."]
        groups = group_wos_authors(names)
        # Each should be its own group (different surnames)
        assert len(groups) == 3

    def test_singleton_still_returned(self):
        names = ["Georgieva, Maria"]
        groups = group_wos_authors(names)
        assert len(groups) == 1
        assert "Georgieva, Maria" in list(groups.values())[0]


class TestClassifyWosAuthors:
    def test_batch_classification(self, researcher_df):
        names = [
            "Lazarov, N.",        # → initial_expansion → 2570
            "Velyanov, V. V.",    # → initial_expansion → 2548
            "Bratoeva, Kamelya",  # → fuzzy → 2161
            "Tonchev, Anton B.",  # → exact → 2406
            "Unknown, X.",        # → new
        ]
        results = classify_wos_authors(names, researcher_df)

        assert results["Lazarov, N."].kind == "initial_expansion"
        assert results["Velyanov, V. V."].kind in ("exact", "initial_expansion")
        assert results["Bratoeva, Kamelya"].kind == "fuzzy"
        assert results["Tonchev, Anton B."].kind == "exact"
        assert results["Unknown, X."].kind == "new"
