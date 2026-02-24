from collections import defaultdict

from .normalize import (
    normalize_name,
    is_initials_name,
    surname_initials_key,
    surname_first_initial_key,
)


# ==========================================================
# BUILD PERSON INDEX
# ==========================================================

def build_person_index(persons):
    """
    Builds:
    1) Normal index
    2) Initials-only index
    """
    person_index = defaultdict(list)
    person_index_initials = defaultdict(list)

    for p in persons:
        full_name = p.get("AuthorFullName", "")

        norm = normalize_name(full_name)
        person_index[norm].append(p)

        initials_key = surname_initials_key(full_name)
        if initials_key:
            person_index_initials[initials_key].append(p)

    return person_index, person_index_initials


# ==========================================================
# MAIN BATCH PROCESS
# ==========================================================

def batch_process(persons, wos_author_pairs):
    """
    persons: list of researcher records from ResearcherAndDocument.csv
    wos_author_pairs: parsed WoS author records
    """

    person_index, person_index_initials = build_person_index(persons)

    matched = []
    needs_review = []
    new_author_groups = defaultdict(list)

    for pair in wos_author_pairs:
        author_full = pair["author"]
        norm = normalize_name(author_full)

        # ==================================================
        # INITIALS-ONLY MATCHING RESTRICTION
        # ==================================================
        if is_initials_name(author_full):
            key = surname_initials_key(author_full)
            candidates = person_index_initials.get(key, [])
        else:
            candidates = person_index.get(norm, [])

        # ==================================================
        # EXACT MATCH
        # ==================================================
        if len(candidates) == 1:
            person = candidates[0]

            matched.append({
                "PersonID": person.get("PersonID", ""),
                "AuthorFullName": author_full,
                "UT": pair["ut"],
                "OrganizationID": person.get("OrganizationID", ""),
                "match_type": "exact",
            })

            continue

        # ==================================================
        # MULTIPLE CANDIDATES → REVIEW
        # ==================================================
        if len(candidates) > 1:
            top = candidates[0]

            needs_review.append({
                "PersonID": "",
                "AuthorFullName": author_full,
                "UT": pair["ut"],
                "match_type": "multiple",
                "candidates": candidates,
                "suggested_pid": top.get("PersonID", ""),
                "suggested_name": top.get("AuthorFullName", ""),
                "OrganizationID": top.get("OrganizationID", ""),
            })
            continue

        # ==================================================
        # NO MATCH → GROUP NEW AUTHORS
        # ==================================================
        group_key = surname_first_initial_key(author_full)

        new_author_groups[group_key].append({
            "PersonID": "",
            "AuthorFullName": author_full,
            "UT": pair["ut"],
            "match_type": "new",
            "OrganizationID": "",
            "group_key": group_key,
        })

    # ======================================================
    # FLATTEN GROUPED NEW AUTHORS
    # ======================================================
    for group in new_author_groups.values():

        if len(group) == 1:
            needs_review.append(group[0])
        else:
            base = group[0]
            variants = sorted(set(x["AuthorFullName"] for x in group))
            base["GroupedVariants"] = "; ".join(variants)
            needs_review.append(base)

    return matched, needs_review
