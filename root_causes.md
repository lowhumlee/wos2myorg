# Root Cause Analysis: Duplicate Detection Failures

The following logical errors were identified in the `wos2myorg` application during the code audit:

## 1. Missing `existing_pairs` in `batch_process` Call
In `cli.py`, the `run_batch` function calls `batch_process` but fails to pass the `existing_pairs` set returned by `build_person_index`.
- **Location:** `cli.py`, line 417.
- **Impact:** The system cannot detect if a (PersonID, DocumentID) already exists in `ResearcherAndDocument.csv` during batch mode, leading to redundant records in the output.

## 2. Incomplete Deduplication in `extract_muv_author_pairs`
The function `extract_muv_author_pairs` extracts (Author, Affiliation, UT) tuples but does not perform any deduplication.
- **Location:** `core.py`, line 204.
- **Impact:** If the same author-document pair appears multiple times in the input WoS files (common in 2024 repeat rows), they are all added to the extraction list.

## 3. Lack of Intra-Batch Deduplication in `batch_process`
Inside `batch_process`, while unique authors are grouped, the individual `pairs` for each author are processed without checking if the same (PersonID, UT) has already been added to `confirmed` or `needs_review` within the *same* run.
- **Location:** `core.py`, lines 442 and 498.
- **Impact:** Duplicate rows within the same batch (or across files in the same run) are not filtered out.

## 4. Inconsistent UT Usage
The code uses both `ut` and `UT` as keys in dictionaries. While mostly handled, normalization of these values (stripping, case-consistency) is not explicitly enforced during comparison against `existing_pairs`.

## 5. PersonID Generation Risk
New PersonIDs are generated using a counter starting from `max_pid + 1`. While correct for a single run, if multiple runs are performed without updating the master `ResearcherAndDocument.csv`, the same "new" person might be assigned different IDs or conflict with other runs. (Note: The prompt asks to preserve design, but this is a robustness point).
