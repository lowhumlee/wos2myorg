# WoS My Organization — MUV Affiliation Ingestion Tool

> **Medical University of Varna · Research Information Systems**  
> Detect new MUV-affiliated authors in Web of Science exports and generate validated, upload-ready entries for WoS My Organization.

---

## Overview

This tool automates the most tedious part of WoS My Organization curation:

1. **Parses** WoS export CSVs and extracts author-affiliation mappings from the `C1` field
2. **Identifies** authors affiliated with Medical University of Varna (MUV) using a configurable pattern dictionary with fuzzy matching
3. **Compares** detected authors against your existing `ResearcherAndDocument.csv` baseline
4. **Flags** new and ambiguous persons for human review
5. **Generates** upload-ready CSV, Excel review sheets, and a full audit log

### Architecture

```
core.py          — Shared processing engine (parsing, matching, output generation)
cli.py           — Command-line interface (interactive + batch modes)
app.py           — Streamlit desktop GUI
config.json      — Configurable patterns, thresholds, and settings
```

---

## Installation

### Requirements
- Python 3.9 or newer
- pip

### Steps

```bash
# 1. Clone or copy the tool files to a directory
cd wos_muv_tool/

# 2. Install dependencies
pip install -r requirements.txt

# 3. Verify installation
python cli.py --help
```

**Minimum required packages:**
- `openpyxl` — Excel review sheets
- `streamlit` — GUI mode
- `pandas` — GUI data previews
- `rich` — Pretty CLI output (optional but recommended)

---

## Input Files

### 1. WoS Export (`input*.csv`)

Export from Web of Science → **Save to Other File Formats → Tab-delimited (Win, UTF-8)**.  
Open in Excel, save as CSV UTF-8, then provide to this tool.

Required columns:
| Column | Description |
|--------|-------------|
| `AF`   | Author full names, semicolon-separated |
| `C1`   | Author-affiliation mapping with `[Author] Affiliation` syntax |
| `UT`   | WoS accession number (e.g., `WOS:001234567800001`) |

Optional: `AU`, `C3`, `OI` (ORCID)

### 2. `ResearcherAndDocument.csv`

Download from WoS My Organization → Export (Researchers + Documents).

Required columns: `PersonID`, `FirstName`, `LastName`, `OrganizationID`, `DocumentID`

### 3. `OrganizationHierarchy.csv`

Download from WoS My Organization → Settings → Organization Structure.

Required columns: `OrganizationID`, `OrganizationName`, `ParentOrgaID`

---

## Running the Tool

### Option A — Streamlit GUI (recommended for library workflows)

```bash
streamlit run app.py
```

Opens a browser at `http://localhost:8501`.

**Workflow:**
1. **Tab 1 — Load Files**: Upload all three input files → click "Detect MUV Authors"
2. **Tab 2 — Review & Resolve**: Verify fuzzy matches, assign organizations, approve/reject
3. **Tab 3 — Export Output**: Download upload-ready CSV, review Excel, audit log
4. **Tab 4 — Statistics**: Visualize MUV author distribution and affiliation strings

---

### Option B — CLI Interactive Mode

Prompts for each ambiguous case in real time. Best for small/medium exports.

```bash
python cli.py input100.csv \
    --researcher ResearcherAndDocument.csv \
    --orgs OrganizationHierarchy.csv \
    --config config.json \
    --mode interactive \
    --out output/
```

**What happens:**
- Exact matches are confirmed automatically
- Fuzzy matches prompt: `Choose: 1. Ivanov, Ivan (9876)  2. Ivanov, I. (9877)  3. NEW`
- New persons prompt for organization selection with keyword search
- All decisions logged to `output/staging.db`

---

### Option C — CLI Batch Mode

Auto-confirms exact matches, exports everything else to an Excel review sheet.  
Best for large exports or team-based curation workflows.

```bash
# Step 1: Run batch (auto-confirms exact matches, exports review Excel)
python cli.py input100.csv --mode batch --out output/

# Step 2: Library staff fill in output/review_*.xlsx
#   - Set OrganizationID for each new person
#   - Change APPROVED to NO to reject
#   - Verify/correct PersonID for fuzzy matches

# Step 3: Re-import filled Excel
python cli.py input100.csv --mode batch \
    --reimport output/review_20241201_143022.xlsx \
    --confirmed-csv output/upload_ready_20241201_143022.csv \
    --out output/
```

---

## Configuration (`config.json`)

```json
{
  "muv_affiliation_patterns": [
    "medical university varna",
    "med univ varna",
    "mu varna",
    "medical university of varna",
    "муварна"
  ],
  "fuzzy_threshold": 0.85,
  "interactive_mode": true,
  "allow_multi_org": true,
  "new_person_id_start": 9000,
  "output_dir": "output",
  "db_path": "staging.db"
}
```

| Setting | Description | Default |
|---------|-------------|---------|
| `muv_affiliation_patterns` | Substrings that identify MUV in C1 field (case-insensitive, diacritics stripped) | See config |
| `fuzzy_threshold` | Minimum name similarity (0–1) to trigger review | `0.85` |
| `interactive_mode` | Prompt for decisions in CLI | `true` |
| `allow_multi_org` | Allow assigning >1 org per author | `true` |
| `new_person_id_start` | First ID to use for new persons (will auto-increment above max existing) | `9000` |
| `output_dir` | Where to write output files | `"output"` |
| `db_path` | SQLite staging database filename (inside output_dir) | `"staging.db"` |

---

## Output Files

| File | Description |
|------|-------------|
| `output/upload_ready_TIMESTAMP.csv` | **Import this into WoS My Organization** |
| `output/review_TIMESTAMP.xlsx` | Review sheet for human curation (batch mode) |
| `output/audit_TIMESTAMP.json` | Full audit trail |
| `output/staging.db` | SQLite database (persons, affiliations, decisions, rejected) |
| `output/upload_ready_merged_TIMESTAMP.csv` | Merged CSV after review Excel re-import |

### Upload-Ready CSV columns

| Column | Description |
|--------|-------------|
| `PersonID` | WoS My Organization PersonID |
| `AuthorFullName` | Canonical full name (Last, First) |
| `UT` | WoS document accession number |
| `OrganizationID` | Assigned MUV unit ID |
| `SourceFile` | Source WoS export filename |
| `Timestamp` | Processing timestamp |

---

## How Matching Works

### C1 Field Parsing

WoS C1 field format:
```
[Author1; Author2] Affiliation1; [Author3] Affiliation2
```

The tool:
1. Extracts each `[author list] affiliation` block using regex
2. Matches C1 author names to canonical AF names using sequence similarity
3. Builds a many-to-many author ↔ affiliation mapping

### MUV Detection

An affiliation is flagged as MUV if any configured pattern is found as a substring (after stripping diacritics, lowercasing, collapsing whitespace).

Example: `"Med Univ Varna, Dept Physiol & Pathophysiol, Varna, Bulgaria"` → ✅ matches `"med univ varna"`

### Name Normalization

```
"Bratoeva, Kamelya"  →  strip diacritics  →  lowercase  →  remove punctuation
→  "bratoeva kamelya"  →  comparison key
```

### Fuzzy Matching

Uses Python's `difflib.SequenceMatcher` (Levenshtein-like ratio). If `rapidfuzz` is installed, it is used automatically as a drop-in for much better performance.

| Similarity | Outcome |
|-----------|---------|
| 1.00 | Exact match → auto-confirmed |
| ≥ threshold (default 0.85) | Fuzzy match → human review |
| < threshold | New person → staged |

---

## Edge Cases Handled

- **Multiple UT per author** — each document processed separately with deduplication
- **Same author in multiple C1 blocks** — all MUV affiliations collected
- **Empty C1 fields** — record skipped with log entry
- **Cyrillic/transliteration variants** — diacritic stripping and fuzzy matching
- **Author appears multiple times in same record** — deduplicated by `(PersonID, UT, OrgID)` composite key
- **Different first name formats** (`Kamelya` vs `Kameliya`) — caught by fuzzy threshold
- **Broken exports** — records without UT or AF are skipped with warning

---

## Deduplication

Output rows are deduplicated on composite key `(PersonID, UT, OrganizationID)`.  
Duplicate attempts are logged to the audit trail and staging DB.

---

## Audit Trail

Every decision is logged:
```json
{
  "generated_at": "2024-12-01T14:30:22",
  "summary": {
    "exact_matches": 5,
    "new_persons": 2,
    "finalized_records": 8,
    "rejected_records": 0
  },
  "new_persons": [
    {"PersonID": "9001", "Name": "Marinov, Simeon P."}
  ]
}
```

---

## Validation Test (using provided sample files)

```bash
python cli.py imput100.csv \
    --researcher ResearcherAndDocument.csv \
    --orgs OrganizationHierarchy.csv \
    --mode batch --out output_test/
```

**Expected results for `WOS:001666195400001`:**

| Author | Expected outcome |
|--------|-----------------|
| Velyanov, Viktor V. | ✅ Exact match → PersonID 2548 |
| Lazarov, Nikola R. | ✅ Exact match → PersonID 2570 |
| Bratoeva, Kamelya | ⚠ Fuzzy match → compare with Kameliya Bratoeva (2161) |
| Tonchev, Anton B. | ✅ Exact match → PersonID 2406 |
| Evtimov, Nikolai T. | ⚠ Fuzzy match → compare with Nikolay Evtimov (2556) |
| Lubomirov, Lubomir T. | ✅ Exact match → PersonID 2572 |

---

## Future Enhancements

- **ORCID enrichment**: If `OI` column present, use as primary identity key
- **ROR matching**: Map raw affiliations to Research Organization Registry IDs  
- **CERIF/CRIS export**: Generate CERIF-compatible XML output
- **rapidfuzz integration**: Auto-detected for 10–100× faster fuzzy matching
- **Incremental runs**: SQLite staging DB already supports this; add `--incremental` flag

---

## License

MIT License — Medical University of Varna, Research Information Systems  
Built for WoS My Organization curation workflows.
