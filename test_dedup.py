
import csv
import io
from core import build_person_index, extract_muv_author_pairs, batch_process, DEFAULT_CONFIG

# 1. Setup Mock Data
researcher_csv = """PersonID,FirstName,LastName,OrganizationID,DocumentID
100,Nikolay,Lazarov,ORG1,WOS:0001
100,Nikolay,Lazarov,ORG1,WOS:0002
101,Viktor,Velyanov,ORG2,WOS:0003
"""

# WoS data with:
# - A record that already exists (Lazarov, WOS:0001)
# - A duplicate within the same file (Velyanov, WOS:0004 twice)
# - A new record (Lazarov, WOS:0005)
wos_records = [
    {"UT": "WOS:0001", "C1": "[Lazarov, N.] Medical University Varna, Bulgaria"},
    {"UT": "WOS:0004", "C1": "[Velyanov, V.] Medical University Varna, Bulgaria"},
    {"UT": "WOS:0004", "C1": "[Velyanov, V.] Medical University Varna, Bulgaria"}, # Intra-batch duplicate
    {"UT": "WOS:0005", "C1": "[Lazarov, N.] Medical University Varna, Bulgaria"},
]

def test_deduplication():
    print("--- Testing Deduplication Logic ---")
    
    # Parse existing persons
    person_index, max_pid, existing_pairs = build_person_index(researcher_csv)
    print(f"Existing pairs: {existing_pairs}")
    
    # Extract pairs from WoS (should handle intra-file duplicates)
    muv_pairs = extract_muv_author_pairs(wos_records, DEFAULT_CONFIG)
    print(f"Extracted pairs (after extraction dedup): {len(muv_pairs)}")
    for p in muv_pairs:
        print(f"  {p['author_full']} - {p['UT']}")

    # Process batch
    result = batch_process(
        muv_pairs, person_index, {}, DEFAULT_CONFIG, 
        start_pid=max_pid+1, 
        researcher_csv_content=researcher_csv,
        existing_pairs=existing_pairs
    )
    
    confirmed = result['confirmed']
    needs_review = result['needs_review']
    already_uploaded = result['already_uploaded']
    
    print(f"\nNeeds Review: {len(needs_review)}")
    for r in needs_review:
        print(f"  {r['AuthorFullName']} (Type: {r['match_type']}) - {r['UT']}")
    
    print(f"\nConfirmed for upload: {len(confirmed)}")
    for c in confirmed:
        print(f"  {c['AuthorFullName']} (PID {c['PersonID']}) - {c['UT']}")
        
    print(f"\nAlready uploaded / Skipped: {len(already_uploaded)}")
    for a in already_uploaded:
        print(f"  {a['AuthorFullName']} (PID {a['PersonID']}) - {a['UT']} - Reason: {a['Reason']}")

    # Assertions
    # 1. Lazarov WOS:0001 should be in already_uploaded (Reason: Already in MyOrg)
    # 2. Velyanov WOS:0004 should be in needs_review ONCE (initial_expansion)
    # 3. Lazarov WOS:0005 should be in needs_review (initial_expansion)
    
    lazarov_0001_skipped = any(a['UT'] == 'WOS:0001' and a['PersonID'] == '100' for a in already_uploaded)
    velyanov_0004_review = [r for r in needs_review if r['UT'] == 'WOS:0004' and r['suggested_pid'] == '101']
    lazarov_0005_review = [r for r in needs_review if r['UT'] == 'WOS:0005' and r['suggested_pid'] == '100']
    
    print("\n--- Validation Results ---")
    print(f"Lazarov WOS:0001 skipped: {lazarov_0001_skipped}")
    print(f"Velyanov WOS:0004 review count (expect 1): {len(velyanov_0004_review)}")
    print(f"Lazarov WOS:0005 review count (expect 1): {len(lazarov_0005_review)}")
    
    if lazarov_0001_skipped and len(velyanov_0004_review) == 1 and len(lazarov_0005_review) == 1:
        print("\n✅ SUCCESS: Deduplication logic is working correctly.")
    else:
        print("\n❌ FAILURE: Deduplication logic issues detected.")

if __name__ == "__main__":
    test_deduplication()
