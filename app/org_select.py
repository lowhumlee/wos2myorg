import pandas as pd
from rich.console import Console

console = Console()

def choose_org(org_file, person_name):
    orgs = pd.read_csv(org_file)
    console.print(f"\nAssign organization for {person_name}")
    term = console.input("Search term: ").lower()
    subset = orgs[orgs["OrganizationName"].str.lower().str.contains(term, na=False)]
    for i, r in subset.head(10).iterrows():
        console.print(f"{r['OrganizationID']}: {r['OrganizationName']}")
    choice = console.input("Enter OrganizationID: ")
    return int(choice)