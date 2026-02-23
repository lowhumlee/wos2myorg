import pandas as pd
from rapidfuzz import fuzz
from rich.console import Console
from .utils import normalize_name

console = Console()

def build_registry(existing_csv):
    df = pd.read_csv(existing_csv)
    people = df[["PersonID", "AuthorFullName"]].drop_duplicates()
    people["norm"] = people["AuthorFullName"].apply(normalize_name)
    return people


def find_match(name, registry, threshold=0.85):
    norm = normalize_name(name)
    exact = registry[registry["norm"] == norm]
    if not exact.empty:
        return int(exact.iloc[0]["PersonID"])

    # fuzzy
    scores = registry.copy()
    scores["score"] = scores["norm"].apply(lambda x: fuzz.ratio(norm, x)/100)
    cand = scores[scores["score"] >= threshold].sort_values("score", ascending=False)

    if cand.empty:
        return None

    console.print(f"\nPossible matches for: [bold]{name}[/bold]")
    for i, r in cand.head(5).iterrows():
        console.print(f"{i}: {r['AuthorFullName']} (ID {r['PersonID']}, score {r['score']:.2f})")
    console.print("n: new person")
    choice = console.input("Select index or 'n': ")

    if choice.lower() == "n":
        return None
    try:
        return int(cand.loc[int(choice)]["PersonID"])
    except:
        return None