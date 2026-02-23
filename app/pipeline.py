import pandas as pd
from pathlib import Path
from datetime import datetime
from rich.console import Console

from .config import load_config
from .parser import parse_wos
from .matching import build_registry, find_match
from .org_select import choose_org

console = Console()


def run_pipeline(input_file, existing, orgs, config_file, out_dir):
    cfg = load_config(config_file)
    out_dir = Path(out_dir)
    out_dir.mkdir(exist_ok=True)

    parsed = parse_wos(input_file)
    patterns = [p.lower() for p in cfg["muv_patterns"]]

    parsed["is_muv"] = parsed["affiliation"].str.lower().apply(
        lambda x: any(p in x for p in patterns)
    )
    muv = parsed[parsed["is_muv"]].copy()

    registry = build_registry(existing)
    max_id = registry["PersonID"].max()

    rows = []
    new_people = []

    for _, r in muv.iterrows():
        name = r["author"]
        ut = r["UT"]

        pid = find_match(name, registry, cfg["fuzzy_threshold"])

        if pid is None:
            max_id += 1
            pid = int(max_id)
            org = choose_org(orgs, name)
            new_people.append({
                "PersonID": pid,
                "AuthorFullName": name,
                "OrganizationID": org
            })
        else:
            org = None

        rows.append({
            "PersonID": pid,
            "AuthorFullName": name,
            "UT": ut,
            "OrganizationID": org,
            "Timestamp": datetime.utcnow().isoformat()
        })

    out = pd.DataFrame(rows).drop_duplicates()
    out.to_csv(out_dir / "upload_ready.csv", index=False)

    if new_people:
        pd.DataFrame(new_people).to_csv(out_dir / "new_persons.csv", index=False)

    console.print("\nDone. Files written to output/")