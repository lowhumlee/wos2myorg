import re
import pandas as pd

C1_PATTERN = re.compile(r"\[(.*?)\]\s*([^\[]+?)(?=;\s*\[|$)")

def parse_wos(input_file):
    df = pd.read_csv(input_file)
    rows = []
    for _, r in df.iterrows():
        ut = r.get("UT")
        c1 = r.get("C1", "")
        for authors, aff in C1_PATTERN.findall(str(c1)):
            for a in authors.split(";"):
                rows.append({
                    "author": a.strip(),
                    "affiliation": aff.strip(),
                    "UT": ut
                })
    return pd.DataFrame(rows)