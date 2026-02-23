import re
from unidecode import unidecode

def normalize_name(name: str) -> str:
    name = unidecode(name or "")
    name = name.lower()
    name = re.sub(r"[^a-z ]", " ", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name