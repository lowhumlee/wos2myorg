import re


def normalize_name(name: str) -> str:
    """
    Existing normalization logic.
    Keeps your original behavior intact.
    """
    if not name:
        return ""

    name = name.strip().lower()
    name = re.sub(r"\s+", " ", name)
    return name


# ==========================================================
# NEW LOGIC — INITIALS DETECTION
# ==========================================================

def is_initials_name(name: str) -> bool:
    """
    Detect names like:
    Lazarov, N.
    Lazarov, N. R.
    Velyanov, V V
    """
    if not name:
        return False

    parts = name.split(",")
    if len(parts) != 2:
        return False

    given = parts[1].strip()

    # Matches N. or N. R. or N R
    return bool(re.fullmatch(r"(?:[A-Z]\.?\s?)+", given))


def surname_initials_key(name: str) -> str:
    """
    Converts:
    Lazarov, Nikolay Rumenov -> lazarov_nr
    Lazarov, N. R.           -> lazarov_nr
    Lazarov, N               -> lazarov_n
    """
    if not name or "," not in name:
        return ""

    surname, given = name.split(",", 1)
    surname = surname.strip().lower()

    initials = []
    for part in given.strip().split():
        part = part.strip().replace(".", "")
        if part:
            initials.append(part[0].lower())

    return f"{surname}_{''.join(initials)}"


def surname_first_initial_key(name: str) -> str:
    """
    Used for grouping new authors.
    Lazarov, N. R. -> lazarov_n
    Lazarov, N.    -> lazarov_n
    """
    if not name or "," not in name:
        return ""

    surname, given = name.split(",", 1)
    surname = surname.strip().lower()

    first_initial = ""
    given_parts = given.strip().split()
    if given_parts:
        first_initial = given_parts[0][0].lower()

    return f"{surname}_{first_initial}"
