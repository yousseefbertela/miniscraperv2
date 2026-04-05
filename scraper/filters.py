"""
scraper/filters.py — Model-name diesel filter + brand detection from series label.
"""

import re

# ---------------------------------------------------------------------------
# Diesel detection
# ---------------------------------------------------------------------------
# Covers all diesel naming patterns found on RealOEM for BMW / MINI / Rolls Royce:
#
#   (?<=\d)d[xX]?$    — digit + d (optionally dX/dx):  320d, 18d, 18dX, 20dX
#   (?<=\d)d\s+ed\b   — diesel edition:                 20d ed, 318d ed
#   xd$               — xDrive diesel suffix:           320xd
#   td$               — touring diesel:                 some older models
#   \bsdi?\b          — SDi / SDI
#   \btdi\b           — TDi (MINI)
#   \bcdti\b          — CDTi

_DIESEL_RE = re.compile(
    r"(?<=\d)d[xX]?$"
    r"|(?<=\d)d\s+ed\b"
    r"|xd$"
    r"|td$"
    r"|\bsdi?\b"
    r"|\btdi\b"
    r"|\bcdti\b",
    re.IGNORECASE,
)


def is_diesel(model_name: str) -> bool:
    """Return True if the model name indicates a diesel engine."""
    return bool(_DIESEL_RE.search(model_name.strip()))


# ---------------------------------------------------------------------------
# Brand detection from series label / value
# ---------------------------------------------------------------------------
# RealOEM serves all three brands from the same bmw/enUS/select page.
# We detect the brand from the series label and series value:
#   • "MINI" in label                          → mini
#   • Series value starts with "RR"            → rolls_royce
#   • Label contains any Rolls Royce model name → rolls_royce
#   • Everything else                          → bmw

_RR_KEYWORDS = {
    "PHANTOM", "GHOST", "WRAITH", "CULLINAN", "DAWN",
    "SILVER SERAPH", "ROLLS", "SILVER SPUR",
}


def detect_brand(series_label: str, series_value: str) -> str:
    """
    Detect brand from the series label and value shown in the RealOEM dropdown.

    Returns: "bmw" | "mini" | "rolls_royce"
    """
    label_up = series_label.upper()
    value_up = series_value.upper()

    if "MINI" in label_up:
        return "mini"

    if value_up.startswith("RR"):
        return "rolls_royce"

    if any(kw in label_up for kw in _RR_KEYWORDS):
        return "rolls_royce"

    return "bmw"
