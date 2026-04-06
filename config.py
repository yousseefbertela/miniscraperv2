"""
config.py — EUR-LHD Scraper (5/4)

Controlled entirely by the SCRAPER_MODE environment variable:
  SCRAPER_MODE=current  →  Scraper 1: EUR-LHD-CURRENT  (BMW + MINI + Rolls Royce)
  SCRAPER_MODE=classic  →  Scraper 2: EUR-LHD-CLASSIC  (BMW only)

Browser always runs headed (headless=False). On the server, Xvfb provides a
virtual display so headed Chrome can run without a physical screen.
This bypasses Cloudflare which blocks headless Chrome.

Set TEST_MODE=true to write output to test-data/ instead of output/.
"""

import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Load .env if present (local dev only — never commit .env to git)
# ---------------------------------------------------------------------------
_env = Path(__file__).parent / ".env"
if _env.exists():
    for _line in _env.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _, _v = _line.partition("=")
            os.environ.setdefault(_k.strip(), _v.strip())

# ---------------------------------------------------------------------------
# Runtime flags
# ---------------------------------------------------------------------------
SCRAPER_MODE = os.getenv("SCRAPER_MODE", "current").lower()   # "current" | "classic"
TEST_MODE    = os.getenv("TEST_MODE",    "false").lower() == "true"

# ---------------------------------------------------------------------------
# URLs
# ---------------------------------------------------------------------------
BMW_SELECT_URL = "https://www.realoem.com/bmw/enUS/select"

# ---------------------------------------------------------------------------
# Human-like delay ranges (seconds)
# ---------------------------------------------------------------------------
PAGE_LOAD_DELAY = (0.1, 0.2)
ACTION_DELAY    = (0.05, 0.1)
RETRY_DELAY     = (8, 20)
MAX_RETRIES     = 3

# ---------------------------------------------------------------------------
# Scrape rules (same for both scrapers)
# ---------------------------------------------------------------------------
MARKET          = "EUR"
PROD_MONTH_STEP = 6       # sample every 6th prod month: index 0, 6, 12 … + always last
STRICT_LHD      = True    # if steering dropdown exists and LHD not available → skip car

# ---------------------------------------------------------------------------
# Per-mode settings
# ---------------------------------------------------------------------------
_BASE_DIR = os.path.dirname(os.path.abspath(__file__))

if SCRAPER_MODE == "classic":
    CATALOG        = "Classic"
    ALLOWED_BRANDS = {"bmw"}                            # Classic = BMW only
    _OUT_NAME      = "EUR-LHD-CLASSIC.json"
    _CKPT_NAME     = "checkpoint-classic.json"
else:
    CATALOG        = "Current"
    ALLOWED_BRANDS = {"bmw", "mini", "rolls_royce"}     # Current = all 3 brands
    _OUT_NAME      = "EUR-LHD-CURRENT.json"
    _CKPT_NAME     = "checkpoint-current.json"

OUTPUT_DIR      = os.path.join(_BASE_DIR, "test-data" if TEST_MODE else "output")
OUTPUT_FILE     = os.path.join(OUTPUT_DIR, _OUT_NAME)
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, _CKPT_NAME)

# ---------------------------------------------------------------------------
# Database — DigitalOcean PostgreSQL
# ---------------------------------------------------------------------------
DB_HOST     = os.getenv("DB_HOST",     "")
DB_PORT     = os.getenv("DB_PORT",     "25060")
DB_NAME     = os.getenv("DB_NAME",     "defaultdb")
DB_USER     = os.getenv("DB_USER",     "")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_SSLMODE  = os.getenv("DB_SSLMODE",  "require")
