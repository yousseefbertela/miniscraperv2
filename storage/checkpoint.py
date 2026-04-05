"""
storage/checkpoint.py — Positional resume checkpoint.

PRIMARY store : PostgreSQL scraper_checkpoints table (survives container restarts).
FALLBACK store: local JSON file (local dev / DB unavailable).

On DigitalOcean App Platform the container's filesystem is wiped on every restart.
Using the DB as primary store means the scraper resumes exactly where it left off
instead of re-navigating everything from scratch.

Two levels tracked:
  1. done_model_markets — (series, body, model, market) fully finished.
  2. done_prods         — (series, body, model, market, prod) finished.
"""

import json
import logging
import os

logger = logging.getLogger(__name__)


def _file():
    from config import CHECKPOINT_FILE
    return CHECKPOINT_FILE


def _dir():
    from config import OUTPUT_DIR
    return OUTPUT_DIR


# ---------------------------------------------------------------------------
# Load — DB first, local JSON fallback
# ---------------------------------------------------------------------------

def load() -> dict:
    """
    Load checkpoint. DB is tried first (persistent across DO restarts).
    Falls back to local JSON file. Returns empty structure if neither available.
    """
    from config import SCRAPER_MODE
    from storage import db

    # 1. Try DB
    cp = db.load_checkpoint(SCRAPER_MODE)
    if cp is not None:
        return cp

    # 2. Try local file
    path = _file()
    if os.path.exists(path):
        try:
            with open(path, encoding="utf-8") as f:
                cp = json.load(f)
            dm = len(cp.get("done_model_markets", []))
            dp = len(cp.get("done_prods", []))
            logger.info(f"Local checkpoint loaded: {dm} models done, {dp} prods done")
            return cp
        except Exception as e:
            logger.warning(f"Could not load local checkpoint ({e})")

    logger.info("No checkpoint found — starting fresh.")
    return {"done_model_markets": [], "done_prods": []}


# ---------------------------------------------------------------------------
# Local file save (backup — fast, secondary)
# ---------------------------------------------------------------------------

def _save_local(cp: dict):
    path = _file()
    os.makedirs(_dir(), exist_ok=True)
    tmp = path + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(cp, f, indent=2)
        os.replace(tmp, path)
    except Exception as e:
        logger.warning(f"Could not save local checkpoint: {e}")


# ---------------------------------------------------------------------------
# In-memory sets for O(1) lookups during the scrape loop
# ---------------------------------------------------------------------------

def build_sets(cp: dict) -> tuple[set, set]:
    """Return (done_model_markets_set, done_prods_set) from the loaded checkpoint."""
    dm = {
        (e["series"], e["body"], e["model"], e["market"])
        for e in cp.get("done_model_markets", [])
    }
    dp = {
        (e["series"], e["body"], e["model"], e["market"], e["prod"])
        for e in cp.get("done_prods", [])
    }
    return dm, dp


# ---------------------------------------------------------------------------
# Mark complete — DB first (survives restart), then local file backup
# ---------------------------------------------------------------------------

def mark_prod_done(cp: dict, dm_set: set, dp_set: set,
                   series: str, body: str, model: str,
                   market: str, prod: str):
    """Record that all engines for this prod month have been collected."""
    from config import SCRAPER_MODE
    from storage import db

    key = (series, body, model, market, prod)
    if key not in dp_set:
        dp_set.add(key)
        cp["done_prods"].append(
            {"series": series, "body": body, "model": model,
             "market": market, "prod": prod}
        )
        # DB first — survives container restart
        db.save_checkpoint_entry(SCRAPER_MODE, "prod",
                                  series, body, model, market, prod)
        # Local backup
        _save_local(cp)


def mark_model_market_done(cp: dict, dm_set: set,
                            series: str, body: str,
                            model: str, market: str):
    """Record that all prod months for this model+market have been collected."""
    from config import SCRAPER_MODE
    from storage import db

    key = (series, body, model, market)
    if key not in dm_set:
        dm_set.add(key)
        cp["done_model_markets"].append(
            {"series": series, "body": body,
             "model": model, "market": market}
        )
        # DB first
        db.save_checkpoint_entry(SCRAPER_MODE, "model_market",
                                  series, body, model, market)
        # Local backup
        _save_local(cp)
