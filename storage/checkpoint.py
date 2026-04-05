"""
storage/checkpoint.py — Positional resume checkpoint.

Tracks two levels of completion so the scraper can skip ahead on restart:

  1. done_model_markets — (series, body, model, market) fully finished.
                          The entire model is skipped on resume.

  2. done_prods         — (series, body, model, market, prod) fully finished.
                          The production month is skipped on resume.

Checkpoint file is separate per scraper mode (current / classic) so both
scrapers can run independently without interfering with each other.
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
# Load / save
# ---------------------------------------------------------------------------

def load() -> dict:
    """Load checkpoint from disk. Returns an empty structure if not found."""
    path = _file()
    if not os.path.exists(path):
        logger.info("No checkpoint file — starting fresh positional tracking.")
        return {"done_model_markets": [], "done_prods": []}
    try:
        with open(path, encoding="utf-8") as f:
            cp = json.load(f)
        dm = len(cp.get("done_model_markets", []))
        dp = len(cp.get("done_prods", []))
        logger.info(f"Loaded checkpoint: {dm} completed models, {dp} completed prods")
        return cp
    except Exception as e:
        logger.warning(f"Could not load checkpoint ({e}) — starting fresh.")
        return {"done_model_markets": [], "done_prods": []}


def _save(cp: dict):
    path = _file()
    os.makedirs(_dir(), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cp, f, indent=2)
    os.replace(tmp, path)


# ---------------------------------------------------------------------------
# In-memory sets for O(1) lookups during the scrape loop
# ---------------------------------------------------------------------------

def build_sets(cp: dict) -> tuple[set, set]:
    """
    Return (done_model_markets_set, done_prods_set) built from the loaded checkpoint.
    """
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
# Mark complete
# ---------------------------------------------------------------------------

def mark_prod_done(cp: dict, dm_set: set, dp_set: set,
                   series: str, body: str, model: str,
                   market: str, prod: str):
    """Record that all engines for this prod month have been collected."""
    key = (series, body, model, market, prod)
    if key not in dp_set:
        dp_set.add(key)
        cp["done_prods"].append(
            {"series": series, "body": body, "model": model,
             "market": market, "prod": prod}
        )
        _save(cp)


def mark_model_market_done(cp: dict, dm_set: set,
                            series: str, body: str,
                            model: str, market: str):
    """Record that all prod months for this model+market have been collected."""
    key = (series, body, model, market)
    if key not in dm_set:
        dm_set.add(key)
        cp["done_model_markets"].append(
            {"series": series, "body": body,
             "model": model, "market": market}
        )
        _save(cp)
