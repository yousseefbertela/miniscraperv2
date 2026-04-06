"""
scraper/collector.py — Core collection loop.

Rules enforced:
  - EUR market only
  - Left Hand Drive only (STRICT — no RHD fallback)
  - Sample every 3rd production month: prods[::PROD_MONTH_STEP]
  - Skip diesel models
  - Only collect brands in config.ALLOWED_BRANDS

Threading design:
  Two parallel Chrome browsers are launched by the main entry point.
  Each thread calls collect_series_slice() with its own page and a disjoint
  slice of the series list. All shared output state is protected by a Lock.

  SharedState holds: result dict, known_type_codes set, prefix_to_num map,
  next_group_num counter, and the threading.Lock.

Speed per model (2 navs/model instead of the old 78):
  get_prods_eur(1) + per prod: get_cars_for_prod(2) = 1 + N*2 navs/model.

Output structure:
{
  "typecode#1 [1R11]": {
    "1. BMW E81 116i 1.6 EUR N43 20070300": {
      "type_code_full": "1R11-EUR-03-2007-E81-BMW-116i_1.6",
      ...
    }
  }
}
"""

import logging
import threading

from config import CATALOG, ALLOWED_BRANDS, MARKET, PROD_MONTH_STEP
from scraper import discovery as disc
from scraper.filters import is_diesel, detect_brand
from storage.writer import save
from storage import checkpoint as ckpt
from storage import db

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Shared state — thread-safe container for output data
# ---------------------------------------------------------------------------

class SharedState:
    """
    Holds all mutable output state shared between the two parallel browsers.
    All writes are protected by self.lock.
    """
    def __init__(self, result: dict, known_type_codes: set):
        self.lock = threading.Lock()
        self.result = result
        self.known_type_codes = known_type_codes
        self.total_added = 0

        # Rebuild prefix → group number from existing data
        self.prefix_to_num: dict[str, int] = {}
        for group_key in result:
            try:
                parts = group_key.split(" ")
                num   = int(parts[0].replace("typecode#", ""))
                pfx   = parts[1].strip("[]")
                self.prefix_to_num[pfx] = num
            except Exception:
                pass
        self.next_group_num = max(self.prefix_to_num.values(), default=0) + 1


# ---------------------------------------------------------------------------
# Phase 1 — fetch and filter series list (called once before threads launch)
# ---------------------------------------------------------------------------

def get_filtered_series(page) -> list:
    """
    Enumerate all series for the current catalog and filter by ALLOWED_BRANDS.
    Returns list of {value, label, brand} dicts.
    Called once in the main thread before the two worker threads start.
    """
    all_series = disc.get_all_series(page, CATALOG)
    if not all_series:
        logger.error(f"No series found for catalog={CATALOG}.")
        return []

    series_list, skipped = [], []
    for s in all_series:
        brand = detect_brand(s["label"], s["value"])
        if brand in ALLOWED_BRANDS:
            series_list.append({**s, "brand": brand})
        else:
            skipped.append(s["label"])

    if skipped:
        logger.info(
            f"Skipped {len(skipped)} series not in ALLOWED_BRANDS {ALLOWED_BRANDS}: "
            f"{skipped[:5]}{'…' if len(skipped) > 5 else ''}"
        )
    logger.info(
        f"{len(series_list)} series to process "
        f"(catalog={CATALOG}, brands={ALLOWED_BRANDS}, market={MARKET})"
    )
    return series_list


# ---------------------------------------------------------------------------
# Phase 2 — collect one slice of series (runs in a dedicated thread)
# ---------------------------------------------------------------------------

def collect_series_slice(page, series_slice: list, state: SharedState,
                          thread_id: int = 1):
    """
    Process a disjoint slice of series using the given browser page.
    Thread-safe: all writes to shared state go through state.lock.
    DB writes use thread-local connections — no locking needed there.
    """
    tag = f"[T{thread_id}]"

    # Each thread loads its own checkpoint view from DB
    cp = ckpt.load()
    dm_set, dp_set = ckpt.build_sets(cp)

    for series_info in series_slice:
        series_val   = series_info["value"]
        series_label = series_info["label"]
        brand        = series_info["brand"]
        brand_label  = brand.upper().replace("_", " ")

        logger.info(f"{tag} [{brand_label}] Series: {series_label}  ({series_val})")

        bodies = disc.get_bodies(page, series_val, CATALOG)
        if not bodies:
            logger.warning(f"{tag} No bodies for {series_val}")
            continue

        for body_info in bodies:
            body_val = body_info["value"]

            models = disc.get_models(page, series_val, body_val, CATALOG)
            if not models:
                continue

            for model_info in models:
                model_val = model_info["value"]

                if is_diesel(model_val):
                    logger.debug(f"{tag} Skip diesel: {model_val}")
                    continue

                if (series_val, body_val, model_val, MARKET) in dm_set:
                    logger.info(
                        f"{tag} Skip (checkpoint): {series_val}/{body_val}/{model_val}"
                    )
                    continue

                all_prods = disc.get_prods_eur(
                    page, series_val, body_val, model_val, CATALOG
                )
                if not all_prods:
                    logger.debug(
                        f"{tag} EUR not available or no prods: "
                        f"{series_val}/{body_val}/{model_val}"
                    )
                    # Checkpoint as done so restarts don't re-navigate it
                    ckpt.mark_model_market_done(
                        cp, dm_set, series_val, body_val, model_val, MARKET
                    )
                    continue

                sampled = all_prods[::PROD_MONTH_STEP]
                # Always include the very last prod month (most recent)
                # in case it wasn't naturally hit by the step
                if all_prods[-1] not in sampled:
                    sampled = sampled + [all_prods[-1]]
                logger.info(
                    f"{tag} {series_val}/{body_val}/{model_val}: "
                    f"{len(all_prods)} prod months → sampling {len(sampled)} "
                    f"(every {PROD_MONTH_STEP}th + last)"
                )

                for prod in sampled:
                    if (series_val, body_val, model_val, MARKET, prod) in dp_set:
                        logger.debug(f"{tag} Skip prod (checkpoint): {prod}")
                        continue

                    cars = disc.get_cars_for_prod(
                        page, series_val, body_val, model_val, prod, CATALOG
                    )

                    for car in cars:
                        tc_full     = car["type_code_full"]
                        engine      = car["engine"]
                        steering    = car["steering"]
                        trans_label = car["transmission"]

                        # ---- Thread-safe: check + add to shared state ----
                        record = None
                        group_key = None
                        with state.lock:
                            if tc_full in state.known_type_codes:
                                logger.info(f"{tag} Skip (already known): {tc_full}")
                                continue

                            prefix = tc_full[:4]
                            if prefix not in state.prefix_to_num:
                                state.prefix_to_num[prefix] = state.next_group_num
                                state.next_group_num += 1

                            group_key = f"typecode#{state.prefix_to_num[prefix]} [{prefix}]"
                            if group_key not in state.result:
                                state.result[group_key] = {}

                            variant_num  = len(state.result[group_key]) + 1
                            trans_suffix = f" {trans_label}" if trans_label else ""
                            variant_key  = (
                                f"{variant_num}. {brand_label} {series_val}"
                                f" {model_val} {MARKET} {engine}{trans_suffix} {prod}"
                            )

                            record = {
                                "type_code_full": tc_full,
                                "series_value":   series_val,
                                "series_label":   series_label,
                                "body":           body_val,
                                "model":          model_val,
                                "market":         MARKET,
                                "engine":         engine,
                                "transmission":   trans_label,
                                "prod_month":     prod,
                                "steering":       steering,
                                "brand":          brand,
                                "catalog":        CATALOG,
                            }
                            state.result[group_key][variant_key] = record
                            state.known_type_codes.add(tc_full)
                            state.total_added += 1

                            logger.info(
                                f"{tag} + Added ({state.total_added} total): {tc_full}"
                                + (f"  [{trans_label}]" if trans_label else "")
                            )
                        # ---- End of lock ----

                        # DB upsert outside lock (thread-local connection)
                        if record:
                            db.upsert(record)

                    ckpt.mark_prod_done(
                        cp, dm_set, dp_set,
                        series_val, body_val, model_val, MARKET, prod
                    )

                # All sampled prods for this model done — save JSON
                with state.lock:
                    save(state.result)
                ckpt.mark_model_market_done(
                    cp, dm_set,
                    series_val, body_val, model_val, MARKET
                )

    with state.lock:
        logger.info(
            f"{tag} Slice complete — {state.total_added} total type codes so far."
        )


# ---------------------------------------------------------------------------
# Single-thread entry point (kept for backward compat / local testing)
# ---------------------------------------------------------------------------

def collect_all_type_codes(page, existing_data: dict) -> dict:
    """
    Single-browser entry point. Used for local test runs.
    For production (DO deployment) the main_*.py files launch two parallel
    browsers and call collect_series_slice() directly.
    """
    from config import SCRAPER_MODE
    from storage.writer import get_all_known_type_codes

    # Known type codes: DB first (survives restart), then JSON
    known = db.get_known_type_codes(SCRAPER_MODE)
    if not known:
        known = get_all_known_type_codes(existing_data)

    state = SharedState(dict(existing_data), known)

    series_list = get_filtered_series(page)
    if not series_list:
        return state.result

    collect_series_slice(page, series_list, state, thread_id=1)

    save(state.result)
    total = sum(len(v) for v in state.result.values())
    logger.info(
        f"Collection complete — {state.total_added} new type codes added, "
        f"{total} total across {len(state.result)} prefix groups."
    )
    return state.result
