"""
scraper/collector.py — Core collection loop.

Rules enforced here:
  - EUR market only
  - Left Hand Drive only (STRICT — no RHD fallback, enforced in discovery)
  - Sample every 3rd production month: prods[::PROD_MONTH_STEP] → index 0, 3, 6, 9 …
  - Skip diesel models (filters.is_diesel)
  - Only collect brands in config.ALLOWED_BRANDS (detected from series label/value)

Output structure:
{
  "typecode#1 [1R11]": {
    "1. BMW E81 116i 1.6 EUR N43 20070300": {
      "type_code_full": "1R11-EUR-03-2007-E81-BMW-116i_1.6",
      "series_value":   "E81",
      "series_label":   "1' E81 (2006 — 2011)",
      "body":           "HC",
      "model":          "116i 1.6",
      "market":         "EUR",
      "engine":         "N43",
      "transmission":   "",
      "prod_month":     "20070300",
      "steering":       "Left hand drive",
      "brand":          "bmw",
      "catalog":        "Current"
    }
  }
}
"""

import logging

from config import CATALOG, ALLOWED_BRANDS, MARKET, PROD_MONTH_STEP
from scraper import discovery as disc
from scraper.filters import is_diesel, detect_brand
from storage.writer import save, get_all_known_type_codes
from storage import checkpoint as ckpt
from storage import db

logger = logging.getLogger(__name__)


def collect_all_type_codes(page, existing_data: dict) -> dict:
    """
    Main entry point. Enumerates all series on the RealOEM select page,
    filters by brand / market / steering / diesel rules, samples every 3rd
    production month, and collects type_code_full values.

    Parameters
    ----------
    page          : Playwright page object
    existing_data : dict loaded from disk at startup (resume support)

    Returns the fully populated output dict.
    """
    result           = dict(existing_data)
    known_type_codes = get_all_known_type_codes(result)

    # Load checkpoint for model/prod-level skipping on resume
    cp = ckpt.load()
    dm_set, dp_set = ckpt.build_sets(cp)

    # Rebuild prefix → group number from existing data so numbering is consistent
    prefix_to_num: dict[str, int] = {}
    for group_key in result:
        try:
            parts = group_key.split(" ")
            num   = int(parts[0].replace("typecode#", ""))
            pfx   = parts[1].strip("[]")
            prefix_to_num[pfx] = num
        except Exception:
            pass
    next_group_num = max(prefix_to_num.values(), default=0) + 1

    total_added = 0

    # ------------------------------------------------------------------
    # 1. Fetch all series for the current catalog
    # ------------------------------------------------------------------
    all_series = disc.get_all_series(page, CATALOG)
    if not all_series:
        logger.error(f"No series found for catalog={CATALOG}. Aborting.")
        return result

    # ------------------------------------------------------------------
    # 2. Filter series to allowed brands
    # ------------------------------------------------------------------
    series_list = []
    skipped     = []
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
        f"Processing {len(series_list)} series "
        f"(catalog={CATALOG}, brands={ALLOWED_BRANDS}, market={MARKET})"
    )

    # ------------------------------------------------------------------
    # 3. Main loop: series → bodies → models → prod samples → engines
    # ------------------------------------------------------------------
    for series_info in series_list:
        series_val   = series_info["value"]
        series_label = series_info["label"]
        brand        = series_info["brand"]
        brand_label  = brand.upper().replace("_", " ")   # "BMW" / "MINI" / "ROLLS ROYCE"

        logger.info(f"  [{brand_label}] Series: {series_label}  ({series_val})")

        bodies = disc.get_bodies(page, series_val, CATALOG)
        if not bodies:
            logger.warning(f"    No bodies for {series_val}")
            continue

        for body_info in bodies:
            body_val = body_info["value"]

            models = disc.get_models(page, series_val, body_val, CATALOG)
            if not models:
                continue

            for model_info in models:
                model_val = model_info["value"]

                # --- Diesel filter ---
                if is_diesel(model_val):
                    logger.debug(f"      Skip diesel: {model_val}")
                    continue

                # --- Checkpoint: skip fully-done model+market combos ---
                if (series_val, body_val, model_val, MARKET) in dm_set:
                    logger.info(
                        f"      Skip (checkpoint): {series_val}/{body_val}/{model_val}"
                    )
                    continue

                # --- EUR market availability check ---
                avail_markets = disc.get_markets(page, series_val, body_val, model_val, CATALOG)
                if MARKET not in avail_markets:
                    logger.debug(
                        f"      EUR not available for {series_val}/{body_val}/{model_val}"
                    )
                    continue

                # --- Get all prod months, then sample every 3rd ---
                all_prods = disc.get_prods(
                    page, series_val, body_val, model_val, MARKET, CATALOG
                )
                if not all_prods:
                    logger.warning(
                        f"      No prod dates for {series_val}/{model_val}/{MARKET}"
                    )
                    continue

                sampled = all_prods[::PROD_MONTH_STEP]
                logger.info(
                    f"      {series_val}/{body_val}/{model_val}: "
                    f"{len(all_prods)} prod months → sampling {len(sampled)} "
                    f"(every {PROD_MONTH_STEP}rd)"
                )

                for prod in sampled:
                    if (series_val, body_val, model_val, MARKET, prod) in dp_set:
                        logger.debug(f"        Skip prod (checkpoint): {prod}")
                        continue

                    engines = disc.get_engines(
                        page, series_val, body_val, model_val, MARKET, prod, CATALOG
                    )
                    if not engines:
                        continue

                    for engine in engines:
                        # Check for transmission dropdown (returns [""] if none)
                        transmissions = disc.get_transmissions(
                            page, series_val, body_val, model_val,
                            MARKET, prod, engine, CATALOG
                        )

                        for transmission in transmissions:
                            result_tc = disc.get_type_code_full(
                                page, series_val, body_val, model_val,
                                MARKET, prod, engine, CATALOG,
                                transmission=transmission,
                            )
                            if not result_tc:
                                # None = no LHD available → skip this car
                                continue

                            tc_full    = result_tc["type_code_full"]
                            steering   = result_tc.get("steering", "Left hand drive")
                            trans_label = result_tc.get("transmission", "")

                            if tc_full in known_type_codes:
                                logger.info(f"        Skip (already collected): {tc_full}")
                                continue

                            # --- Group by 4-char prefix ---
                            prefix = tc_full[:4]
                            if prefix not in prefix_to_num:
                                prefix_to_num[prefix] = next_group_num
                                next_group_num += 1

                            group_key = f"typecode#{prefix_to_num[prefix]} [{prefix}]"
                            if group_key not in result:
                                result[group_key] = {}

                            # Build the variant key (human-readable label)
                            variant_num  = len(result[group_key]) + 1
                            trans_suffix = f" {trans_label}" if trans_label else ""
                            variant_key  = (
                                f"{variant_num}. {brand_label} {series_val}"
                                f" {model_val} {MARKET} {engine}{trans_suffix} {prod}"
                            )

                            result[group_key][variant_key] = {
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

                            known_type_codes.add(tc_full)
                            total_added += 1
                            logger.info(
                                f"        + Added ({total_added} total): {tc_full}"
                                + (f"  [{trans_label}]" if trans_label else "")
                            )

                            # Insert into DB immediately — one row per car
                            db.upsert(result[group_key][variant_key])

                    # All engines for this prod done — mark prod complete
                    ckpt.mark_prod_done(
                        cp, dm_set, dp_set,
                        series_val, body_val, model_val, MARKET, prod
                    )

                # All sampled prods for this model done — save JSON + mark model complete
                save(result)
                ckpt.mark_model_market_done(
                    cp, dm_set,
                    series_val, body_val, model_val, MARKET
                )

    save(result)   # final flush
    logger.info(
        f"Collection complete — {total_added} new type codes added, "
        f"{len(result)} prefix groups total."
    )
    return result
