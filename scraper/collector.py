"""
scraper/collector.py — Core collection loop.

Rules enforced:
  - EUR market only
  - Left Hand Drive only (STRICT — no RHD fallback)
  - Sample every 3rd production month: prods[::PROD_MONTH_STEP]
  - Skip diesel models
  - Only collect brands in config.ALLOWED_BRANDS

Speed design — navigations per model:
  Old: get_markets(1) + get_prods(1) + per prod: get_engines(1) + get_transmissions(1)
       + type_code attempt(1) + type_code steering retry(1) = 2 + 19*4 = 78 navs/model
  New: get_prods_eur(1) + per prod: get_cars_for_prod(2) = 1 + 19*2 = 39 navs/model
  Saving ~39 navs × 9s = ~5 min per model.

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

from config import CATALOG, ALLOWED_BRANDS, MARKET, PROD_MONTH_STEP
from scraper import discovery as disc
from scraper.filters import is_diesel, detect_brand
from storage.writer import save, get_all_known_type_codes
from storage import checkpoint as ckpt
from storage import db

logger = logging.getLogger(__name__)


def collect_all_type_codes(page, existing_data: dict) -> dict:
    """
    Main entry point. Enumerates all series, filters by brand / diesel / LHD,
    samples every 3rd prod month, and collects type_code_full values.
    """
    result           = dict(existing_data)
    known_type_codes = get_all_known_type_codes(result)

    cp = ckpt.load()
    dm_set, dp_set = ckpt.build_sets(cp)

    # Rebuild prefix → group number from existing data
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
    # 2. Filter by allowed brands
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
    # 3. Main loop
    # ------------------------------------------------------------------
    for series_info in series_list:
        series_val   = series_info["value"]
        series_label = series_info["label"]
        brand        = series_info["brand"]
        brand_label  = brand.upper().replace("_", " ")

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

                # --- Checkpoint: skip fully-done model ---
                if (series_val, body_val, model_val, MARKET) in dm_set:
                    logger.info(
                        f"      Skip (checkpoint): {series_val}/{body_val}/{model_val}"
                    )
                    continue

                # ----------------------------------------------------------
                # ONE navigation: EUR market check + prod months combined
                # (replaces old separate get_markets + get_prods = 2 navs)
                # ----------------------------------------------------------
                all_prods = disc.get_prods_eur(page, series_val, body_val, model_val, CATALOG)
                if not all_prods:
                    logger.debug(
                        f"      EUR not available or no prods: "
                        f"{series_val}/{body_val}/{model_val}"
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

                    # ----------------------------------------------------------
                    # TWO navigations per prod month: engines + type codes
                    # (replaces old get_engines + get_transmissions + 2 type_code navs)
                    # ----------------------------------------------------------
                    cars = disc.get_cars_for_prod(
                        page, series_val, body_val, model_val, prod, CATALOG
                    )

                    for car in cars:
                        tc_full    = car["type_code_full"]
                        engine     = car["engine"]
                        steering   = car["steering"]
                        trans_label = car["transmission"]

                        if tc_full in known_type_codes:
                            logger.info(f"        Skip (already collected): {tc_full}")
                            continue

                        # Group by 4-char prefix
                        prefix = tc_full[:4]
                        if prefix not in prefix_to_num:
                            prefix_to_num[prefix] = next_group_num
                            next_group_num += 1

                        group_key = f"typecode#{prefix_to_num[prefix]} [{prefix}]"
                        if group_key not in result:
                            result[group_key] = {}

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

                        db.upsert(result[group_key][variant_key])

                    # All engines for this prod done
                    ckpt.mark_prod_done(
                        cp, dm_set, dp_set,
                        series_val, body_val, model_val, MARKET, prod
                    )

                # All sampled prods for this model done
                save(result)
                ckpt.mark_model_market_done(
                    cp, dm_set,
                    series_val, body_val, model_val, MARKET
                )

    save(result)
    logger.info(
        f"Collection complete — {total_added} new type codes added, "
        f"{len(result)} prefix groups total."
    )
    return result
