"""
scraper/discovery.py — RealOEM dropdown enumeration via URL-param navigation.

Speed design:
  Every URL param we know upfront gets baked into the URL so RealOEM
  pre-populates all downstream dropdowns in one round-trip.

  Key optimisation: always include steering=L (RealOEM's LHD value) in
  every URL that has an engine param. This means the Browse Parts button
  appears in the SAME page that also gives us the engine list — collapsing
  what was 4 navigations per prod month down to 2:

    Old (4 navs/prod):  get_engines | get_transmissions | type_code attempt | type_code retry
    New (2 navs/prod):  get_prods_eur (EUR check + prods) | get_cars_for_prod (engines + type code)

  For 19 sampled prod months × 2 saved navs × ~9 s/nav = ~5 min saved per model.

Fallback: step-by-step form navigation (page.expect_navigation) for the rare
cases where Browse Parts requires JS onChange events to appear.

Steering rule (STRICT_LHD):
  - We always try steering=L first (RealOEM's Left Hand Drive URL value).
  - If Browse Parts appears → done (no extra nav needed).
  - If steering dropdown shows LHD but with a different value → retry (1 extra nav).
  - If steering dropdown exists and NO LHD option → skip this car.
  - If no steering dropdown at all → proceed as-is.
"""

import re
import time
import logging
from urllib.parse import urlencode

from bs4 import BeautifulSoup
from scraper.browser import safe_goto
from config import BMW_SELECT_URL, ACTION_DELAY, STRICT_LHD

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _nav(page, catalog: str = "Current", **params) -> BeautifulSoup:
    """
    Navigate to the RealOEM select page with the given params.
    Always adds product=P and the correct archive= value for the catalog.
    Returns a BeautifulSoup of the resulting page.
    """
    all_params = {
        "product": "P",
        "archive": "1" if catalog == "Classic" else "0",
    }
    all_params.update(params)
    url = f"{BMW_SELECT_URL}?{urlencode(all_params)}"
    safe_goto(page, url)
    return BeautifulSoup(page.content(), "html.parser")


def _read_select(soup, name: str) -> list:
    """Return [{value, label}] from <select name=name>, skipping blank/placeholder rows."""
    sel = soup.find("select", {"name": name})
    if not sel:
        return []
    result = []
    for opt in sel.find_all("option"):
        v = (opt.get("value") or "").strip()
        l = opt.get_text(strip=True)
        if not v or v.startswith("-") or l.startswith("-") or not l:
            continue
        result.append({"value": v, "label": l})
    return result


def _extract_type_code(soup) -> str | None:
    """
    Extract type_code_full from the Browse Parts button.
    RealOEM renders it as a hidden input inside a form[action*=partgrp].
    Falls back to scanning <a href> links.
    """
    for form in soup.find_all("form"):
        action = form.get("action", "")
        if "partgrp" in action or "showparts" in action:
            for inp in form.find_all("input", attrs={"type": "hidden"}):
                val = (inp.get("value") or "").strip()
                if val.count("-") >= 4:
                    return val

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "partgrp" in href or "showparts" in href:
            m = re.search(r"[?&]id=([^& ]+)", href)
            if m:
                tc = m.group(1)
                if tc.count("-") >= 4:
                    return tc
    return None


def _pick_lhd(steerings: list) -> dict | None:
    """
    Return the LHD option from a steering list.
    Returns None if STRICT_LHD=True and no LHD option exists.
    """
    lhd = next((s for s in steerings if "left" in s["label"].lower()), None)
    if lhd:
        return lhd
    return None if STRICT_LHD else steerings[0]


def _form_fallback(page, series: str, body: str, model: str,
                   market: str, prod: str, engine: str,
                   catalog: str = "Current") -> str | None:
    """
    Step-by-step form fallback. Selects each dropdown in order, waiting for
    the page to reload after each. Used when URL-param approach fails.
    Returns type_code_full or None.
    """
    safe_goto(page, f"{BMW_SELECT_URL}?product=P&archive={'1' if catalog == 'Classic' else '0'}")

    def sel_nav(name: str, value: str) -> bool:
        try:
            el = page.locator(f"select[name='{name}']").first
            el.wait_for(state="visible", timeout=12_000)
            if page.locator(f"select[name='{name}'] option[value='{value}']").count() == 0:
                logger.warning(f"Form fallback: option {value!r} not found in {name}")
                return False
            with page.expect_navigation(wait_until="domcontentloaded", timeout=15_000):
                el.select_option(value=value)
            time.sleep(1.2)
            return True
        except Exception as e:
            logger.warning(f"Form fallback: error selecting {value!r} in {name}: {e}")
            return False

    if not sel_nav("series",  series):  return None
    if not sel_nav("body",    body):    return None
    if not sel_nav("model",   model):   return None
    if not sel_nav("market",  market):  return None
    if not sel_nav("prod",    prod):    return None
    if not sel_nav("engine",  engine):  return None

    # Steering: enforce STRICT_LHD
    try:
        page.locator("select[name='steering']").wait_for(state="visible", timeout=4_000)
        opts = page.locator("select[name='steering'] option").all()
        steerings = [
            {"value": o.get_attribute("value") or "", "label": o.inner_text() or ""}
            for o in opts
            if (o.get_attribute("value") or "").strip()
            and not (o.get_attribute("value") or "").startswith("-")
        ]
        if steerings:
            chosen = _pick_lhd(steerings)
            if chosen is None:
                logger.info(f"Form fallback: no LHD for {series}/{model} — skipping")
                return None
            sel_nav("steering", chosen["value"])
    except Exception:
        pass

    try:
        page.wait_for_selector("form[action*='partgrp']", timeout=6_000)
    except Exception:
        try:
            page.wait_for_selector("a[href*='partgrp']", timeout=4_000)
        except Exception:
            pass

    return _extract_type_code(BeautifulSoup(page.content(), "html.parser"))


# ---------------------------------------------------------------------------
# Public discovery — series / bodies / models (unchanged, called once per series)
# ---------------------------------------------------------------------------

def get_all_series(page, catalog: str = "Current") -> list:
    """Return all series options from the select page: [{value, label}, ...]"""
    soup   = _nav(page, catalog)
    series = _read_select(soup, "series")
    logger.info(f"Found {len(series)} series (catalog={catalog})")
    return series


def get_bodies(page, series: str, catalog: str = "Current") -> list:
    soup   = _nav(page, catalog, series=series)
    bodies = _read_select(soup, "body")
    logger.debug(f"Series {series}: {len(bodies)} bodies")
    return bodies


def get_models(page, series: str, body: str, catalog: str = "Current") -> list:
    soup   = _nav(page, catalog, series=series, body=body)
    models = _read_select(soup, "model")
    logger.debug(f"{series}/{body}: {len(models)} models")
    return models


# ---------------------------------------------------------------------------
# Combined fast functions — each replaces 2-4 separate navigations
# ---------------------------------------------------------------------------

def get_prods_eur(page, series: str, body: str, model: str,
                  catalog: str = "Current") -> list:
    """
    ONE navigation: navigate with market=EUR and return the prod month list.
    Returns [] if EUR market is not available for this model (skip the model).

    Replaces the old separate get_markets (1 nav) + get_prods (1 nav) = 2 navs.
    """
    soup  = _nav(page, catalog, series=series, body=body, model=model, market="EUR")
    prods = [o["value"] for o in _read_select(soup, "prod")]
    if prods:
        logger.debug(f"{series}/{body}/{model}: {len(prods)} EUR prod months")
    else:
        logger.debug(f"{series}/{body}/{model}: EUR not available or no prods")
    return prods


def get_cars_for_prod(page, series: str, body: str, model: str,
                      prod: str, catalog: str = "Current") -> list:
    """
    TWO navigations per prod month (was 4):

      Nav 1: URL(series, body, model, EUR, prod, steering=L)
             → reads engine dropdown (replaces old get_engines nav)
             → might already show type code if only 1 engine and L is correct

      Nav 2 (per engine): URL(series, body, model, EUR, prod, engine, steering=L)
             → gets type code (replaces old get_transmissions + 2 type_code navs)
             → also reads transmission dropdown from same page if present

    Returns list of {type_code_full, engine, steering, transmission}.
    Returns [] if no engines found or no LHD available.
    """
    results = []

    # ------------------------------------------------------------------
    # Nav 1: get engine list.  steering=L included so that if there is
    # only one engine AND L is the correct steering value, Browse Parts
    # might already appear here (bonus: saves Nav 2 for those cases).
    # ------------------------------------------------------------------
    soup = _nav(page, catalog, series=series, body=body, model=model,
                market="EUR", prod=prod, steering="L")

    engines = [o["value"] for o in _read_select(soup, "engine")]
    if not engines:
        logger.debug(f"No engines for {series}/{model}/EUR/{prod}")
        return results

    logger.debug(f"Engines for {series}/{model}/EUR/{prod}: {engines}")

    for engine in engines:
        # ------------------------------------------------------------------
        # Nav 2: get type code with engine + steering=L.
        # For the vast majority of EUR LHD cars this is the final step.
        # ------------------------------------------------------------------
        soup2 = _nav(page, catalog, series=series, body=body, model=model,
                     market="EUR", prod=prod, engine=engine, steering="L")

        tc = _extract_type_code(soup2)

        if tc:
            # Check for a transmission dropdown on the SAME page (no extra nav)
            trans_list = (
                _read_select(soup2, "trans")
                or _read_select(soup2, "transmission")
            )
            if trans_list:
                # Transmission exists: need one nav per option (rare)
                for t in trans_list:
                    soup3 = _nav(page, catalog, series=series, body=body, model=model,
                                 market="EUR", prod=prod, engine=engine,
                                 steering="L", trans=t["value"])
                    tc3 = _extract_type_code(soup3)
                    if tc3:
                        logger.info(f"Type code: {tc3}  [trans={t['label']}]")
                        results.append({
                            "type_code_full": tc3,
                            "engine":         engine,
                            "steering":       "Left hand drive",
                            "transmission":   t["label"],
                        })
            else:
                logger.info(f"Type code: {tc}")
                results.append({
                    "type_code_full": tc,
                    "engine":         engine,
                    "steering":       "Left hand drive",
                    "transmission":   "",
                })
            continue

        # ------------------------------------------------------------------
        # steering=L didn't produce a type code.
        # Check if a steering dropdown is present on this page.
        # ------------------------------------------------------------------
        steerings = _read_select(soup2, "steering")
        if steerings:
            lhd = _pick_lhd(steerings)
            if lhd is None:
                logger.info(
                    f"No LHD for {series}/{model}/EUR/{prod}/{engine} "
                    f"— skipping (STRICT_LHD)"
                )
                continue   # skip this engine (and car)

            if lhd["value"] != "L":
                # LHD exists but has a different value — one extra nav
                soup3 = _nav(page, catalog, series=series, body=body, model=model,
                             market="EUR", prod=prod, engine=engine,
                             steering=lhd["value"])
                tc = _extract_type_code(soup3)
                if tc:
                    logger.info(f"Type code: {tc}  (steering: {lhd['label']})")
                    results.append({
                        "type_code_full": tc,
                        "engine":         engine,
                        "steering":       lhd["label"],
                        "transmission":   "",
                    })
                    continue

        # ------------------------------------------------------------------
        # URL-param approach failed entirely — use step-by-step form fallback
        # ------------------------------------------------------------------
        logger.info(
            f"URL-param failed for {series}/{model}/EUR/{prod}/{engine} "
            f"(catalog={catalog}) — trying form fallback"
        )
        tc = _form_fallback(page, series, body, model, "EUR", prod, engine, catalog)
        if tc:
            logger.info(f"Type code (form fallback): {tc}")
            results.append({
                "type_code_full": tc,
                "engine":         engine,
                "steering":       "Left hand drive",
                "transmission":   "",
            })

    return results
