"""
scraper/discovery.py — RealOEM dropdown enumeration via URL-param navigation.

Navigation strategy:
  Primary  : Build the full URL with all known params and parse the resulting HTML.
             RealOEM pre-populates all dropdowns when params are in the URL, so
             no JavaScript events need to fire — we just read the page source.

  Fallback : Step-by-step form navigation. Each dropdown selection triggers a
             GET reload, so we use page.expect_navigation() to wait for each.
             Used when Browse Parts does not appear after the primary URL-param
             approach (JS onChange dependency on some configurations).

Steering rule (STRICT_LHD=True):
  - If steering dropdown is present and LHD option exists  → select LHD, continue.
  - If steering dropdown is present and NO LHD             → return None (skip car).
  - If no steering dropdown                               → proceed as-is.

RealOEM catalog parameter:
  archive=0  → Current catalog   (modern cars: F-series, G-series, etc.)
  archive=1  → Classic catalog   (older cars: E36, E46, E90, etc.)
"""

import re
import time
import logging
from urllib.parse import urlencode

from bs4 import BeautifulSoup
from scraper.browser import safe_goto, human_delay
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
    """
    Return [{value, label}, ...] from <select name=name>.
    Skips blank rows and placeholder rows that start with '-'.
    """
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
    Extract the type_code_full string from the Browse Parts button.

    RealOEM renders Browse Parts as a <form action='/bmw/enUS/partgrp'>
    with a hidden <input value='1R11-EUR-03-2007-E81-BMW-116i_1.6'>.
    We check form hidden inputs first, then fall back to scanning <a> links.
    """
    # Primary: hidden input inside a partgrp/showparts form
    for form in soup.find_all("form"):
        action = form.get("action", "")
        if "partgrp" in action or "showparts" in action:
            for inp in form.find_all("input", attrs={"type": "hidden"}):
                val = (inp.get("value") or "").strip()
                if val.count("-") >= 4:
                    return val

    # Fallback: <a href> with id= param (older RealOEM layout)
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
    Return the Left Hand Drive option from the steering list.
    If STRICT_LHD=True and no LHD found → return None (caller must skip car).
    """
    lhd = next((s for s in steerings if "left" in s["label"].lower()), None)
    if lhd:
        return lhd
    if STRICT_LHD:
        return None    # strict: skip this car entirely
    return steerings[0]


def _form_fallback(page, series: str, body: str, model: str,
                   market: str, prod: str, engine: str,
                   catalog: str = "Current",
                   transmission: str = "") -> str | None:
    """
    Step-by-step form fallback. Selects each dropdown in order, waiting for
    the page to reload after each selection (RealOEM submits on change via GET).

    Returns type_code_full string, or None if LHD unavailable or Browse Parts
    does not appear after all selections.
    """
    safe_goto(page, f"{BMW_SELECT_URL}?product=P&archive={'1' if catalog == 'Classic' else '0'}")

    def sel_nav(name: str, value: str) -> bool:
        """Select a dropdown value and wait for the resulting page reload."""
        selector = f"select[name='{name}']"
        try:
            el = page.locator(selector).first
            el.wait_for(state="visible", timeout=12_000)
            # Make sure the option actually exists before selecting
            if page.locator(f"{selector} option[value='{value}']").count() == 0:
                logger.warning(f"Form fallback: option {value!r} not found in {selector}")
                return False
            with page.expect_navigation(wait_until="domcontentloaded", timeout=15_000):
                el.select_option(value=value)
            time.sleep(1.2)
            return True
        except Exception as e:
            logger.warning(f"Form fallback: error selecting {value!r} in {selector}: {e}")
            return False

    # Required dropdowns in order
    if not sel_nav("series",  series):  return None
    if not sel_nav("body",    body):    return None
    if not sel_nav("model",   model):   return None
    if not sel_nav("market",  market):  return None
    if not sel_nav("prod",    prod):    return None
    if not sel_nav("engine",  engine):  return None

    # Steering — enforce STRICT_LHD
    try:
        steering_el = page.locator("select[name='steering']")
        steering_el.wait_for(state="visible", timeout=4_000)
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
                logger.info(
                    f"Form fallback: no LHD for "
                    f"{series}/{model}/{market}/{prod}/{engine} — skipping"
                )
                return None
            sel_nav("steering", chosen["value"])
    except Exception:
        pass   # no steering dropdown — fine

    # Transmission (optional)
    if transmission:
        for t_name in ("trans", "transmission"):
            try:
                t_el = page.locator(f"select[name='{t_name}']").first
                t_el.wait_for(state="visible", timeout=2_000)
                sel_nav(t_name, transmission)
                break
            except Exception:
                continue

    # Wait for Browse Parts to appear (JS may add it asynchronously)
    try:
        page.wait_for_selector("form[action*='partgrp']", timeout=6_000)
    except Exception:
        try:
            page.wait_for_selector("a[href*='partgrp']", timeout=4_000)
        except Exception:
            pass

    soup = BeautifulSoup(page.content(), "html.parser")
    return _extract_type_code(soup)


# ---------------------------------------------------------------------------
# Public discovery functions
# ---------------------------------------------------------------------------

def get_all_series(page, catalog: str = "Current") -> list:
    """Return all series options from the select page: [{value, label}, ...]"""
    soup   = _nav(page, catalog)
    series = _read_select(soup, "series")
    logger.info(f"Found {len(series)} series (catalog={catalog})")
    return series


def get_bodies(page, series: str, catalog: str = "Current") -> list:
    """Return body type options for the given series."""
    soup   = _nav(page, catalog, series=series)
    bodies = _read_select(soup, "body")
    logger.debug(f"Series {series}: {len(bodies)} bodies")
    return bodies


def get_models(page, series: str, body: str, catalog: str = "Current") -> list:
    """Return model options for series + body."""
    soup   = _nav(page, catalog, series=series, body=body)
    models = _read_select(soup, "model")
    logger.debug(f"{series}/{body}: {len(models)} models")
    return models


def get_markets(page, series: str, body: str, model: str,
                catalog: str = "Current") -> list:
    """Return available market value strings for series+body+model."""
    soup    = _nav(page, catalog, series=series, body=body, model=model)
    markets = [o["value"] for o in _read_select(soup, "market")]
    logger.debug(f"{series}/{body}/{model} → markets: {markets}")
    return markets


def get_prods(page, series: str, body: str, model: str, market: str,
              catalog: str = "Current") -> list:
    """Return ALL production date values for the given combination."""
    soup  = _nav(page, catalog, series=series, body=body, model=model, market=market)
    prods = [o["value"] for o in _read_select(soup, "prod")]
    logger.debug(f"{series}/{model}/{market} → {len(prods)} prod dates")
    return prods


def get_engines(page, series: str, body: str, model: str,
                market: str, prod: str, catalog: str = "Current") -> list:
    """Return all available engine codes."""
    soup    = _nav(page, catalog, series=series, body=body, model=model,
                   market=market, prod=prod)
    engines = [o["value"] for o in _read_select(soup, "engine")]
    logger.debug(f"Engines for {series}/{model}/{market}/{prod}: {engines}")
    return engines


def get_transmissions(page, series: str, body: str, model: str,
                      market: str, prod: str, engine: str,
                      catalog: str = "Current") -> list:
    """
    Check whether a transmission dropdown appears after engine selection.
    Returns a list of transmission value strings, or [""] if no dropdown exists.

    Some cars only reveal a transmission dropdown after steering is also set,
    so we try a second time with LHD steering included.
    """
    soup = _nav(page, catalog, series=series, body=body, model=model,
                market=market, prod=prod, engine=engine)

    for field in ("trans", "transmission"):
        opts = _read_select(soup, field)
        if opts:
            values = [o["value"] for o in opts]
            logger.debug(f"Transmission ({field}): {values}")
            return values

    # Some cars only show transmission after steering — try with LHD
    steerings = _read_select(soup, "steering")
    if steerings:
        chosen = _pick_lhd(steerings)
        if chosen is None:
            return [""]   # No LHD available — car will be skipped in get_type_code_full
        soup2 = _nav(page, catalog, series=series, body=body, model=model,
                     market=market, prod=prod, engine=engine,
                     steering=chosen["value"])
        for field in ("trans", "transmission"):
            opts = _read_select(soup2, field)
            if opts:
                values = [o["value"] for o in opts]
                logger.debug(f"Transmission after steering ({field}): {values}")
                return values

    return [""]


def get_type_code_full(page, series: str, body: str, model: str,
                       market: str, prod: str, engine: str,
                       catalog: str = "Current",
                       transmission: str = "") -> dict | None:
    """
    Navigate with all known params and extract the type_code_full.

    Steering logic (STRICT_LHD=True):
      1. No steering dropdown → proceed (market is LHD-only).
      2. Steering dropdown + LHD option → select LHD, continue.
      3. Steering dropdown + no LHD → return None (skip this car entirely).

    Falls back to step-by-step form navigation if URL-param approach
    does not produce a Browse Parts button.

    Returns {"type_code_full": str, "steering": str, "transmission": str}
    or None if LHD not available or Browse Parts cannot be found.
    """
    extra = {}
    if transmission:
        extra["trans"] = transmission

    soup = _nav(page, catalog, series=series, body=body, model=model,
                market=market, prod=prod, engine=engine, **extra)

    # Happy path — Browse Parts already visible
    tc = _extract_type_code(soup)
    if tc:
        logger.info(f"Type code: {tc}")
        return {"type_code_full": tc, "steering": "", "transmission": transmission}

    # Steering dropdown present?
    steerings = _read_select(soup, "steering")
    if steerings:
        chosen = _pick_lhd(steerings)
        if chosen is None:
            logger.info(
                f"No LHD steering for {series}/{body}/{model}/{market}/{prod}/{engine} "
                f"— skipping (STRICT_LHD)"
            )
            return None

        logger.debug(f"Steering → {chosen['label']}")
        soup2 = _nav(page, catalog, series=series, body=body, model=model,
                     market=market, prod=prod, engine=engine,
                     steering=chosen["value"], **extra)
        tc = _extract_type_code(soup2)
        if tc:
            logger.info(f"Type code: {tc}  (steering: {chosen['label']})")
            return {
                "type_code_full": tc,
                "steering":       chosen["label"],
                "transmission":   transmission,
            }

    # URL-param approach failed — use step-by-step form fallback
    logger.info(
        f"URL-param failed for {series}/{body}/{model}/{market}/{prod}/{engine} "
        f"(catalog={catalog}) — trying form fallback"
    )
    tc = _form_fallback(
        page, series, body, model, market, prod, engine,
        catalog=catalog, transmission=transmission,
    )
    if tc:
        logger.info(f"Type code (form fallback): {tc}")
        return {
            "type_code_full": tc,
            "steering":       "Left hand drive",
            "transmission":   transmission,
        }

    logger.warning(
        f"No Browse Parts found for "
        f"{series}/{body}/{model}/{market}/{prod}/{engine} (catalog={catalog})"
    )
    return None
