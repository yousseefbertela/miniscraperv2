"""
main_classic.py — Scraper 2: EUR-LHD-CLASSIC

Collects type codes for all BMW models from the RealOEM Classic catalog,
EUR market, Left Hand Drive only, sampling every 3rd production month.

Single browser, sequential series navigation.
Checkpoint survives container restarts via PostgreSQL.

Output: output/EUR-LHD-CLASSIC.json
DB:     type_codes table (PostgreSQL on DigitalOcean)
"""

import logging
import os
import sys
import threading

os.environ["SCRAPER_MODE"] = "classic"

from playwright.sync_api import sync_playwright

from scraper.browser import launch_browser, start_virtual_display, stop_virtual_display
from scraper.collector import SharedState, get_filtered_series, collect_series_slice
from storage.writer import load_existing, save, get_all_known_type_codes
from storage import db


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def _heartbeat(state: SharedState, stop_event: threading.Event):
    while not stop_event.wait(60):
        with state.lock:
            total_groups = len(state.result)
            new_codes    = state.total_added
        logger.info(
            f"[HEARTBEAT] {new_codes} new codes this session | "
            f"{total_groups} prefix groups in memory | scraper alive"
        )


logger = logging.getLogger("main_classic")


def main():
    setup_logging()

    from config import OUTPUT_FILE, CATALOG, ALLOWED_BRANDS, TEST_MODE, SCRAPER_MODE
    tag = " [TEST MODE]" if TEST_MODE else ""
    logger.info(
        f"=== EUR-LHD-CLASSIC Scraper starting{tag} ==="
        f"  catalog={CATALOG}  brands={ALLOWED_BRANDS}"
    )

    start_virtual_display()

    try:
        with sync_playwright() as p:
            browser, ctx, page = launch_browser(p)
            try:
                # Build shared state
                existing = load_existing()
                known = db.get_known_type_codes(SCRAPER_MODE)
                if not known:
                    known = get_all_known_type_codes(existing)
                state = SharedState(existing, known)

                existing_total = sum(len(v) for v in existing.values())
                logger.info(
                    f"Resuming from {existing_total} existing type codes "
                    f"({len(known)} known in DB)."
                )

                # Fetch series list
                series_list = get_filtered_series(page)
                if not series_list:
                    logger.error("No series found — aborting.")
                    return

                # Start heartbeat
                stop_hb = threading.Event()
                hb = threading.Thread(
                    target=_heartbeat, args=(state, stop_hb),
                    name="Heartbeat", daemon=True
                )
                hb.start()

                # Single browser, all series
                collect_series_slice(page, series_list, state, thread_id=1)

                stop_hb.set()

                # Final save
                with state.lock:
                    save(state.result)
                    total = sum(len(v) for v in state.result.values())

                logger.info(
                    f"=== Done! {state.total_added} new type codes this session, "
                    f"{total} total across {len(state.result)} prefix groups. ==="
                )
                logger.info(f"Output → {OUTPUT_FILE}")

            except KeyboardInterrupt:
                logger.info("Interrupted — partial results saved.")
            except Exception as e:
                logger.error(f"Unexpected error: {e}", exc_info=True)
            finally:
                try:
                    browser.close()
                    logger.info("Browser closed.")
                except Exception:
                    pass
                db.close()
    finally:
        stop_virtual_display()


if __name__ == "__main__":
    main()
