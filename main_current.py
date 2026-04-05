"""
main_current.py — Scraper 1: EUR-LHD-CURRENT

Collects type codes for ALL BMW, MINI, and Rolls Royce models
from the RealOEM Current catalog, EUR market, Left Hand Drive only,
sampling every 3rd production month.

Speed design — 2 parallel browsers in one worker:
  Phase 1: One browser fetches the full filtered series list (fast, ~30s).
  Phase 2: Series list split in half. Two Chrome browsers run in parallel
           threads, each handling one half. Net result: ~2× throughput
           with no extra DO worker needed.

Checkpoint design — survives container restarts:
  Every completed model+prod is written to PostgreSQL scraper_checkpoints.
  On restart the scraper reads from DB and resumes exactly where it stopped.
  The local JSON checkpoint is a backup only.

Output: output/EUR-LHD-CURRENT.json
DB:     type_codes table (PostgreSQL on DigitalOcean)
"""

import logging
import os
import sys
import threading
import time

os.environ.setdefault("SCRAPER_MODE", "current")

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


# ---------------------------------------------------------------------------
# Heartbeat — logs every 60s so DO logs show the scraper is alive
# ---------------------------------------------------------------------------

def _heartbeat(state: SharedState, stop_event: threading.Event):
    while not stop_event.wait(60):
        with state.lock:
            total_groups = len(state.result)
            new_codes    = state.total_added
        logger.info(
            f"[HEARTBEAT] {new_codes} new codes this session | "
            f"{total_groups} prefix groups in memory | scraper alive"
        )


# ---------------------------------------------------------------------------
# Worker — one browser + one series slice
# ---------------------------------------------------------------------------

def _run_worker(series_slice: list, state: SharedState,
                thread_id: int, errors: list):
    tname = f"Browser-{thread_id}"
    tlog  = logging.getLogger(tname)
    try:
        with sync_playwright() as p:
            browser, ctx, page = launch_browser(p)
            try:
                collect_series_slice(page, series_slice, state, thread_id)
            finally:
                try:
                    browser.close()
                    tlog.info("Browser closed.")
                except Exception:
                    pass
                db.close()
    except Exception as e:
        tlog.error(f"Worker {thread_id} crashed: {e}", exc_info=True)
        errors.append(e)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

logger = logging.getLogger("main_current")


def main():
    setup_logging()

    from config import OUTPUT_FILE, CATALOG, ALLOWED_BRANDS, TEST_MODE, SCRAPER_MODE
    tag = " [TEST MODE]" if TEST_MODE else ""
    logger.info(
        f"=== EUR-LHD-CURRENT Scraper starting{tag} ==="
        f"  catalog={CATALOG}  brands={ALLOWED_BRANDS}"
    )

    start_virtual_display()

    errors = []

    try:
        # ----------------------------------------------------------------
        # Phase 1: fetch series list with one temporary browser (fast)
        # ----------------------------------------------------------------
        logger.info("Phase 1: fetching series list …")
        series_list = []
        with sync_playwright() as p:
            browser, ctx, page = launch_browser(p)
            try:
                series_list = get_filtered_series(page)
            finally:
                try:
                    browser.close()
                except Exception:
                    pass
                db.close()

        if not series_list:
            logger.error("No series found — aborting.")
            return

        # ----------------------------------------------------------------
        # Phase 2: build shared state
        # ----------------------------------------------------------------
        existing = load_existing()

        # Known type codes: DB first (survives restart), then local JSON
        known = db.get_known_type_codes(SCRAPER_MODE)
        if not known:
            known = get_all_known_type_codes(existing)

        state = SharedState(existing, known)

        existing_total = sum(len(v) for v in existing.values())
        logger.info(
            f"Resuming from {existing_total} existing type codes "
            f"({len(known)} known in DB)."
        )

        # ----------------------------------------------------------------
        # Phase 3: split series and launch 2 parallel browsers
        # ----------------------------------------------------------------
        half   = max(len(series_list) // 2, 1)
        slice1 = series_list[:half]
        slice2 = series_list[half:]

        logger.info(
            f"Splitting {len(series_list)} series: "
            f"Browser-1 gets {len(slice1)}, Browser-2 gets {len(slice2)}"
        )

        # Start heartbeat
        stop_hb = threading.Event()
        hb_thread = threading.Thread(
            target=_heartbeat, args=(state, stop_hb),
            name="Heartbeat", daemon=True
        )
        hb_thread.start()

        # Launch workers
        t1 = threading.Thread(
            target=_run_worker,
            args=(slice1, state, 1, errors),
            name="Browser-1"
        )

        t2 = None
        if slice2:
            t2 = threading.Thread(
                target=_run_worker,
                args=(slice2, state, 2, errors),
                name="Browser-2"
            )

        t1.start()
        if t2:
            time.sleep(5)   # stagger launch so both don't hit Cloudflare at once
            t2.start()

        t1.join()
        if t2:
            t2.join()

        stop_hb.set()

        # ----------------------------------------------------------------
        # Final save
        # ----------------------------------------------------------------
        with state.lock:
            save(state.result)
            total = sum(len(v) for v in state.result.values())

        logger.info(
            f"=== Done! {state.total_added} new type codes added this session, "
            f"{total} total across {len(state.result)} prefix groups. ==="
        )
        logger.info(f"Output → {OUTPUT_FILE}")

        if errors:
            logger.error(f"{len(errors)} worker error(s): {errors}")

    except KeyboardInterrupt:
        logger.info("Interrupted — partial results saved.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
    finally:
        stop_virtual_display()


if __name__ == "__main__":
    main()
