"""
main_classic.py — Scraper 2: EUR-LHD-CLASSIC

Collects type codes for all BMW models from the RealOEM Classic catalog
(E-series and older), EUR market, Left Hand Drive only,
sampling every 3rd production month.

Output: output/EUR-LHD-CLASSIC.json
DB:     type_codes table (PostgreSQL on DigitalOcean)

Usage (local — headed browser):
    py -3 main_classic.py

Usage (test mode — saves to test-data/ instead):
    set TEST_MODE=true && py -3 main_classic.py

Usage (Docker):
    docker compose up scraper-classic
"""

import logging
import os
import sys

# Set mode BEFORE importing config so all constants load correctly
os.environ["SCRAPER_MODE"] = "classic"

from playwright.sync_api import sync_playwright

from scraper.browser import launch_browser
from scraper.collector import collect_all_type_codes
from storage.writer import load_existing, save
from storage import db


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def main():
    setup_logging()
    logger = logging.getLogger("main_classic")

    from config import OUTPUT_FILE, CATALOG, ALLOWED_BRANDS, TEST_MODE
    tag = " [TEST MODE → test-data/]" if TEST_MODE else ""
    logger.info(
        f"=== EUR-LHD-CLASSIC Scraper starting{tag} ==="
        f"  catalog={CATALOG}  brands={ALLOWED_BRANDS}"
    )

    existing = load_existing()
    existing_count = sum(len(v) for v in existing.values())
    if existing_count:
        logger.info(f"Resuming: {existing_count} type codes already in {OUTPUT_FILE}")

    with sync_playwright() as p:
        browser, context, page = launch_browser(p)
        try:
            result = collect_all_type_codes(page, existing)
            save(result)

            total = sum(len(v) for v in result.values())
            logger.info(
                f"=== Done! {total} type codes across {len(result)} prefix groups ==="
            )
            logger.info(f"Output → {OUTPUT_FILE}")

        except KeyboardInterrupt:
            logger.info("Interrupted by user — partial results saved.")
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)
        finally:
            try:
                browser.close()
                logger.info("Browser closed.")
            except Exception:
                pass
            db.close()


if __name__ == "__main__":
    main()
