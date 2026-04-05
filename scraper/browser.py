"""
scraper/browser.py — Browser launch and navigation helpers.

Mirrors the main RealOEM scraper browser.py exactly — same Chrome version,
same args, same stealth setup — because that scraper has no Cloudflare issues.

Key points:
  - Always headless=False (headed Chrome)
  - Xvfb virtual display on Linux server (pyvirtualdisplay)
  - playwright-stealth==2.0.1 (pinned — version matters for stealth quality)
  - Chrome/124 user agent (matches what stealth expects)
  - No extra add_init_script on top of stealth (stealth handles it all)
"""

import random
import time
import logging
from playwright.sync_api import Page, Browser, BrowserContext
from playwright_stealth import Stealth as _Stealth

logger = logging.getLogger(__name__)

_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

_virtual_display = None


class BrowserCrashError(RuntimeError):
    """Raised when Chromium crashes — signals caller to restart the browser."""
    pass


# ---------------------------------------------------------------------------
# Virtual display (Xvfb) — Linux server only
# ---------------------------------------------------------------------------

def start_virtual_display():
    """
    Start a Xvfb virtual display so headed Chrome can run on a headless server.
    No-op on Windows or if pyvirtualdisplay is not installed.
    """
    global _virtual_display
    try:
        from pyvirtualdisplay import Display
        _virtual_display = Display(visible=False, size=(1920, 1080))
        _virtual_display.start()
        logger.info("Xvfb virtual display started (1920x1080)")
    except Exception as e:
        logger.info(f"Virtual display not started ({e}) — continuing without it")
        _virtual_display = None


def stop_virtual_display():
    """Stop the Xvfb virtual display if one was started."""
    global _virtual_display
    if _virtual_display is not None:
        try:
            _virtual_display.stop()
            logger.info("Virtual display stopped.")
        except Exception:
            pass
        _virtual_display = None


# ---------------------------------------------------------------------------
# Browser launch — always headed, same setup as main scraper
# ---------------------------------------------------------------------------

def launch_browser(playwright_instance) -> tuple:
    """
    Launch headed Chrome with stealth. Returns (browser, context, page).
    Identical to the main RealOEM scraper's launch_browser.
    """
    browser: Browser = playwright_instance.chromium.launch(
        headless=False,
        args=[
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-infobars",
            "--window-size=1920,1080",
            "--start-maximized",
        ],
    )

    context: BrowserContext = browser.new_context(
        viewport={"width": 1920, "height": 1080},
        user_agent=_USER_AGENT,
        locale="en-US",
        timezone_id="America/New_York",
        java_script_enabled=True,
        accept_downloads=False,
        extra_http_headers={
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;"
                "q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"
            ),
        },
    )

    page: Page = context.new_page()
    _Stealth().apply_stealth_sync(page)
    page.set_default_timeout(45_000)
    logger.info("Browser launched (headed Chrome + stealth)")
    return browser, context, page


# ---------------------------------------------------------------------------
# Human-like helpers
# ---------------------------------------------------------------------------

def human_delay(range_tuple: tuple):
    duration = random.uniform(*range_tuple)
    logger.debug(f"Sleeping {duration:.1f}s")
    time.sleep(duration)


def human_scroll(page: Page):
    page.mouse.wheel(0, random.randint(200, 600))
    time.sleep(random.uniform(0.3, 0.8))


# ---------------------------------------------------------------------------
# Cloudflare challenge detection
# ---------------------------------------------------------------------------

def wait_for_no_cloudflare(page: Page, timeout: int = 60):
    """Poll until Cloudflare challenge clears or timeout is reached."""
    start = time.time()
    while True:
        title = page.title()
        if "just a moment" not in title.lower():
            cf_frames = [f for f in page.frames if "challenges.cloudflare.com" in f.url]
            if not cf_frames:
                return
        elapsed = time.time() - start
        if elapsed > timeout:
            raise TimeoutError(
                f"Cloudflare challenge did not clear in {timeout}s. "
                "Consider using a residential proxy."
            )
        logger.warning(f"Cloudflare challenge active … ({elapsed:.0f}s)")
        time.sleep(2)


# ---------------------------------------------------------------------------
# Popup dismissal
# ---------------------------------------------------------------------------

_CLOSE_SELECTORS = [
    "button[class*=close]", "button[class*=dismiss]",
    "button[aria-label*=Close]", "a[class*=close]",
    "div[class*=close-btn]", "span[class*=close]",
    "[class*=overlay] button", "[class*=modal] button", "[class*=popup] button",
]

def dismiss_popups(page: Page):
    try:
        page.keyboard.press("Escape")
        time.sleep(0.3)
    except Exception:
        pass
    for sel in _CLOSE_SELECTORS:
        try:
            btn = page.locator(sel).first
            if btn.is_visible(timeout=300):
                btn.click(timeout=500)
                time.sleep(0.2)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Safe navigation with retry
# ---------------------------------------------------------------------------

def safe_goto(page: Page, url: str, retries: int = 3):
    """Navigate to url with retry logic and Cloudflare handling."""
    from config import PAGE_LOAD_DELAY, RETRY_DELAY, MAX_RETRIES

    max_tries = max(retries, MAX_RETRIES)
    for attempt in range(1, max_tries + 1):
        try:
            logger.debug(f"→ {url}  (attempt {attempt})")
            page.goto(url, wait_until="domcontentloaded", timeout=30_000)
            try:
                page.wait_for_load_state("networkidle", timeout=1_000)
            except Exception:
                pass
            wait_for_no_cloudflare(page)
            dismiss_popups(page)
            human_delay(PAGE_LOAD_DELAY)
            return
        except BrowserCrashError:
            raise
        except Exception as e:
            err = str(e).lower()
            logger.warning(f"Navigation error (attempt {attempt}): {e}")
            if "crashed" in err:
                raise BrowserCrashError(f"Chromium crashed: {e}")
        if attempt < max_tries:
            logger.info(f"Retrying in {RETRY_DELAY[0]}–{RETRY_DELAY[1]}s …")
            human_delay(RETRY_DELAY)

    raise RuntimeError(f"Failed to navigate to {url} after {max_tries} attempts")
