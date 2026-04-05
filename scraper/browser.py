"""
scraper/browser.py — Browser launch and navigation helpers.

Always runs headed Chrome (headless=False) — same as our main scraper.
On the server (Docker/DigitalOcean), a Xvfb virtual display is started
first so headed Chrome has a display to render into.
This is what bypasses Cloudflare — headless Chrome gets blocked instantly.

Call start_virtual_display() at the top of main(), then launch_browser(),
then stop_virtual_display() in the finally block.
"""

import random
import time
import logging
from playwright.sync_api import Page, Browser, BrowserContext

logger = logging.getLogger(__name__)

_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/135.0.0.0 Safari/537.36"
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
# Browser launch — always headed
# ---------------------------------------------------------------------------

def launch_browser(playwright_instance) -> tuple:
    """
    Launch headed Chrome with stealth patches applied.
    Returns (browser, context, page).
    Always headed (headless=False) — Xvfb provides the display on the server.
    """
    proxy_url = __import__("os").getenv("PROXY_URL", "").strip()
    proxy_cfg = {"server": proxy_url} if proxy_url else None

    browser: Browser = playwright_instance.chromium.launch(
        headless=False,   # always headed — Cloudflare defeats headless Chrome
        args=[
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-dev-shm-usage",
            "--disable-infobars",
            "--disable-extensions",
            "--window-size=1920,1080",
            "--start-maximized",
        ],
        proxy=proxy_cfg,
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
            "Accept-Encoding": "gzip, deflate, br",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        },
    )

    page: Page = context.new_page()

    # Apply playwright-stealth to remove webdriver fingerprints
    try:
        from playwright_stealth import Stealth as _Stealth
        _Stealth().apply_stealth_sync(page)
        logger.info("playwright-stealth applied")
    except Exception as e:
        logger.warning(f"playwright-stealth not applied: {e} — continuing without it")

    page.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        Object.defineProperty(navigator, 'plugins',   { get: () => [1, 2, 3, 4, 5] });
        Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
        window.chrome = { runtime: {} };
    """)

    page.set_default_timeout(60_000)
    logger.info("Browser launched (headed Chrome + stealth)")
    return browser, context, page


# ---------------------------------------------------------------------------
# Human-like helpers
# ---------------------------------------------------------------------------

def human_delay(range_tuple: tuple):
    time.sleep(random.uniform(*range_tuple))


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
        try:
            title = page.title().lower()
        except Exception:
            title = ""

        if "just a moment" not in title and "checking your browser" not in title:
            try:
                cf_frames = [f for f in page.frames if "challenges.cloudflare.com" in f.url]
                if not cf_frames:
                    return
            except Exception:
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
# Safe navigation with retry
# ---------------------------------------------------------------------------

def safe_goto(page: Page, url: str, retries: int = 3):
    """Navigate to url with retry logic and Cloudflare handling."""
    from config import PAGE_LOAD_DELAY, RETRY_DELAY, MAX_RETRIES

    max_tries = max(retries, MAX_RETRIES)
    for attempt in range(1, max_tries + 1):
        try:
            logger.debug(f"→ {url}  (attempt {attempt})")
            page.goto(url, wait_until="domcontentloaded", timeout=45_000)
            try:
                page.wait_for_load_state("networkidle", timeout=3_000)
            except Exception:
                pass
            wait_for_no_cloudflare(page)
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
