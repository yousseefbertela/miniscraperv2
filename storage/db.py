"""
storage/db.py — DigitalOcean PostgreSQL integration.

Thread-safe: each thread gets its own psycopg2 connection via threading.local().
Creates two tables:
  - type_codes         : the collected type codes (scraper output)
  - scraper_checkpoints: persistent checkpoint — survives container restarts

Checkpoint lives in DB so that when DigitalOcean restarts the container the
scraper can resume exactly where it left off instead of re-navigating from scratch.
"""

import logging
import os
import threading

logger = logging.getLogger(__name__)

_local = threading.local()   # each thread gets its own connection


# ---------------------------------------------------------------------------
# Connection (per-thread)
# ---------------------------------------------------------------------------

def _get_conn():
    """Return (or open) a per-thread psycopg2 connection. Returns None if not configured."""
    conn = getattr(_local, "conn", None)

    if conn is not None:
        try:
            conn.isolation_level   # cheap liveness check
            return conn
        except Exception:
            _local.conn = None

    host     = os.getenv("DB_HOST",     "")
    port     = os.getenv("DB_PORT",     "25060")
    dbname   = os.getenv("DB_NAME",     "defaultdb")
    user     = os.getenv("DB_USER",     "")
    password = os.getenv("DB_PASSWORD", "")
    sslmode  = os.getenv("DB_SSLMODE",  "require")

    if not host or not user:
        logger.debug("DB credentials not set — running in JSON-only mode.")
        return None

    try:
        import psycopg2
        conn = psycopg2.connect(
            host=host, port=int(port), dbname=dbname,
            user=user, password=password,
            sslmode=sslmode, connect_timeout=15,
        )
        conn.autocommit = True
        _local.conn = conn
        tname = threading.current_thread().name
        logger.info(f"[{tname}] Connected to DB: {user}@{host}:{port}/{dbname}")
        _ensure_tables()
        return conn
    except Exception as e:
        logger.error(f"DB connection failed: {e}")
        _local.conn = None
        return None


# ---------------------------------------------------------------------------
# Schema — two tables
# ---------------------------------------------------------------------------

_CREATE_TYPE_CODES = """
CREATE TABLE IF NOT EXISTS type_codes (
    id               SERIAL PRIMARY KEY,
    type_code_full   VARCHAR(120) UNIQUE NOT NULL,
    type_code_prefix VARCHAR(10),
    series_value     VARCHAR(30),
    series_label     VARCHAR(120),
    body             VARCHAR(30),
    model            VARCHAR(60),
    market           VARCHAR(10),
    engine           VARCHAR(30),
    transmission     VARCHAR(30),
    prod_month       VARCHAR(10),
    steering         VARCHAR(60),
    brand            VARCHAR(30),
    catalog          VARCHAR(20),
    scraper_mode     VARCHAR(20),
    created_at       TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_tc_brand    ON type_codes (brand);
CREATE INDEX IF NOT EXISTS idx_tc_catalog  ON type_codes (catalog);
CREATE INDEX IF NOT EXISTS idx_tc_series   ON type_codes (series_value);
CREATE INDEX IF NOT EXISTS idx_tc_market   ON type_codes (market);
"""

_CREATE_CHECKPOINTS = """
CREATE TABLE IF NOT EXISTS scraper_checkpoints (
    id           SERIAL PRIMARY KEY,
    scraper_mode VARCHAR(20)  NOT NULL,
    level        VARCHAR(20)  NOT NULL,
    series       VARCHAR(30)  NOT NULL DEFAULT '',
    body         VARCHAR(30)  NOT NULL DEFAULT '',
    model        VARCHAR(60)  NOT NULL DEFAULT '',
    market       VARCHAR(10)  NOT NULL DEFAULT '',
    prod         VARCHAR(10)  NOT NULL DEFAULT '',
    created_at   TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (scraper_mode, level, series, body, model, market, prod)
);
CREATE INDEX IF NOT EXISTS idx_ckpt_mode ON scraper_checkpoints (scraper_mode, level);
"""


def _ensure_tables():
    conn = _get_conn()
    if not conn:
        return
    try:
        with conn.cursor() as cur:
            cur.execute(_CREATE_TYPE_CODES)
            cur.execute(_CREATE_CHECKPOINTS)
        logger.info("DB tables ready (type_codes + scraper_checkpoints).")
    except Exception as e:
        logger.error(f"Failed to create tables: {e}")


# ---------------------------------------------------------------------------
# Checkpoint — read
# ---------------------------------------------------------------------------

def load_checkpoint(scraper_mode: str) -> dict | None:
    """
    Load checkpoint from DB. Returns dict {done_model_markets, done_prods}
    or None if DB unavailable / no entries exist.
    """
    conn = _get_conn()
    if not conn:
        return None
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT level, series, body, model, market, prod "
                "FROM scraper_checkpoints WHERE scraper_mode = %s",
                (scraper_mode,)
            )
            rows = cur.fetchall()

        if not rows:
            return None

        dm, dp = [], []
        for level, series, body, model, market, prod in rows:
            if level == "model_market":
                dm.append({"series": series, "body": body,
                           "model": model, "market": market})
            elif level == "prod":
                dp.append({"series": series, "body": body,
                           "model": model, "market": market, "prod": prod})

        logger.info(f"DB checkpoint: {len(dm)} done models, {len(dp)} done prods")
        return {"done_model_markets": dm, "done_prods": dp}
    except Exception as e:
        logger.error(f"Failed to load DB checkpoint: {e}")
        return None


# ---------------------------------------------------------------------------
# Checkpoint — write (one entry per call, ON CONFLICT DO NOTHING)
# ---------------------------------------------------------------------------

def save_checkpoint_entry(scraper_mode: str, level: str,
                           series: str, body: str, model: str,
                           market: str, prod: str = ""):
    """
    Persist one checkpoint entry to DB. Thread-safe (each thread has own conn).
    """
    conn = _get_conn()
    if not conn:
        return
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO scraper_checkpoints
                    (scraper_mode, level, series, body, model, market, prod)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (scraper_mode, level, series, body, model, market, prod)
                DO NOTHING
                """,
                (scraper_mode, level, series, body, model, market, prod or "")
            )
    except Exception as e:
        logger.warning(f"Failed to save checkpoint entry: {e}")


# ---------------------------------------------------------------------------
# Known type codes — loaded at startup so we skip already-collected codes
# even when the local JSON file is gone (container restart).
# ---------------------------------------------------------------------------

def bootstrap_checkpoint_from_type_codes(scraper_mode: str):
    """
    One-time migration: populate scraper_checkpoints from existing type_codes.
    Called automatically when checkpoint is empty but type_codes has data —
    i.e. first deploy of the DB-checkpoint system, or after a table wipe.
    Safe to call multiple times (ON CONFLICT DO NOTHING).
    """
    conn = _get_conn()
    if not conn:
        return
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO scraper_checkpoints
                    (scraper_mode, level, series, body, model, market, prod)
                SELECT DISTINCT %s, 'model_market',
                       series_value, COALESCE(body,''), model, market, ''
                FROM type_codes WHERE scraper_mode = %s
                ON CONFLICT DO NOTHING
                """,
                (scraper_mode, scraper_mode)
            )
            dm = cur.rowcount
            cur.execute(
                """
                INSERT INTO scraper_checkpoints
                    (scraper_mode, level, series, body, model, market, prod)
                SELECT DISTINCT %s, 'prod',
                       series_value, COALESCE(body,''), model, market, prod_month
                FROM type_codes WHERE scraper_mode = %s
                ON CONFLICT DO NOTHING
                """,
                (scraper_mode, scraper_mode)
            )
            dp = cur.rowcount
        if dm or dp:
            logger.info(
                f"Bootstrapped checkpoint from type_codes: "
                f"{dm} model_market + {dp} prod entries ({scraper_mode})"
            )
    except Exception as e:
        logger.error(f"Failed to bootstrap checkpoint: {e}")


def get_known_type_codes(scraper_mode: str) -> set:
    """
    Return all type_code_full values in DB for this scraper_mode.
    Fast set lookup prevents re-inserting duplicates after a restart.
    """
    conn = _get_conn()
    if not conn:
        return set()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT type_code_full FROM type_codes WHERE scraper_mode = %s",
                (scraper_mode,)
            )
            rows = cur.fetchall()
        result = {r[0] for r in rows}
        logger.info(f"Loaded {len(result)} known type codes from DB ({scraper_mode})")
        return result
    except Exception as e:
        logger.error(f"Failed to load known type codes from DB: {e}")
        return set()


# ---------------------------------------------------------------------------
# Type code upsert
# ---------------------------------------------------------------------------

_UPSERT = """
INSERT INTO type_codes (
    type_code_full, type_code_prefix,
    series_value, series_label, body, model,
    market, engine, transmission, prod_month,
    steering, brand, catalog, scraper_mode
) VALUES (
    %(type_code_full)s,   %(type_code_prefix)s,
    %(series_value)s,     %(series_label)s,
    %(body)s,             %(model)s,
    %(market)s,           %(engine)s,
    %(transmission)s,     %(prod_month)s,
    %(steering)s,         %(brand)s,
    %(catalog)s,          %(scraper_mode)s
)
ON CONFLICT (type_code_full) DO NOTHING;
"""


def upsert(record: dict):
    """
    Insert one type code record. Thread-safe (each thread has own conn).
    Silent no-op if DB is not configured.
    """
    conn = _get_conn()
    if not conn:
        return

    from config import SCRAPER_MODE
    try:
        row = {
            "type_code_full":   record.get("type_code_full", ""),
            "type_code_prefix": record.get("type_code_full", "")[:4],
            "series_value":     record.get("series_value",   ""),
            "series_label":     record.get("series_label",   ""),
            "body":             record.get("body",           ""),
            "model":            record.get("model",          ""),
            "market":           record.get("market",         ""),
            "engine":           record.get("engine",         ""),
            "transmission":     record.get("transmission",   ""),
            "prod_month":       record.get("prod_month",     ""),
            "steering":         record.get("steering",       ""),
            "brand":            record.get("brand",          ""),
            "catalog":          record.get("catalog",        ""),
            "scraper_mode":     SCRAPER_MODE,
        }
        with conn.cursor() as cur:
            cur.execute(_UPSERT, row)
        logger.debug(f"DB upsert: {row['type_code_full']}")
    except Exception as e:
        logger.warning(f"DB upsert failed for {record.get('type_code_full')}: {e}")


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

def close():
    """Close the current thread's DB connection gracefully."""
    conn = getattr(_local, "conn", None)
    if conn:
        try:
            conn.close()
            logger.info("DB connection closed.")
        except Exception:
            pass
        _local.conn = None
