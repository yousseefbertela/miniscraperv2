"""
storage/db.py — DigitalOcean PostgreSQL integration.

Opens one persistent connection at first use and reuses it for the whole run.
Creates the type_codes table on first run if it doesn't exist.
Each type_code_full is unique — upserts use ON CONFLICT DO NOTHING (safe to re-run).

If DB credentials are not set the module is a silent no-op (JSON-only mode).
"""

import logging
import os

logger = logging.getLogger(__name__)

_conn = None   # module-level persistent connection


def _get_conn():
    """Return (or open) the shared psycopg2 connection. Returns None if not configured."""
    global _conn

    # Check if existing connection is still alive
    if _conn is not None:
        try:
            _conn.isolation_level   # cheap liveness check
            return _conn
        except Exception:
            _conn = None

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
        _conn = psycopg2.connect(
            host=host, port=int(port), dbname=dbname,
            user=user, password=password,
            sslmode=sslmode, connect_timeout=15,
        )
        _conn.autocommit = True
        logger.info(f"Connected to DB: {user}@{host}:{port}/{dbname}")
        _ensure_table()
        return _conn
    except Exception as e:
        logger.error(f"DB connection failed: {e}")
        _conn = None
        return None


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_CREATE_TABLE = """
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
"""

_CREATE_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_tc_brand    ON type_codes (brand);
CREATE INDEX IF NOT EXISTS idx_tc_catalog  ON type_codes (catalog);
CREATE INDEX IF NOT EXISTS idx_tc_series   ON type_codes (series_value);
CREATE INDEX IF NOT EXISTS idx_tc_market   ON type_codes (market);
"""


def _ensure_table():
    conn = _get_conn()
    if not conn:
        return
    try:
        with conn.cursor() as cur:
            cur.execute(_CREATE_TABLE)
            cur.execute(_CREATE_INDEXES)
        logger.info("DB table 'type_codes' ready.")
    except Exception as e:
        logger.error(f"Failed to create table: {e}")


# ---------------------------------------------------------------------------
# Upsert
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
    Insert one type code record into PostgreSQL.
    Silent no-op if DB is not configured or connection is unavailable.
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


def close():
    """Close the DB connection gracefully (call at scraper shutdown)."""
    global _conn
    if _conn:
        try:
            _conn.close()
            logger.info("DB connection closed.")
        except Exception:
            pass
        _conn = None
