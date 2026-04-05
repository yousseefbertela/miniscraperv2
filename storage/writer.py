"""
storage/writer.py — Atomic JSON output file read/write.

Saved once per model (after all prod months for that model finish),
plus a final flush at the end. Uses temp-file + rename to avoid
corruption if the scraper is interrupted mid-write.
"""

import json
import logging
import os

logger = logging.getLogger(__name__)


def _paths():
    from config import OUTPUT_FILE, OUTPUT_DIR
    return OUTPUT_FILE, OUTPUT_DIR


def load_existing() -> dict:
    """
    Load the output file for resume support.
    Returns an empty dict if the file does not exist or cannot be parsed.
    """
    output_file, _ = _paths()
    if not os.path.exists(output_file):
        logger.info("No existing output file — starting fresh.")
        return {}
    try:
        with open(output_file, encoding="utf-8") as f:
            data = json.load(f)
        total = sum(len(v) for v in data.values())
        logger.info(
            f"Loaded existing output: {len(data)} groups, {total} type codes"
        )
        return data
    except Exception as e:
        logger.warning(f"Could not load existing output ({e}) — starting fresh.")
        return {}


def save(data: dict):
    """
    Atomically write the output dict to disk.
    Writes to a .tmp file first, then renames — crash-safe.
    """
    output_file, output_dir = _paths()
    os.makedirs(output_dir, exist_ok=True)
    tmp = os.path.join(output_dir, ".tmp_output.json")
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        os.replace(tmp, output_file)
        logger.debug(f"Saved ({len(data)} groups) → {output_file}")
    except Exception as e:
        logger.error(f"Failed to save output: {e}")


def get_all_known_type_codes(data: dict) -> set:
    """
    Return a flat set of all type_code_full strings already in the output.
    Used at startup to skip already-collected codes on resume.
    """
    known = set()
    for group in data.values():
        for variant in group.values():
            if isinstance(variant, dict):
                tc = variant.get("type_code_full")
                if tc:
                    known.add(tc)
    return known
