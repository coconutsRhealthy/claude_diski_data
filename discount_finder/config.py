from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# Claude model. Haiku 4.5 handles structured extraction reliably at a fraction
# of Opus pricing ($1/$5 per 1M tokens vs $5/$25); switch back to claude-opus-4-7
# for spot-checks or if Haiku misses too many multilingual edge cases.
MODEL = "claude-haiku-4-5"

# Only consider posts newer than this many days.
MAX_AGE_DAYS = 3

# How many posts to send to Claude in one request. Bigger batches = fewer
# requests but slower per-call latency and higher single-call failure cost.
BATCH_SIZE = 10

# Truncate captions before sending to Claude (chars). Most discount-code
# context lives in the first ~1500 chars; trimming saves input tokens.
CAPTION_MAX_CHARS = 1500

# Markets the pipeline knows about. Add a country here and you can
# immediately run `python main.py --market <name>`. Folder layout per market:
#   inputs/<market>/dataset_*.json      Apify exports (gitignored)
#   data/<market>/codes.json            persistent codes registry
#   output/<market>/discount_codes*.json full + public outputs
MARKETS: list[str] = ["germany", "belgium"]

# Shared across all markets — brand identity is universal, so Shein in
# Germany and Shein in Belgium share one canonical id.
COMPANIES_REGISTRY_PATH = ROOT / "data" / "companies.json"

# A code re-extracted within this many days of its last publication is treated
# as a recent duplicate and kept off the public feed (its position on the feed
# stays unchanged). After the window it can resurface and jump back to the top.
PUBLIC_DEDUP_WINDOW_DAYS = 20


def codes_registry_path(market: str) -> Path:
    return ROOT / "data" / market / "codes.json"


def public_output_path(market: str) -> Path:
    return ROOT / "output" / market / "discount_codes_public.json"


def apify_dataset_env(market: str) -> str:
    """Env var name where the Apify dataset id for ``market`` is read from."""
    return f"APIFY_DATASET_ID_{market.upper()}"


def default_input_for(market: str) -> Path | None:
    """Latest ``dataset_*.json`` in ``inputs/<market>/``, if any.

    Used when neither ``--input`` nor ``--apify-dataset`` is supplied.
    """
    folder = ROOT / "inputs" / market
    if not folder.exists():
        return None
    candidates = sorted(
        folder.glob("dataset_*.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None
