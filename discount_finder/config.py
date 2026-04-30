from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# Claude model. claude-opus-4-7 is the default; for high-volume extraction
# claude-haiku-4-5 is a strong cost/perf trade-off ($1/$5 per 1M tokens vs $5/$25).
MODEL = "claude-opus-4-7"

# Only consider posts newer than this many days.
MAX_AGE_DAYS = 3

# How many posts to send to Claude in one request. Bigger batches = fewer
# requests but slower per-call latency and higher single-call failure cost.
BATCH_SIZE = 10

# Truncate captions before sending to Claude (chars). Most discount-code
# context lives in the first ~1500 chars; trimming saves input tokens.
CAPTION_MAX_CHARS = 1500

# Full pipeline output (everything we know about each code).
OUTPUT_PATH = ROOT / "output" / "discount_codes.json"

# Trimmed file the frontend consumes — derived from OUTPUT_PATH.
PUBLIC_OUTPUT_PATH = ROOT / "output" / "discount_codes_public.json"

# Default local input file (export from Apify Instagram scraper).
DEFAULT_INPUT_PATH = ROOT / "dataset_instagram-scraper_2026-04-29_03-34-34-770.json"
