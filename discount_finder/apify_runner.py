"""Trigger the Apify Instagram scraper actor for a market.

Mirrors the setup used in the other diski project: same actor
(``apify/instagram-scraper``), same run_input including resultsLimit=5
and resultsType=details. ``client.actor(...).call()`` blocks until the
run finishes — the SDK polls internally — so no manual loop here.

Per-run handle selection: instead of scraping every URL in
``inputs/<market>/influencers.txt``, we pick up to
``config.SCRAPE_TARGET_DEFAULT`` handles from the live pool
(``data/<market>/handles.json`` ∪ ``inputs/<market>/handles_pool.txt``)
using ``selector.select_handles``. See ``selector.py`` for the tiered
policy.
"""

from __future__ import annotations

import os
import re
from datetime import date
from pathlib import Path

from . import config
from .handles import HandlesRegistry, merge_pool_file
from .loader import iter_posts
from .selector import select_handles


APIFY_ACTOR = "apify/instagram-scraper"

# Default actor run_input. Match the configuration used in the other diski
# project — keep in sync if it changes there.
DEFAULT_RUN_INPUT = {
    "addParentData": False,
    "enhanceUserSearchWithFacebookPage": False,
    "isUserReelFeedURL": False,
    "isUserTaggedFeedURL": False,
    "resultsLimit": 5,
    "resultsType": "details",
    "searchType": "hashtag",
}

_IG_HANDLE_RE = re.compile(r"instagram\.com/([^/?#\s]+)", re.I)
_RESERVED_HANDLES = {"p", "reel", "reels", "explore", "stories", "tv", "accounts", "direct"}


def _parse_handle_from_url(url: str) -> str | None:
    m = _IG_HANDLE_RE.search(url or "")
    if not m:
        return None
    h = m.group(1).strip().lower()
    if not h or h in _RESERVED_HANDLES:
        return None
    return h


def load_urls(market: str) -> list[str]:
    """Legacy: read the full URL list from inputs/<market>/influencers.txt.

    Kept as a bootstrap source for ``handles_pool.txt`` and as a last-resort
    fallback when both the pool file and ``handles.json`` are empty.
    """
    path = config.ROOT / "inputs" / market / "influencers.txt"
    if not path.exists():
        raise RuntimeError(
            f"No URL list at {path}. Generate one (e.g. from your influencers DB) before --apify-run."
        )
    urls = [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    if not urls:
        raise RuntimeError(f"URL list at {path} is empty.")
    return urls


def _bootstrap_pool_file_from_influencers(market: str, pool_path: Path) -> int:
    """Seed handles_pool.txt from influencers.txt on first run.

    One-shot: if the pool file already exists we leave it alone. Returns the
    number of handles written (0 if no bootstrap was needed or possible).
    """
    if pool_path.exists():
        return 0
    influencers = config.ROOT / "inputs" / market / "influencers.txt"
    if not influencers.exists():
        return 0
    handles: list[str] = []
    seen: set[str] = set()
    for line in influencers.read_text(encoding="utf-8").splitlines():
        h = _parse_handle_from_url(line.strip())
        if h and h not in seen:
            seen.add(h)
            handles.append(h)
    if not handles:
        return 0
    pool_path.parent.mkdir(parents=True, exist_ok=True)
    with pool_path.open("w", encoding="utf-8") as f:
        f.write("# Curated pool of Instagram handles for this market.\n")
        f.write("# One bare handle per line. Blank lines and # comments ignored.\n")
        f.write(f"# Initial seed: derived from influencers.txt ({len(handles)} handles).\n")
        for h in handles:
            f.write(h + "\n")
    print(
        f"Bootstrapped {pool_path} with {len(handles)} handle(s) from influencers.txt.",
        flush=True,
    )
    return len(handles)


def _resolve_token() -> str:
    token = os.environ.get("APIFY_TOKEN") or os.environ.get("APIFY_KEY")
    if not token:
        raise RuntimeError("APIFY_TOKEN not set in environment (.env).")
    return token


def select_and_record(
    market: str,
    *,
    today: date,
    target: int = config.SCRAPE_TARGET_DEFAULT,
) -> list[str]:
    """Pick this run's handles, record the scrape attempt, return the handles.

    1. Load ``handles.json`` for the market.
    2. Bootstrap ``handles_pool.txt`` from ``influencers.txt`` if missing.
    3. Merge pool file into the registry (new handles get
       ``source="pool"``, counters at 0).
    4. Run the tiered selector.
    5. Mark each selected handle as attempted (``runs_scraped += 1``,
       ``last_run_at = today``) and persist.
    """
    pool_path = config.handles_pool_path(market)
    _bootstrap_pool_file_from_influencers(market, pool_path)

    registry = HandlesRegistry(config.handles_registry_path(market))
    added = merge_pool_file(registry, pool_path, today)
    if added:
        print(f"Pool merge: {added} new handle(s) from {pool_path}.", flush=True)

    if not registry.entries():
        raise RuntimeError(
            f"No handles available for market {market!r}: both "
            f"{config.handles_registry_path(market)} and {pool_path} are empty."
        )

    selected = select_handles(registry, target=target, today=today)
    if not selected:
        raise RuntimeError(
            f"Selector returned 0 handles for market {market!r}. "
            "Check handles.json contents."
        )

    registry.record_attempt(selected, today)
    registry.save()
    return selected


def run_actor_for_market(market: str) -> tuple[list[dict], str]:
    """Trigger the Instagram scraper for ``market`` and return ``(items, dataset_id)``.

    ``items`` is the flattened post list — same shape as
    ``loader.load_from_file`` and ``loader.load_from_apify``.
    """
    from apify_client import ApifyClient

    today = date.today()
    handles = select_and_record(market, today=today)
    urls = [f"https://www.instagram.com/{h}/" for h in handles]
    run_input = {**DEFAULT_RUN_INPUT, "directUrls": urls}

    client = ApifyClient(_resolve_token())
    print(
        f"Triggering {APIFY_ACTOR} for market={market!r} with {len(urls)} URLs "
        f"(resultsLimit={run_input['resultsLimit']})…",
        flush=True,
    )
    run = client.actor(APIFY_ACTOR).call(run_input=run_input)
    # apify-client 3.x returns a pydantic ActorRun model; older versions
    # returned a dict. Support both so a future regression in either
    # direction doesn't take the pipeline down again.
    dataset_id = (
        run.default_dataset_id if hasattr(run, "default_dataset_id")
        else run["defaultDatasetId"]
    )
    print(f"Apify run finished. Fetching dataset {dataset_id}…", flush=True)
    raw_items = list(client.dataset(dataset_id).iterate_items())
    return list(iter_posts(raw_items)), dataset_id
