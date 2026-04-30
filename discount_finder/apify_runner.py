"""Trigger the Apify Instagram scraper actor for a market.

Mirrors the setup used in the other diski project: same actor
(``apify/instagram-scraper``), same run_input including resultsLimit=5
and resultsType=details. ``client.actor(...).call()`` blocks until the
run finishes — the SDK polls internally — so no manual loop here.
"""

from __future__ import annotations

import os

from . import config
from .loader import iter_posts


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


def load_urls(market: str) -> list[str]:
    """Read the Instagram URL list for ``market`` from inputs/<market>/influencers.txt."""
    path = config.ROOT / "inputs" / market / "influencers.txt"
    if not path.exists():
        raise RuntimeError(
            f"No URL list at {path}. Generate one (e.g. from your influencers DB) before --apify-run."
        )
    urls = [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    if not urls:
        raise RuntimeError(f"URL list at {path} is empty.")
    return urls


def _resolve_token() -> str:
    token = os.environ.get("APIFY_TOKEN") or os.environ.get("APIFY_KEY")
    if not token:
        raise RuntimeError("APIFY_TOKEN not set in environment (.env).")
    return token


def run_actor_for_market(market: str) -> tuple[list[dict], str]:
    """Trigger the Instagram scraper for ``market`` and return ``(items, dataset_id)``.

    ``items`` is the flattened post list — same shape as
    ``loader.load_from_file`` and ``loader.load_from_apify``.
    """
    from apify_client import ApifyClient

    urls = load_urls(market)
    run_input = {**DEFAULT_RUN_INPUT, "directUrls": urls}

    client = ApifyClient(_resolve_token())
    print(
        f"Triggering {APIFY_ACTOR} for market={market!r} with {len(urls)} URLs "
        f"(resultsLimit={run_input['resultsLimit']})…",
        flush=True,
    )
    run = client.actor(APIFY_ACTOR).call(run_input=run_input)
    dataset_id = run["defaultDatasetId"]
    print(f"Apify run finished. Fetching dataset {dataset_id}…", flush=True)
    raw_items = list(client.dataset(dataset_id).iterate_items())
    return list(iter_posts(raw_items)), dataset_id
