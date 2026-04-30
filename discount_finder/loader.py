from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator


def iter_posts(profiles: list[dict]) -> Iterator[dict]:
    """Flatten Apify profile records into individual posts with profile context."""
    for profile in profiles:
        profile_ctx = {
            "username": profile.get("username"),
            "full_name": profile.get("fullName"),
            "followers": profile.get("followersCount"),
            "profile_pic_url": profile.get("profilePicUrlHD") or profile.get("profilePicUrl"),
            "verified": profile.get("verified", False),
            "business_category": profile.get("businessCategoryName"),
        }
        for post in profile.get("latestPosts") or []:
            yield {"post": post, "profile": profile_ctx}


def load_from_file(path: Path) -> list[dict]:
    with open(path) as f:
        data = json.load(f)
    return list(iter_posts(data))


def load_from_apify(dataset_id: str, token: str) -> list[dict]:
    """Fetch all items from an existing Apify dataset and flatten into posts."""
    from apify_client import ApifyClient

    client = ApifyClient(token)
    items = list(client.dataset(dataset_id).iterate_items())
    return list(iter_posts(items))


def filter_recent(items: list[dict], max_age_days: int) -> list[dict]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)
    out = []
    for item in items:
        ts = item["post"].get("timestamp")
        if not ts:
            continue
        when = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if when >= cutoff:
            item["post_datetime"] = when
            out.append(item)
    return out
