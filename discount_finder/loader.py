from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator

import requests


def _iter_posts(profiles: list[dict]) -> Iterator[dict]:
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
    return list(_iter_posts(data))


def load_from_apify(dataset_id: str, token: str) -> list[dict]:
    """Fetch the latest items from an Apify dataset."""
    url = f"https://api.apify.com/v2/datasets/{dataset_id}/items"
    resp = requests.get(url, params={"token": token, "clean": "true"}, timeout=120)
    resp.raise_for_status()
    return list(_iter_posts(resp.json()))


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
