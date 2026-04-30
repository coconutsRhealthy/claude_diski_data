from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

from . import config, loader
from .companies import CompanyRegistry
from .prescan import is_likely_discount_post
from .registry import CodesRegistry, public_entry


_PLACEHOLDER_COMPANY_EXACT = {
    "n/a", "na", "unspecified", "various", "multiple",
    "the brand", "the shop", "brand", "shop",
}


def _is_placeholder_company(raw: str) -> bool:
    """True if the LLM emitted a generic placeholder instead of a real brand.

    Belt-and-suspenders alongside the prompt rule: catches "Unknown",
    "Unknown Electric Mop Brand", "n/a", etc., so they never reach the
    registry or the public feed.
    """
    s = (raw or "").strip().lower()
    if not s:
        return True
    if s.startswith("unknown"):
        return True
    return s in _PLACEHOLDER_COMPANY_EXACT


def _chunks(items: list, size: int):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def run(
    market: str,
    input_path: Path | None = None,
    apify_dataset_id: str | None = None,
    apify_run: bool = False,
    output_path: Path | None = None,
    max_age_days: int = config.MAX_AGE_DAYS,
    batch_size: int = config.BATCH_SIZE,
    dry_run: bool = False,
) -> dict:
    if market not in config.MARKETS:
        raise ValueError(
            f"Unknown market {market!r}. Configured markets: {config.MARKETS}."
        )

    output_path = output_path or config.output_path(market)
    public_path = config.public_output_path(market)
    codes_path = config.codes_registry_path(market)

    # 1. Resolve the input source for this market.
    if apify_run:
        from .apify_runner import run_actor_for_market

        items, dataset_id = run_actor_for_market(market)
        source = f"apify-run:{dataset_id}"
    elif apify_dataset_id:
        token = os.environ.get("APIFY_TOKEN") or os.environ.get("APIFY_KEY")
        if not token:
            raise RuntimeError("APIFY_TOKEN not set in environment (.env).")
        items = loader.load_from_apify(apify_dataset_id, token)
        source = f"apify:{apify_dataset_id}"
    elif input_path:
        items = loader.load_from_file(input_path)
        source = str(input_path)
    else:
        env_dataset = os.environ.get(config.apify_dataset_env(market))
        if env_dataset:
            token = os.environ.get("APIFY_TOKEN") or os.environ.get("APIFY_KEY")
            if not token:
                raise RuntimeError("APIFY_TOKEN not set in environment (.env).")
            items = loader.load_from_apify(env_dataset, token)
            source = f"apify:{env_dataset}"
        else:
            input_path = config.default_input_for(market)
            if input_path is None:
                raise RuntimeError(
                    f"No input source for market {market!r}. Provide --input, "
                    f"--apify-dataset, --apify-run, set ${config.apify_dataset_env(market)}, "
                    f"or drop a dataset_*.json file in inputs/{market}/."
                )
            items = loader.load_from_file(input_path)
            source = str(input_path)

    total = len(items)

    # 2. Recency filter
    items = loader.filter_recent(items, max_age_days)
    after_recency = len(items)

    # 3. Prescan (cheap regex)
    candidates = [it for it in items if is_likely_discount_post(it["post"].get("caption") or "")]
    after_prescan = len(candidates)

    print(
        f"Loaded {total} posts from {source}\n"
        f"  → {after_recency} within last {max_age_days} day(s)\n"
        f"  → {after_prescan} candidates after keyword prescan"
    )

    if dry_run:
        return {
            "total": total,
            "after_recency": after_recency,
            "after_prescan": after_prescan,
            "discount_codes": [],
        }

    # 4. LLM extraction in batches (lazy import so --dry-run works without the SDK installed)
    import anthropic

    from .analyzer import analyze_batch

    client = anthropic.Anthropic()
    registry = CompanyRegistry()
    discount_codes = []
    for batch_num, batch in enumerate(_chunks(candidates, batch_size), start=1):
        print(f"  batch {batch_num} ({len(batch)} posts)…", flush=True)
        try:
            results = analyze_batch(client, batch)
        except Exception as e:
            print(f"    !! batch failed: {e}")
            continue

        for r in results:
            if not r.get("has_discount_code"):
                continue
            idx = r["post_index"]
            if idx >= len(batch):
                continue
            item = batch[idx]
            post = item["post"]
            for code in r.get("discount_codes", []):
                raw_company = code["company"].strip()
                if _is_placeholder_company(raw_company):
                    continue
                canonical_id, display_name = registry.resolve(raw_company)
                discount_codes.append(
                    {
                        "code": code["code"].strip(),
                        "canonical_company_id": canonical_id,
                        "company": display_name,
                        "company_raw": raw_company,
                        "value": (code.get("value") or "").strip(),
                        "discount_description": code["discount_description"].strip(),
                        "percentage": code.get("percentage"),
                        "post_url": post.get("url"),
                        "post_caption": post.get("caption"),
                        "post_timestamp": post.get("timestamp"),
                        "post_image_url": post.get("displayUrl"),
                        "influencer": item["profile"],
                    }
                )

    registry.save()

    # 5. Deduplicate by (code, canonical_company_id) within this run — keep newest post.
    deduped: dict[tuple[str, str], dict] = {}
    for entry in discount_codes:
        key = (entry["code"].upper(), entry["canonical_company_id"])
        existing = deduped.get(key)
        if not existing or (entry["post_timestamp"] or "") > (existing["post_timestamp"] or ""):
            deduped[key] = entry
    final = sorted(deduped.values(), key=lambda x: x["post_timestamp"] or "", reverse=True)

    # 6. Reconcile with the persistent codes registry. Codes whose last
    #    publication is within PUBLIC_DEDUP_WINDOW_DAYS are flagged
    #    is_fresh=False and kept off the public feed.
    today = datetime.now(timezone.utc).date()
    codes_registry = CodesRegistry(path=codes_path)
    enriched = codes_registry.classify_and_update(
        final, today, config.PUBLIC_DEDUP_WINDOW_DAYS
    )
    codes_registry.save()
    fresh_count = sum(1 for e in enriched if e["is_fresh"])
    suppressed_count = len(enriched) - fresh_count

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "source": source,
        "max_age_days": max_age_days,
        "stats": {
            "posts_scanned": total,
            "posts_recent": after_recency,
            "posts_after_prescan": after_prescan,
            "codes_found": len(enriched),
            "codes_fresh": fresh_count,
            "codes_suppressed": suppressed_count,
        },
        "discount_codes": enriched,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(output, f, indent=2, default=str, ensure_ascii=False)

    # Public feed: cumulative view derived from the registry, sorted by
    # last_published_at desc (newest publications on top).
    public = {
        "generated_at": output["generated_at"],
        "discount_codes": [
            public_entry(e) for e in codes_registry.all_published_sorted()
        ],
    }
    public_path.parent.mkdir(parents=True, exist_ok=True)
    with open(public_path, "w") as f:
        json.dump(public, f, indent=2, default=str, ensure_ascii=False)

    print(
        f"\nWrote {len(enriched)} discount codes to {output_path} "
        f"({fresh_count} new/resurfaced, {suppressed_count} recent duplicates)"
    )
    print(f"Wrote public feed to {public_path} ({len(public['discount_codes'])} codes)")
    return output
