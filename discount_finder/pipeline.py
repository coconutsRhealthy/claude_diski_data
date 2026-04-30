from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

from . import config, loader
from .companies import CompanyRegistry
from .prescan import is_likely_discount_post


def _chunks(items: list, size: int):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def run(
    input_path: Path | None = None,
    apify_dataset_id: str | None = None,
    output_path: Path | None = None,
    max_age_days: int = config.MAX_AGE_DAYS,
    batch_size: int = config.BATCH_SIZE,
    dry_run: bool = False,
) -> dict:
    output_path = output_path or config.OUTPUT_PATH

    # 1. Load
    if apify_dataset_id:
        token = os.environ["APIFY_API_TOKEN"]
        items = loader.load_from_apify(apify_dataset_id, token)
        source = f"apify:{apify_dataset_id}"
    else:
        path = input_path or config.DEFAULT_INPUT_PATH
        items = loader.load_from_file(path)
        source = str(path)

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

    # 5. Deduplicate by (code, canonical_company_id) — keep newest post.
    deduped: dict[tuple[str, str], dict] = {}
    for entry in discount_codes:
        key = (entry["code"].upper(), entry["canonical_company_id"])
        existing = deduped.get(key)
        if not existing or (entry["post_timestamp"] or "") > (existing["post_timestamp"] or ""):
            deduped[key] = entry
    final = sorted(deduped.values(), key=lambda x: x["post_timestamp"] or "", reverse=True)

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "source": source,
        "max_age_days": max_age_days,
        "stats": {
            "posts_scanned": total,
            "posts_recent": after_recency,
            "posts_after_prescan": after_prescan,
            "codes_found": len(final),
        },
        "discount_codes": final,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(output, f, indent=2, default=str, ensure_ascii=False)

    # Trimmed feed for the frontend — the full file stays as the source of truth.
    public = {
        "generated_at": output["generated_at"],
        "discount_codes": [_public_entry(e) for e in final],
    }
    public_path = config.PUBLIC_OUTPUT_PATH
    public_path.parent.mkdir(parents=True, exist_ok=True)
    with open(public_path, "w") as f:
        json.dump(public, f, indent=2, default=str, ensure_ascii=False)

    print(f"\nWrote {len(final)} discount codes to {output_path}")
    print(f"Wrote public feed to {public_path}")
    return output


def _public_entry(entry: dict) -> dict:
    """Trim a full entry to the four fields the frontend consumes."""
    # Prefer the LLM's short `value`; fall back to a percentage or the long
    # description so older entries written before `value` existed still render.
    value = entry.get("value") or ""
    if not value:
        pct = entry.get("percentage")
        value = f"{pct}%" if isinstance(pct, int) else (entry.get("discount_description") or "")

    ts = entry.get("post_timestamp") or ""
    date = ts[:10] if len(ts) >= 10 else ts  # ISO timestamp → "YYYY-MM-DD"

    return {
        "company": entry["company"],
        "code": entry["code"],
        "discount": value,
        "date": date,
    }
