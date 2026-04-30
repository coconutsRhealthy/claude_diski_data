"""Persistent registry of every discount code ever extracted.

Tracks ``first_seen_at`` / ``last_seen_at`` / ``last_published_at`` per
``(canonical_company_id, code)``. Each pipeline run consults the registry
*after* Claude has analysed posts: a code is "fresh" (eligible for the
public feed) if it's brand new, or if its last publication is older than
``PUBLIC_DEDUP_WINDOW_DAYS``. Recent duplicates only bump ``last_seen_at``,
so the public feed doesn't re-surface them.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path

from . import config


# Generic words the LLM occasionally returns as a "value" instead of a real
# discount amount ("Coupon" for SHEIN B8QTA, etc.). They mean "I don't know" —
# render as null on the frontend.
_PLACEHOLDER_VALUES = {
    "coupon", "discount", "promo", "code", "deal",
    "sale", "offer", "voucher", "savings",
}


def public_entry(entry: dict) -> dict:
    """Trim a registry/run entry to the four fields the frontend consumes."""
    raw_value = (entry.get("value") or "").strip()
    if raw_value.lower() in _PLACEHOLDER_VALUES:
        raw_value = ""
    discount = raw_value or None

    date_str = entry.get("last_published_at")
    if not date_str:
        ts = entry.get("post_timestamp") or ""
        date_str = ts[:10] if len(ts) >= 10 else ts

    return {
        "company_id": entry.get("canonical_company_id"),
        "company": entry["company"],
        "code": entry["code"],
        "discount": discount,
        "date": date_str,
    }


class CodesRegistry:
    def __init__(self, path: Path):
        self.path = path
        self._entries: dict[str, dict] = {}
        self._load()

    @staticmethod
    def _key(entry: dict) -> str:
        return f"{entry['canonical_company_id']}:{entry['code'].upper()}"

    def _load(self) -> None:
        if not self.path.exists():
            return
        with open(self.path) as f:
            self._entries = json.load(f)

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.path, "w") as f:
            json.dump(
                self._entries,
                f,
                indent=2,
                sort_keys=True,
                default=str,
                ensure_ascii=False,
            )

    def classify_and_update(
        self,
        run_entries: list[dict],
        today: date,
        window_days: int,
    ) -> list[dict]:
        """Apply this run's findings to the registry.

        Returns each input entry annotated with ``is_fresh`` plus the
        registry timestamps, ready to be written to the run's audit file.
        """
        today_iso = today.isoformat()
        enriched: list[dict] = []
        for entry in run_entries:
            key = self._key(entry)
            existing = self._entries.get(key)

            if existing is None:
                is_fresh = True
                self._entries[key] = {
                    **entry,
                    "first_seen_at": today_iso,
                    "last_seen_at": today_iso,
                    "last_published_at": today_iso,
                }
            else:
                last_pub_iso = existing.get("last_published_at") or existing["first_seen_at"]
                last_pub = date.fromisoformat(last_pub_iso)
                is_fresh = (today - last_pub).days > window_days
                if is_fresh:
                    # Resurface: refresh metadata to the new mention,
                    # but preserve the original first_seen_at for audit.
                    self._entries[key] = {
                        **entry,
                        "first_seen_at": existing["first_seen_at"],
                        "last_seen_at": today_iso,
                        "last_published_at": today_iso,
                    }
                else:
                    # Recent duplicate: only signal that we still see it.
                    existing["last_seen_at"] = today_iso

            stored = self._entries[key]
            enriched.append(
                {
                    **entry,
                    "is_fresh": is_fresh,
                    "first_seen_at": stored["first_seen_at"],
                    "last_seen_at": stored["last_seen_at"],
                    "last_published_at": stored["last_published_at"],
                }
            )
        return enriched

    def all_published_sorted(self) -> list[dict]:
        """All stored entries sorted by ``last_published_at`` desc."""
        return sorted(
            self._entries.values(),
            key=lambda e: (
                e.get("last_published_at") or "",
                e.get("first_seen_at") or "",
            ),
            reverse=True,
        )

    def migrate_canonical(
        self, from_id: str, into_id: str, new_display_name: str
    ) -> int:
        """Re-key every entry from ``from_id`` to ``into_id``.

        Used after a company merge so the codes registry stays in sync.
        Collisions on ``(into_id, code)`` resolve to the entry with the
        newer ``last_published_at``. Returns the number of moved entries.
        """
        moved = 0
        rebuilt: dict[str, dict] = {}
        for key, entry in self._entries.items():
            if entry.get("canonical_company_id") != from_id:
                rebuilt[key] = entry
                continue
            entry["canonical_company_id"] = into_id
            entry["company"] = new_display_name
            new_key = f"{into_id}:{entry['code'].upper()}"
            existing = rebuilt.get(new_key)
            if existing is None:
                rebuilt[new_key] = entry
            else:
                # Keep the entry with the newer last_published_at; bump
                # last_seen_at to the max of the two.
                a, b = existing, entry
                pick = a if (a.get("last_published_at") or "") >= (b.get("last_published_at") or "") else b
                drop = b if pick is a else a
                pick["last_seen_at"] = max(
                    pick.get("last_seen_at") or "",
                    drop.get("last_seen_at") or "",
                )
                rebuilt[new_key] = pick
            moved += 1
        self._entries = rebuilt
        return moved


def regenerate_public_feed(market: str) -> int:
    """Rewrite the public feed for ``market`` from its codes registry.

    Used after manual maintenance (merges, prunes) so the frontend reflects
    the change without waiting for the next full pipeline run.
    """
    codes_path = config.codes_registry_path(market)
    public_path = config.public_output_path(market)
    registry = CodesRegistry(path=codes_path)
    public = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "discount_codes": [public_entry(e) for e in registry.all_published_sorted()],
    }
    public_path.parent.mkdir(parents=True, exist_ok=True)
    with open(public_path, "w") as f:
        json.dump(public, f, indent=2, default=str, ensure_ascii=False)
    return len(public["discount_codes"])
