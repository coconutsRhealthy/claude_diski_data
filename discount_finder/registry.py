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
from datetime import date
from pathlib import Path

from . import config


class CodesRegistry:
    def __init__(self, path: Path = config.CODES_REGISTRY_PATH):
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
