"""Persistent registry of every Instagram handle in our pool.

Counterpart to ``registry.CodesRegistry``. Tracks per-handle history:
when we first saw it, when we last asked Apify to scrape it, how many
times we've scraped it, when we last extracted a code from it, and how
many codes total. The selector consumes this metadata to decide which
handles to scrape each run.

Handle keys are bare lowercase usernames (e.g. ``"0nlysale"``), matching
the existing handles.json shape seeded externally on the droplet.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Iterable


class HandlesRegistry:
    def __init__(self, path: Path):
        self.path = path
        self._entries: dict[str, dict] = {}
        self._load()

    @staticmethod
    def _key(handle: str) -> str:
        return handle.strip().lower().lstrip("@")

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

    def entries(self) -> dict[str, dict]:
        """Live view of the underlying dict — read-only by convention."""
        return self._entries

    def ensure(self, handle: str, *, source: str, today: date) -> dict:
        """Add a handle if missing; return its entry either way.

        Preserves any pre-existing fields (e.g. an ``awin`` block seeded
        from an Awin import) for handles already present.
        """
        key = self._key(handle)
        if not key:
            raise ValueError(f"Empty handle: {handle!r}")
        entry = self._entries.get(key)
        if entry is None:
            entry = {
                "handle": key,
                "source": source,
                "first_seen_at": today.isoformat(),
                "last_run_at": None,
                "last_code_seen_at": None,
                "runs_scraped": 0,
                "codes_found": 0,
            }
            self._entries[key] = entry
        return entry

    def record_attempt(self, handles: Iterable[str], today: date) -> int:
        """Mark each handle as scraped this run.

        Bumps ``runs_scraped`` and sets ``last_run_at`` to today. Creates
        the entry first (with source="run") if for some reason it's missing.
        Returns the number of handles touched.
        """
        today_iso = today.isoformat()
        n = 0
        for h in handles:
            entry = self.ensure(h, source="run", today=today)
            entry["runs_scraped"] = (entry.get("runs_scraped") or 0) + 1
            entry["last_run_at"] = today_iso
            n += 1
        return n

    def record_codes(self, per_handle: dict[str, int], today: date) -> int:
        """Bump ``codes_found`` and set ``last_code_seen_at`` per handle.

        ``per_handle`` maps handle → number of codes extracted from that
        handle this run. Defensive: creates the entry (source="code") if
        a code surfaces from a handle we somehow never registered.
        Returns the number of handles touched.
        """
        today_iso = today.isoformat()
        n = 0
        for h, count in per_handle.items():
            if count <= 0:
                continue
            entry = self.ensure(h, source="code", today=today)
            entry["codes_found"] = (entry.get("codes_found") or 0) + count
            entry["last_code_seen_at"] = today_iso
            n += 1
        return n


def merge_pool_file(
    registry: HandlesRegistry, pool_path: Path, today: date
) -> int:
    """Add every handle from ``pool_path`` to ``registry`` if missing.

    Pool file format: one bare handle per line. Blank lines and lines
    starting with ``#`` are ignored. Returns the number of new entries
    added (existing entries are untouched).
    """
    if not pool_path.exists():
        return 0
    before = len(registry.entries())
    with pool_path.open(encoding="utf-8") as f:
        for line in f:
            handle = line.strip()
            if not handle or handle.startswith("#"):
                continue
            registry.ensure(handle, source="pool", today=today)
    return len(registry.entries()) - before
