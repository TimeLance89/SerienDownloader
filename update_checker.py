"""Vergleicht den lokalen Build mit dem neuesten Stand des GitHub-Repositories."""

import os
import re
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import quote

import requests


DEFAULT_REPOSITORY = "TimeLance89/SerienDownloader"
DEFAULT_BRANCH = "main"
_COMMIT_RE = re.compile(r"^[0-9a-f]{7,40}$", re.IGNORECASE)


def _valid_commit(value: str) -> str:
    value = str(value or "").strip()
    return value if _COMMIT_RE.fullmatch(value) else ""


def detect_local_commit(app_dir: Optional[Path] = None) -> str:
    """Liest die Build-Revision aus der Umgebung oder aus einem Git-Checkout."""
    for key in ("APP_COMMIT_SHA", "GIT_COMMIT", "SOURCE_COMMIT"):
        commit = _valid_commit(os.environ.get(key, ""))
        if commit:
            return commit

    root = Path(app_dir or Path(__file__).resolve().parent)
    marker = root / ".git"
    if marker.is_file():
        try:
            raw = marker.read_text(encoding="utf-8").strip()
            if raw.startswith("gitdir:"):
                marker = (root / raw.split(":", 1)[1].strip()).resolve()
        except OSError:
            return ""
    if not marker.is_dir():
        return ""

    try:
        head = (marker / "HEAD").read_text(encoding="utf-8").strip()
    except OSError:
        return ""
    direct = _valid_commit(head)
    if direct:
        return direct
    if not head.startswith("ref:"):
        return ""
    ref = head.split(":", 1)[1].strip()
    try:
        direct = _valid_commit((marker / ref).read_text(encoding="utf-8").strip())
        if direct:
            return direct
    except OSError:
        pass
    try:
        for line in (marker / "packed-refs").read_text(encoding="utf-8").splitlines():
            if line.startswith(("#", "^")):
                continue
            commit, _, packed_ref = line.partition(" ")
            if packed_ref == ref:
                return _valid_commit(commit)
    except OSError:
        pass
    return ""


class UpdateChecker:
    def __init__(
        self,
        repository: str = DEFAULT_REPOSITORY,
        branch: str = DEFAULT_BRANCH,
        app_dir: Optional[Path] = None,
        cache_seconds: int = 600,
    ):
        self.repository = (
            repository
            if re.fullmatch(r"[\w.-]+/[\w.-]+", repository)
            else DEFAULT_REPOSITORY
        )
        self.branch = branch.strip() or DEFAULT_BRANCH
        self.app_dir = Path(app_dir or Path(__file__).resolve().parent)
        self.cache_seconds = max(0, int(cache_seconds))
        self._cache: Optional[dict] = None
        self._cache_time = 0.0
        self._lock = threading.Lock()

    @property
    def repository_url(self) -> str:
        return f"https://github.com/{self.repository}"

    def _get_json(self, path: str) -> dict:
        response = requests.get(
            f"https://api.github.com/repos/{self.repository}/{path}",
            headers={
                "Accept": "application/vnd.github+json",
                "User-Agent": "Royal-Downloader-Updater",
                "X-GitHub-Api-Version": "2022-11-28",
            },
            timeout=10,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise RuntimeError("GitHub hat eine ungültige Antwort geliefert")
        return payload

    def _check_uncached(self) -> dict:
        current = detect_local_commit(self.app_dir)
        base = {
            "repository": self.repository,
            "repository_url": self.repository_url,
            "branch": self.branch,
            "current_sha": current,
            "latest_sha": "",
            "latest_url": self.repository_url,
            "latest_message": "",
            "comparison": "unknown",
            "update_available": None,
            "ahead_by": 0,
            "behind_by": 0,
            "checked_at": datetime.now(timezone.utc).isoformat(),
            "error": "",
        }
        try:
            latest = self._get_json(f"commits/{quote(self.branch, safe='')}")
            latest_sha = _valid_commit(latest.get("sha", ""))
            if not latest_sha:
                raise RuntimeError("GitHub lieferte keine gültige Revision")
            commit_data = latest.get("commit") or {}
            base.update({
                "latest_sha": latest_sha,
                "latest_url": latest.get("html_url") or self.repository_url,
                "latest_message": str(commit_data.get("message") or "").splitlines()[0],
            })
            if not current:
                return base
            if current == latest_sha:
                base.update({"comparison": "identical", "update_available": False})
                return base

            comparison = self._get_json(
                f"compare/{quote(current, safe='')}...{quote(latest_sha, safe='')}",
            )
            status = str(comparison.get("status") or "unknown")
            ahead_by = max(0, int(comparison.get("ahead_by") or 0))
            behind_by = max(0, int(comparison.get("behind_by") or 0))
            base.update({
                "comparison": status,
                "update_available": status in {"ahead", "diverged"} and ahead_by > 0,
                "ahead_by": ahead_by,
                "behind_by": behind_by,
            })
            return base
        except (requests.RequestException, RuntimeError, TypeError, ValueError) as exc:
            base["error"] = str(exc)[:240]
            return base

    def check(self, force: bool = False) -> dict:
        with self._lock:
            now = time.monotonic()
            if (
                not force
                and self._cache is not None
                and (now - self._cache_time) < self.cache_seconds
            ):
                return dict(self._cache)
            result = self._check_uncached()
            self._cache = dict(result)
            self._cache_time = now
            return result
