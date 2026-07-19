"""
Hoster-Intelligenz fuer die Download-Pipeline.

Merkt sich pro Domain, ob Probes/Downloads funktionieren, und sortiert Hoster
danach. So wird der schnellste nutzbare Weg bevorzugt, ohne harte Annahmen.
"""

import json
import os
import threading
import time
from pathlib import Path
from typing import Iterable, List
from urllib.parse import urlparse

from runtime_paths import data_dir


STATE_FILE = data_dir() / ".hoster_intel.json"


BASE_SCORE = {
    "filmfrei24": 130,
    "filmfrei24 direct": 125,
    "vidara": 120,
    "firestream": 115,
    "voe": 90,
    "moflix": 86,
    "veev": 72,
    "vidoza": 80,
    "streamtape": 75,
    "doodstream": 70,
    "vidmoly": 65,
    "filemoon": 60,
    "vidsonic": 25,
    "flyfile": 20,
}

DOMAIN_SCORE = {
    "moflix-stream.click": 35,
    "moflix-stream.fans": -12,
    "moflix.rpmplay.xyz": -8,
    "moflix.upns.xyz": -8,
}

# Ein instabiler Hoster darf nicht bei jeder Episode erneut die komplette
# Browser-/yt-dlp-Kette blockieren. Harte, hosterweite Fehler öffnen den
# Circuit sofort; sonst sind drei zeitnahe Probe-Fehler nötig.
CIRCUIT_COOLDOWN_SECONDS = 20 * 60
CIRCUIT_FAILURE_WINDOW_SECONDS = 10 * 60
CIRCUIT_FAILURE_THRESHOLD = 3

# Drosselphasen kleiner Hoster sind oft nach Minuten vorbei. Die Slow-Strafe
# klingt deshalb schneller ab, und ein erfolgreicher Download mit ordentlicher
# Rate hebt sie sofort auf – sonst wird z.B. der schnelle Direct-Endpunkt
# stundenlang gemieden, obwohl er sich laengst erholt hat.
SLOW_PENALTY_HARD_SECONDS = 2 * 60 * 60
SLOW_PENALTY_SOFT_SECONDS = 6 * 60 * 60
SLOW_RECOVERY_MIN_BPS = 512 * 1024
_HARD_CIRCUIT_MARKERS = (
    "unsupported url",
    "cloudflare anti-bot",
    "anti-bot challenge",
    "certificate_verify_failed",
    "certificate verify failed",
    "self-signed certificate",
)


class HosterIntel:
    def __init__(self, path: Path = STATE_FILE):
        self.path = path
        self._lock = threading.RLock()
        self.stats = self._load()

    def rank(self, hosters: Iterable) -> List:
        return sorted(hosters, key=self.score, reverse=True)

    def score(self, hoster) -> float:
        name = (getattr(hoster, "name", "") or "").lower()
        domain = self.domain(getattr(hoster, "url", "") or "")
        data = self.stats.get(domain, {})
        name_data = self.stats.get(self.name_key(name), {})
        score = BASE_SCORE.get(name, 40)
        score += DOMAIN_SCORE.get(domain, 0)
        if getattr(hoster, "is_de", False):
            score += 12
        if getattr(hoster, "is_hd", False):
            score += 8
        # Bei serienstream ist die URL vor der Auswahl nur ein s.to-Redirect.
        # Dann existieren Domainwerte erst nach der Aufloesung; die parallel
        # gepflegte Namensstatistik (z.B. @name:voe) liefert das Lernsignal.
        learned = data if any(
            data.get(key) for key in ("ok", "fail", "download_ok", "download_fail")
        ) else name_data
        ok = learned.get("ok", 0)
        fail = learned.get("fail", 0)
        total = ok + fail
        if total:
            score += (ok / total) * 30
            score -= min(fail, 5) * 8
        if learned.get("download_ok"):
            score += 18
        if learned.get("download_fail"):
            score -= min(learned["download_fail"], 5) * 10

        speed_data = data if data.get("speed_samples") else name_data
        speed = float(speed_data.get("speed_bps_ewma", 0) or 0)
        if speed:
            if speed < 256 * 1024:
                score -= 35
            elif speed < 512 * 1024:
                score -= 22
            elif speed < 1024 * 1024:
                score -= 8
            elif speed >= 8 * 1024 * 1024:
                score += 18
            elif speed >= 4 * 1024 * 1024:
                score += 12
            elif speed >= 2 * 1024 * 1024:
                score += 6
        slow_age = time.time() - float(speed_data.get("last_slow", 0) or 0)
        if 0 <= slow_age < SLOW_PENALTY_HARD_SECONDS:
            score -= 35
        elif 0 <= slow_age < SLOW_PENALTY_SOFT_SECONDS:
            score -= 15
        return score

    def record_probe(
        self, url: str, ok: bool, message: str = "", hoster_name: str = "",
    ):
        with self._lock:
            now = time.time()
            entries = [self._entry(url)]
            if hoster_name:
                entries.append(self._name_entry(hoster_name))
            for data in entries:
                data["last_probe"] = now
                if ok:
                    data["ok"] = data.get("ok", 0) + 1
                    data["unsupported"] = False
                    data["probe_fail_streak"] = 0
                    data.pop("circuit_until", None)
                    data.pop("circuit_reason", None)
                else:
                    data["fail"] = data.get("fail", 0) + 1
                    last_failure = float(data.get("last_probe_failure", 0) or 0)
                    if now - last_failure > CIRCUIT_FAILURE_WINDOW_SECONDS:
                        data["probe_fail_streak"] = 0
                    streak = int(data.get("probe_fail_streak", 0) or 0) + 1
                    data["probe_fail_streak"] = streak
                    data["last_probe_failure"] = now
                    normalized = " ".join(str(message or "").split())
                    if self._should_open_circuit(normalized, streak):
                        data["circuit_until"] = max(
                            float(data.get("circuit_until", 0) or 0),
                            now + CIRCUIT_COOLDOWN_SECONDS,
                        )
                        data["circuit_reason"] = normalized[:160]
                    # Ein einzelner signierter Link darf nicht die komplette
                    # rotierende Hoster-Domain dauerhaft sperren. Deshalb
                    # ignoriert der Circuit einzelne 404-Antworten.
            self._save()

    def cooldown(
        self, url: str = "", hoster_name: str = "",
    ) -> tuple[int, str]:
        """Restliche temporäre Sperre für Domain oder Hostername."""
        now = time.time()
        entries = []
        domain = self.domain(url)
        if domain:
            entries.append(self.stats.get(domain, {}))
        name_key = self.name_key(hoster_name)
        if name_key:
            entries.append(self.stats.get(name_key, {}))
        blocked = max(
            entries,
            key=lambda data: float(data.get("circuit_until", 0) or 0),
            default={},
        )
        remaining = int(max(0, float(blocked.get("circuit_until", 0) or 0) - now))
        return remaining, str(blocked.get("circuit_reason", "") or "")

    @staticmethod
    def _should_open_circuit(message: str, streak: int) -> bool:
        low = message.casefold()
        if "404" in low or "not found" in low:
            return False
        if any(marker in low for marker in _HARD_CIRCUIT_MARKERS):
            return True
        return streak >= CIRCUIT_FAILURE_THRESHOLD

    def record_download(
        self,
        url: str,
        ok: bool,
        hoster_name: str = "",
        speed_bps: float = 0,
        failure_kind: str = "",
    ):
        with self._lock:
            entries = [self._entry(url)]
            if hoster_name:
                entries.append(self._name_entry(hoster_name))
            for data in entries:
                data["last_download"] = time.time()
                key = "download_ok" if ok else "download_fail"
                data[key] = data.get(key, 0) + 1
                if speed_bps and speed_bps > 0:
                    previous = float(data.get("speed_bps_ewma", 0) or 0)
                    data["speed_bps_ewma"] = (
                        float(speed_bps) if previous <= 0
                        else previous * 0.75 + float(speed_bps) * 0.25
                    )
                    data["speed_samples"] = int(data.get("speed_samples", 0) or 0) + 1
                if failure_kind == "slow":
                    data["slow"] = int(data.get("slow", 0) or 0) + 1
                    data["last_slow"] = time.time()
                elif ok and speed_bps and speed_bps >= SLOW_RECOVERY_MIN_BPS:
                    # Erholung nachgewiesen: Slow-Strafe sofort aufheben.
                    data.pop("last_slow", None)
            self._save()

    def best_label(self, hosters: Iterable) -> str:
        ranked = self.rank(hosters)
        if not ranked:
            return "kein Hoster"
        best = ranked[0]
        rest = max(0, len(ranked) - 1)
        suffix = f" +{rest}" if rest else ""
        return f"{best.name}{suffix}"

    def route_text(self, hosters: Iterable) -> str:
        ranked = self.rank(hosters)
        if not ranked:
            return "keine Route"
        return " -> ".join(h.name for h in ranked[:4])

    @staticmethod
    def domain(url: str) -> str:
        return urlparse(url).netloc.lower()

    @staticmethod
    def name_key(name: str) -> str:
        normalized = "".join(ch for ch in str(name or "").casefold() if ch.isalnum())
        return f"@name:{normalized}" if normalized else ""

    def _entry(self, url: str) -> dict:
        domain = self.domain(url)
        return self.stats.setdefault(domain, {})

    def _name_entry(self, name: str) -> dict:
        key = self.name_key(name)
        return self.stats.setdefault(key, {}) if key else {}

    def _load(self) -> dict:
        try:
            if self.path.exists():
                data = json.loads(self.path.read_text(encoding="utf-8"))
                return data if isinstance(data, dict) else {}
        except Exception:
            pass
        return {}

    def _save(self):
        tmp = self.path.with_name(f".{self.path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with tmp.open("w", encoding="utf-8") as file:
                file.write(json.dumps(self.stats, indent=2))
                file.flush()
                os.fsync(file.fileno())
            os.replace(tmp, self.path)
        except Exception:
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass
