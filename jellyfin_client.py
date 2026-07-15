"""
Prüft, ob ein gesuchter Film bereits in der eigenen Jellyfin-Bibliothek
(NAS) vorhanden ist, bevor er zur Warteschlange hinzugefügt wird.

Nutzt die Jellyfin-REST-API (/Items?IncludeItemTypes=Movie) mit einem
API-Key (Dashboard -> Erweitert -> API-Schlüssel). Reine stdlib-
Implementierung (urllib), da Jellyfin im lokalen Netz läuft und keine
Cloudflare-/TLS-Tricks nötig sind.
"""

import json
import logging
import re
import unicodedata
import urllib.request
from typing import List, Optional
from urllib.parse import urlencode

logger = logging.getLogger(__name__)


def _normalize(title: str) -> str:
    title = re.sub(r"\s*[\(\[]?(?:19|20)\d{2}[\)\]]?\s*$", "", title or "")
    ascii_title = unicodedata.normalize("NFKD", title).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]+", "", ascii_title.casefold())


_PART_MARKERS = {"teil", "part", "chapter", "kapitel", "volume", "vol"}
_PART_NUMBERS = {
    "one": "1", "eins": "1", "i": "1",
    "two": "2", "zwei": "2", "ii": "2",
    "three": "3", "drei": "3", "iii": "3",
    "four": "4", "vier": "4", "iv": "4",
    "five": "5", "funf": "5", "v": "5",
}


def _title_tokens(title: str) -> List[str]:
    ascii_title = unicodedata.normalize("NFKD", title or "").encode("ascii", "ignore").decode("ascii")
    return re.findall(r"[a-z0-9]+", ascii_title.casefold())


def _installment_parts(title: str) -> tuple[Optional[str], set[str]]:
    tokens = _title_tokens(title)
    for index, token in enumerate(tokens[:-1]):
        if token not in _PART_MARKERS:
            continue
        number = _PART_NUMBERS.get(tokens[index + 1], tokens[index + 1])
        base = set(tokens[:index] + tokens[index + 2:])
        return number, base
    return None, set(tokens)


def _same_installment_title(wanted: str, candidate: str) -> bool:
    """Erkennt verkürzte Untertitel nur bei identischer Teilnummer."""
    wanted_part, wanted_base = _installment_parts(wanted)
    candidate_part, candidate_base = _installment_parts(candidate)
    return bool(
        wanted_part
        and wanted_part == candidate_part
        and len(wanted_base) >= 2
        and wanted_base <= candidate_base
    )


class JellyfinClient:
    def __init__(self, base_url: str = "", api_key: str = "", timeout: float = 5.0):
        self.base_url = (base_url or "").rstrip("/")
        self.api_key = (api_key or "").strip()
        self.timeout = timeout

    @property
    def configured(self) -> bool:
        return bool(self.base_url and self.api_key)

    def _list_items(self, params: dict, page_size: int, label: str) -> Optional[List[dict]]:
        """Liest /Items vollständig; Jellyfin begrenzt große Antworten serverseitig."""
        if not self.configured:
            return []
        page_size = max(1, min(int(page_size or 1000), 5000))
        start = 0
        result: List[dict] = []
        while True:
            page_params = dict(params)
            page_params.update({"StartIndex": str(start), "Limit": str(page_size)})
            req = urllib.request.Request(
                f"{self.base_url}/Items?{urlencode(page_params)}",
                headers={"X-Emby-Token": self.api_key},
            )
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                    data = json.loads(resp.read().decode("utf-8"))
            except Exception as exc:
                logger.warning("%s fehlgeschlagen (%s): %s", label, self.base_url, exc)
                return None
            page = data.get("Items") or []
            if not isinstance(page, list):
                logger.warning("%s lieferte ungültige Daten (%s)", label, self.base_url)
                return None
            result.extend(page)
            start += len(page)
            try:
                total = int(data.get("TotalRecordCount"))
            except (TypeError, ValueError):
                total = None
            if (
                not page
                or (total is not None and start >= total)
                or (total is None and len(page) < page_size)
            ):
                break
        return result

    def list_movies(self, limit: int = 1000) -> Optional[List[dict]]:
        """Liefert die komplette Filmbibliothek (für Duplikat-Checks von
        Listen wie Neu/Top/Genre, bei denen kein gemeinsamer Suchbegriff existiert)."""
        items = self._list_items({
            "IncludeItemTypes": "Movie",
            "Recursive": "true",
            # Sonst blendet Jellyfin einzelne Filme aus Collections/Boxsets
            # aus. Diese Filme sind in der UI sichtbar, fehlen aber in /Items.
            "CollapseBoxSetItems": "false",
            "Fields": "ProductionYear,OriginalTitle,SortName,ProviderIds",
        }, limit, "Jellyfin-Bibliotheksabruf")
        if items is None:
            return None
        return [{
            "name": it.get("Name", ""),
            "original_title": it.get("OriginalTitle", ""),
            "sort_name": it.get("SortName", ""),
            "year": it.get("ProductionYear"),
            "tmdb_id": str(
                (it.get("ProviderIds") or {}).get("Tmdb")
                or (it.get("ProviderIds") or {}).get("TheMovieDb")
                or ""
            ),
        } for it in items]

    def match(
        self, title: str, year: str = "", items: Optional[List[dict]] = None, tmdb_id="",
    ) -> bool:
        candidates = items if items is not None else self.list_movies()
        if candidates is None:
            return False
        wanted_tmdb = str(tmdb_id or "").strip()
        if wanted_tmdb and any(str(item.get("tmdb_id") or "") == wanted_tmdb for item in candidates):
            return True
        norm_title = _normalize(title)
        if not norm_title:
            return False
        title_matches = []
        for it in candidates:
            aliases = (it.get("name"), it.get("original_title"), it.get("sort_name"))
            if any(
                norm_title == _normalize(alias) or _same_installment_title(title, alias)
                for alias in aliases if alias
            ):
                title_matches.append(it)
        if not title_matches:
            return False
        if wanted_tmdb and any(item.get("tmdb_id") for item in title_matches):
            # Gleicher Titel, aber eine andere stabile ID: nicht vermischen.
            return False
        if not year:
            return True
        if any(it.get("year") and str(it["year"]) == str(year) for it in title_matches):
            return True
        # Gleichnamige Remakes dürfen bei abweichendem Jahr nicht vermischt werden.
        return False

    def refresh_library(self) -> bool:
        """Startet einen Jellyfin-Bibliotheksscan."""
        if not self.configured:
            return False
        url = f"{self.base_url}/Library/Refresh"
        req = urllib.request.Request(
            url, data=b"", method="POST", headers={"X-Emby-Token": self.api_key},
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout):
                return True
        except Exception as exc:
            logger.warning("Jellyfin-Bibliotheksscan fehlgeschlagen (%s): %s", self.base_url, exc)
            return False

    def list_users(self) -> Optional[List[dict]]:
        """Liefert auswählbare Jellyfin-Benutzer für den Gesehen-Status."""
        if not self.configured:
            return []
        req = urllib.request.Request(
            f"{self.base_url}/Users", headers={"X-Emby-Token": self.api_key},
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except Exception as exc:
            logger.warning("Jellyfin-Benutzerabruf fehlgeschlagen (%s): %s", self.base_url, exc)
            return None
        return [
            {"id": str(user.get("Id", "")), "name": str(user.get("Name", ""))}
            for user in data
            if user.get("Id") and not (user.get("Policy") or {}).get("IsDisabled", False)
        ]

    def list_episodes(self, limit: int = 1000) -> Optional[List[dict]]:
        """Liefert alle Episoden aus Jellyfin: [{"series", "season", "episode"}, ...].
        Damit lässt sich prüfen, ob eine neu gefundene Episode einer
        abonnierten Serie bereits in der Bibliothek vorhanden ist – ohne
        das wüsste die Watchlist-Prüfung nur, dass die Quelle (Filmpalast/
        Moflix) einen neuen Episoden-Slug gescraped hat, nicht ob die
        Episode für den Nutzer überhaupt neu/fehlend ist."""
        items = self._list_items({
            "IncludeItemTypes": "Episode",
            "Recursive": "true",
            "Fields": "ParentIndexNumber,IndexNumber,SeriesName,SeriesId",
        }, limit, "Jellyfin-Episodenabruf")
        if items is None:
            return None
        result = []
        for it in items:
            season = it.get("ParentIndexNumber")
            episode = it.get("IndexNumber")
            series = it.get("SeriesName")
            if season is None or episode is None or not series:
                continue
            result.append({
                "series": series,
                "series_id": str(it.get("SeriesId") or ""),
                "season": season,
                "episode": episode,
            })
        return result

    def list_series(self, limit: int = 1000) -> Optional[List[dict]]:
        """Liefert Serien-IDs und Provider-IDs für stabile Zuordnungen."""
        items = self._list_items({
            "IncludeItemTypes": "Series",
            "Recursive": "true",
            "Fields": "ProviderIds,OriginalTitle,SortName",
        }, limit, "Jellyfin-Serienabruf")
        if items is None:
            return None
        result = []
        for item in items:
            provider_ids = item.get("ProviderIds") or {}
            tmdb_id = provider_ids.get("Tmdb") or provider_ids.get("TheMovieDb") or ""
            result.append({
                "id": str(item.get("Id") or ""),
                "name": str(item.get("Name") or ""),
                "original_title": str(item.get("OriginalTitle") or ""),
                "sort_name": str(item.get("SortName") or ""),
                "tmdb_id": str(tmdb_id),
            })
        return result

    def list_episodes_with_user_data(
        self, user_id: str, limit: int = 1000,
    ) -> Optional[List[dict]]:
        """Liefert Episoden inklusive benutzerbezogenem ``played``-Status.

        ``None`` signalisiert eine fehlende Konfiguration oder einen Abruffehler.
        So startet die Abo-Automatik bei unsicherem Gesehen-Status nicht weiter.
        """
        user_id = (user_id or "").strip()
        if not self.configured or not user_id:
            return None
        items = self._list_items({
            "UserId": user_id,
            "IncludeItemTypes": "Episode",
            "Recursive": "true",
            "EnableUserData": "true",
            "Fields": "ParentIndexNumber,IndexNumber,SeriesName,SeriesId",
        }, limit, "Jellyfin-Gesehen-Status")
        if items is None:
            return None
        result = []
        for item in items:
            season = item.get("ParentIndexNumber")
            episode = item.get("IndexNumber")
            series = item.get("SeriesName")
            if season is None or episode is None or not series:
                continue
            result.append({
                "series": series,
                "series_id": str(item.get("SeriesId") or ""),
                "season": int(season),
                "episode": int(episode),
                "played": bool((item.get("UserData") or {}).get("Played", False)),
            })
        return result

    def has_episode(
        self, series_title: str, season: int, episode: int,
        items: Optional[List[dict]] = None, aliases=(), series_ids=None,
    ) -> bool:
        candidates = items if items is not None else self.list_episodes()
        if candidates is None:
            return False
        normalized_titles = {
            _normalize(title) for title in (series_title, *aliases) if _normalize(title)
        }
        wanted_ids = (
            {str(series_id) for series_id in series_ids if series_id}
            if series_ids is not None else None
        )
        if not normalized_titles and wanted_ids is None:
            return False
        for it in candidates:
            identity_matches = (
                str(it.get("series_id") or "") in wanted_ids
                if wanted_ids is not None
                else _normalize(it.get("series", "")) in normalized_titles
            )
            if it["season"] == season and it["episode"] == episode and identity_matches:
                return True
        return False

    def episodes_for_series(
        self, series_title: str, items: Optional[List[dict]] = None,
        aliases=(), series_ids=None,
    ) -> set[tuple[int, int]]:
        """Indexiert eine Serie einmal statt die gesamte Bibliothek pro Episode
        erneut linear zu durchsuchen."""
        candidates = items if items is not None else self.list_episodes()
        if candidates is None:
            return set()
        normalized_titles = {
            _normalize(title) for title in (series_title, *aliases) if _normalize(title)
        }
        wanted_ids = (
            {str(series_id) for series_id in series_ids if series_id}
            if series_ids is not None else None
        )
        if not normalized_titles and wanted_ids is None:
            return set()
        return {
            (int(it["season"]), int(it["episode"]))
            for it in candidates
            if (
                (str(it.get("series_id") or "") in wanted_ids)
                if wanted_ids is not None
                else _normalize(it.get("series", "")) in normalized_titles
            )
        }

    def watched_episodes_for_series(
        self, series_title: str, items: Optional[List[dict]], aliases=(), series_ids=None,
    ) -> set[tuple[int, int]]:
        """Indexiert alle beim ausgewählten Benutzer gesehenen Episoden."""
        if items is None:
            return set()
        normalized_titles = {
            _normalize(title) for title in (series_title, *aliases) if _normalize(title)
        }
        wanted_ids = (
            {str(series_id) for series_id in series_ids if series_id}
            if series_ids is not None else None
        )
        if not normalized_titles and wanted_ids is None:
            return set()
        return {
            (int(item["season"]), int(item["episode"]))
            for item in items
            if item.get("played") and (
                (str(item.get("series_id") or "") in wanted_ids)
                if wanted_ids is not None
                else _normalize(item.get("series", "")) in normalized_titles
            )
        }

    def series_ids_for(
        self, series_title: str, tmdb_id="", aliases=(), items: Optional[List[dict]] = None,
    ) -> Optional[set[str]]:
        candidates = items if items is not None else self.list_series()
        if candidates is None:
            return set()
        wanted_tmdb = str(tmdb_id or "").strip()
        if wanted_tmdb:
            matched = {
                str(item.get("id")) for item in candidates
                if item.get("id") and str(item.get("tmdb_id") or "") == wanted_tmdb
            }
            if matched:
                return matched
            normalized_titles = {
                _normalize(title) for title in (series_title, *aliases) if _normalize(title)
            }
            unknown_identity = any(
                item.get("id")
                and not str(item.get("tmdb_id") or "").strip()
                and normalized_titles.intersection({
                    _normalize(item.get("name", "")),
                    _normalize(item.get("original_title", "")),
                    _normalize(item.get("sort_name", "")),
                })
                for item in candidates
            )
            if unknown_identity:
                return None
            # Eine explizite stabile ID darf niemals durch einen bloßen
            # Titelgleichstand ersetzt werden. Das würde gleichnamige Remakes
            # oder Jellyfin-Einträge ohne Provider-ID miteinander vermischen.
            return set()
        normalized_titles = {
            _normalize(title) for title in (series_title, *aliases) if _normalize(title)
        }
        title_matches = [
            item for item in candidates
            if item.get("id") and normalized_titles.intersection({
                _normalize(item.get("name", "")),
                _normalize(item.get("original_title", "")),
                _normalize(item.get("sort_name", "")),
            })
        ]
        matched = {str(item.get("id")) for item in title_matches}
        # Mehrere Jellyfin-Serien mit demselben Titel dürfen niemals zu einer
        # gemeinsamen Episodenmenge verschmolzen werden.
        if len(matched) > 1:
            return None
        return matched
