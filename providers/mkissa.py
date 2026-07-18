"""MKissa-/AllAnime-Anbieter für englischsprachige Anime.

MKissa schützt Episodenquellen mit einem kurzlebigen Client-Crypto-Bootstrap:
Die GraphQL-Anfrage erhält einen AES-GCM-signierten ``aaReq``-Wert und die
Antwort liefert ihre eigentlichen Daten wiederum AES-GCM-verschlüsselt.
"""

from __future__ import annotations

import base64
import hashlib
import json
import math
import re
import threading
import time
from dataclasses import asdict, dataclass, field
from datetime import date
from typing import Callable, Dict, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from providers.models import FilmpalastMovie, HosterInfo, parse_episode_slug


BASE_URL = "https://mkissa.to/anime"
API_URL = "https://api.mkissa.net/api"
API_ORIGIN = "https://api.mkissa.net"
CLOCK_URL = "https://allanime.day"
SOURCE_PREFIX = "mkissa:"
DEFAULT_BUILD_ID = "44"
DEFAULT_KEY_MASK = (
    "cd7f14dbf40734836eb46eb14758e49ef9d81e61686d84d467b2e32063ef4af9"
)

SEARCH_HASH = "a24c500a1b765c68ae1d8dd85174931f661c71369c89b92b88b75a725afc471c"
POPULAR_DAILY_HASH = "a0aca6827cc9a3ad7bc711da4d200a04adea8f1a7545dc418d5e92e74c3aad15"
DETAIL_HASH = "043448386c7a686bc2aabfbb6b80f6074e795d350df48015023b079527b0848a"
SOURCE_HASH = "d405d0edd690624b66baba3068e0edc3ac90f1597d898a1ec8db4e5c43c00fec"

SHOW_FIELDS = """
  _id type englishName name nativeName nameOnlyString altNames slugTime
  description availableEpisodes episodeCount lastEpisodeInfo episodeDuration
  airedStart score thumbnail banner genres isAdult
"""

SEARCH_QUERY = f"""
query(
  $search: SearchInput
  $limit: Int
  $page: Int
  $translationType: VaildTranslationTypeEnumType
  $allowAdult: Boolean
) {{
  shows(
    search: $search
    limit: $limit
    page: $page
    translationType: $translationType
    allowAdult: $allowAdult
  ) {{
    pageInfo {{ total }}
    edges {{ {SHOW_FIELDS} }}
  }}
}}
"""

POPULAR_DAILY_QUERY = f"""
query(
  $type: VaildPopularTypeEnumType!
  $size: Int!
  $dateRange: Int
  $page: Int
  $allowAdult: Boolean
  $allowUnknown: Boolean
) {{
  queryPopular(
    type: $type
    size: $size
    dateRange: $dateRange
    page: $page
    allowAdult: $allowAdult
    allowUnknown: $allowUnknown
  ) {{
    total
    recommendations {{ anyCard {{ {SHOW_FIELDS} }} }}
  }}
}}
"""

DETAIL_QUERY = f"""
query($_id: String!) {{
  show(_id: $_id) {{
    {SHOW_FIELDS}
    status averageScore rating airedEnd studios countryOfOrigin
    availableEpisodesDetail tags
  }}
}}
"""

SOURCE_QUERY = f"""
query(
  $showId: String!
  $translationType: VaildTranslationTypeEnumType!
  $episodeString: String!
) {{
  episode(
    showId: $showId
    translationType: $translationType
    episodeString: $episodeString
  ) {{
    episodeString uploadDate sourceUrls thumbnail notes
    show {{ {SHOW_FIELDS} }}
  }}
}}
"""

CRYPTO_ERROR_CODES = {
    "AA_CRYPTO_MISSING",
    "AA_CRYPTO_EXPIRED",
    "AA_CRYPTO_STALE",
    "AA_CRYPTO_BUILD_MISMATCH",
    "AA_CRYPTO_QUERY_MISMATCH",
}


@dataclass
class MkissaAnime:
    id: str
    title: str
    media_type: str = "TV"
    year: str = ""
    cover_url: str = ""
    banner_url: str = ""
    description: str = ""
    genres: List[str] = field(default_factory=list)
    rating: Optional[float] = None
    translations: Dict[str, int] = field(default_factory=dict)
    provider: str = "mkissa"
    content_language: str = "en"

    def public_dict(self) -> dict:
        payload = asdict(self)
        payload["episode_count"] = max(self.translations.values(), default=0)
        return payload


class MkissaScraper:
    """GraphQL-Client mit dynamischer MKissa-Quellentschlüsselung."""

    def __init__(
        self,
        progress_cb: Optional[Callable[[str], None]] = None,
        session: Optional[requests.Session] = None,
    ):
        self.progress_cb = progress_cb or (lambda _message: None)
        self.session = session or requests.Session()
        self.build_id = DEFAULT_BUILD_ID
        self.key_mask = bytes.fromhex(DEFAULT_KEY_MASK)
        self._crypto_key: Optional[bytes] = None
        self._crypto_epoch: Optional[int] = None
        self._crypto_switch_at = 0
        self._crypto_lock = threading.RLock()
        self._detail_cache: Dict[str, tuple[float, MkissaAnime]] = {}
        self._detail_lock = threading.RLock()
        self._catalog_totals: Dict[tuple[str, str], int] = {}
        self._browse_cache: Dict[tuple[str, str], dict] = {}

    @property
    def headers(self) -> dict:
        return {
            "Accept": "application/json",
            "Origin": "https://mkissa.to",
            "Referer": "https://mkissa.to/",
            "User-Agent": "Mozilla/5.0",
            "x-build-id": self.build_id,
        }

    def _log(self, message: str) -> None:
        self.progress_cb(message)

    def browse(
        self,
        mode: str = "latest",
        query: str = "",
        page: int = 1,
        limit: int = 24,
    ) -> dict:
        page = max(1, int(page))
        limit = max(1, min(50, int(limit)))
        if mode == "trending":
            variables = {
                "type": "anime",
                "size": limit,
                "dateRange": 1,
                "page": page,
                "allowAdult": False,
                "allowUnknown": False,
            }
            response = self._graphql(
                variables,
                POPULAR_DAILY_HASH,
                POPULAR_DAILY_QUERY,
            )
            popular = (
                response.get("data", {})
                .get("queryPopular", {})
            )
            shows = [
                item.get("anyCard")
                for item in popular.get("recommendations") or []
                if isinstance(item, dict)
            ]
            entries = self._parse_entries(shows)
            total = int(popular.get("total") or 0)
            total_key = (mode, "")
            if total:
                self._catalog_totals[total_key] = total
            else:
                total = self._catalog_totals.get(total_key, 0)
            return {
                "results": [entry.public_dict() for entry in entries],
                "page": page,
                "has_more": (
                    page * limit < total
                    if total
                    else len(entries) >= limit
                ),
                "total": total,
            }

        search = {"query": query.strip()} if mode == "search" else {
            "sortBy": "Popular" if mode == "popular" else "Recent",
        }
        cache_key = (mode, query.strip().casefold())
        cached = self._browse_cache.get(cache_key)
        if (
            cached is None
            or (
                page == 1
                and time.time() - float(cached.get("created_at") or 0) > 300
            )
        ):
            cached = {
                "created_at": time.time(),
                "entries": [],
                "by_id": {},
                "next_page": 1,
                "total": 0,
                "exhausted": False,
            }
            self._browse_cache[cache_key] = cached

        target_count = page * limit
        while (
            len(cached["entries"]) < target_count
            and not cached["exhausted"]
        ):
            upstream_page = int(cached["next_page"])
            received_counts: List[int] = []
            new_entries = 0
            for translation in ("dub", "sub"):
                variables = {
                    "search": search,
                    "limit": limit,
                    "page": upstream_page,
                    "translationType": translation,
                    "allowAdult": False,
                }
                response = self._graphql(variables, SEARCH_HASH, SEARCH_QUERY)
                shows = response.get("data", {}).get("shows", {})
                raw_entries = shows.get("edges") or []
                received_counts.append(len(raw_entries))
                reported_total = int(
                    (shows.get("pageInfo") or {}).get("total") or 0
                )
                if reported_total:
                    cached["total"] = max(
                        int(cached["total"]),
                        reported_total,
                    )
                for entry in self._parse_entries(raw_entries):
                    existing = cached["by_id"].get(entry.id)
                    if existing is None:
                        cached["by_id"][entry.id] = entry
                        cached["entries"].append(entry)
                        new_entries += 1
                        continue
                    for track, count in entry.translations.items():
                        existing.translations[track] = max(
                            existing.translations.get(track, 0),
                            count,
                        )
            cached["next_page"] = upstream_page + 1
            if (
                not received_counts
                or max(received_counts, default=0) < 20
                or new_entries == 0
            ):
                cached["exhausted"] = True

        start = (page - 1) * limit
        end = start + limit
        results = cached["entries"][start:end]
        has_more = len(cached["entries"]) > end or not cached["exhausted"]
        return {
            "results": [entry.public_dict() for entry in results],
            "page": page,
            "has_more": has_more,
            "total": int(cached["total"]),
        }

    def get_anime(self, anime_id: str, force: bool = False) -> MkissaAnime:
        anime_id = self._normalize_id(anime_id)
        with self._detail_lock:
            cached = self._detail_cache.get(anime_id)
            if cached and not force and time.time() - cached[0] < 300:
                return cached[1]
        response = self._graphql({"_id": anime_id}, DETAIL_HASH, DETAIL_QUERY)
        show = response.get("data", {}).get("show")
        if not isinstance(show, dict) or self._is_adult(show):
            raise LookupError("MKissa-Anime nicht gefunden.")
        entry = self._parse_entry(show)
        if entry is None:
            raise LookupError("MKissa-Anime enthält keine verwertbaren Metadaten.")
        with self._detail_lock:
            self._detail_cache[anime_id] = (time.time(), entry)
        return entry

    def get_episode(self, slug: str) -> FilmpalastMovie:
        parsed = parse_episode_slug(slug)
        if not parsed or not parsed[0].startswith(SOURCE_PREFIX):
            raise ValueError(f"Ungültiger MKissa-Episoden-Slug: {slug}")
        base, season, episode = parsed
        descriptor = base[len(SOURCE_PREFIX):]
        anime_id, separator, translation = descriptor.partition("|")
        if not separator or translation not in {"sub", "dub", "raw"}:
            raise ValueError(f"Ungültige MKissa-Sprachspur: {slug}")
        anime = self.get_anime(anime_id)
        sources = self._episode_sources(anime_id, str(episode), translation)
        language = {
            "dub": "English Dub",
            "sub": "English Sub",
            "raw": "Japanese Raw",
        }[translation]
        hosters = [
            HosterInfo(
                name=str(source.get("sourceName") or self._host_label(url)),
                url=url,
                language=language,
                quality="",
            )
            for source in sources
            if (url := self._resolve_source_url(
                str(
                    source.get("sourceUrl")
                    or source.get("url")
                    or source.get("source")
                    or ""
                )
            ))
        ]
        return FilmpalastMovie(
            title=f"{anime.title} S{season:02d}E{episode:02d}",
            url=slug,
            year=anime.year,
            cover_url=anime.cover_url,
            description=anime.description,
            genres=anime.genres,
            hosters=hosters,
            provider="mkissa",
            content_language="en",
        )

    def _episode_sources(
        self,
        anime_id: str,
        episode: str,
        translation: str,
    ) -> List[dict]:
        response = self._graphql(
            {
                "showId": anime_id,
                "translationType": translation,
                "episodeString": episode,
            },
            SOURCE_HASH,
            SOURCE_QUERY,
            signed=True,
        )
        data = response.get("data") or {}
        episode_data = data.get("episode") if isinstance(data, dict) else None
        sources = (
            episode_data.get("sourceUrls")
            if isinstance(episode_data, dict)
            else data.get("sourceUrls") if isinstance(data, dict) else None
        )
        return sorted(
            (source for source in (sources or []) if isinstance(source, dict)),
            key=lambda source: float(source.get("priority") or 0),
            reverse=True,
        )

    def _graphql(
        self,
        variables: dict,
        query_hash: str,
        fallback_query: str,
        signed: bool = False,
        retry_crypto: bool = True,
        retry_rate_limit: bool = True,
    ) -> dict:
        extensions = {
            "persistedQuery": {
                "version": 1,
                "sha256Hash": query_hash,
            },
        }
        if signed:
            extensions["aaReq"] = self._crypto_token(query_hash)
        response = self.session.get(
            API_URL,
            headers=self.headers,
            params={
                "variables": json.dumps(variables, separators=(",", ":")),
                "extensions": json.dumps(extensions, separators=(",", ":")),
            },
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        errors = payload.get("errors") or []
        rate_limit_message = next((
            str(error.get("message") or "")
            for error in errors
            if isinstance(error, dict)
            and "too many requests" in str(error.get("message") or "").casefold()
        ), "")
        if retry_rate_limit and rate_limit_message:
            seconds_match = re.search(r"(\d+)\s*seconds?", rate_limit_message)
            cooldown = min(
                12,
                max(1, int(seconds_match.group(1)) if seconds_match else 3),
            )
            self._log(
                f"MKissa-API-Limit erreicht – Wiederholung in {cooldown} Sekunden."
            )
            time.sleep(cooldown)
            return self._graphql(
                variables,
                query_hash,
                fallback_query,
                signed=signed,
                retry_crypto=retry_crypto,
                retry_rate_limit=False,
            )
        error_codes = {
            str((error.get("extensions") or {}).get("code") or error.get("message") or "")
            for error in errors
            if isinstance(error, dict)
        }
        if signed and retry_crypto and error_codes & CRYPTO_ERROR_CODES:
            self._reset_crypto()
            if "AA_CRYPTO_BUILD_MISMATCH" in error_codes:
                self._discover_client_crypto_config()
            return self._graphql(
                variables,
                query_hash,
                fallback_query,
                signed=True,
                retry_crypto=False,
                retry_rate_limit=retry_rate_limit,
            )
        if any(
            "PersistedQueryNotFound" in str(error.get("message") or "")
            for error in errors
            if isinstance(error, dict)
        ):
            body = {
                "query": fallback_query,
                "variables": variables,
                "extensions": extensions,
            }
            response = self.session.post(
                API_URL,
                headers={**self.headers, "Content-Type": "application/json"},
                json=body,
                timeout=30,
            )
            response.raise_for_status()
            payload = response.json()
        if signed and isinstance(payload.get("data"), dict):
            encrypted = payload["data"].get("tobeparsed")
            if encrypted:
                payload["data"] = self._decrypt_payload(str(encrypted))
        if payload.get("errors") and not payload.get("data"):
            message = str(payload["errors"][0].get("message") or "GraphQL-Fehler")
            raise RuntimeError(f"MKissa: {message}")
        return payload

    def _crypto_token(self, query_hash: str) -> str:
        key, epoch = self._crypto_material()
        timestamp = int(time.time() * 1000) // 300_000 * 300_000
        iv = hashlib.sha256(
            f"{epoch}:{self.build_id}:{query_hash}:{timestamp}".encode()
        ).digest()[:12]
        body = json.dumps(
            {
                "v": 1,
                "ts": timestamp,
                "epoch": epoch,
                "buildId": self.build_id,
                "qh": query_hash,
            },
            separators=(",", ":"),
        ).encode()
        encrypted = AESGCM(key).encrypt(iv, body, None)
        return base64.b64encode(bytes((1,)) + iv + encrypted).decode()

    def _crypto_material(self) -> tuple[bytes, int]:
        with self._crypto_lock:
            now_ms = int(time.time() * 1000)
            if (
                self._crypto_key is not None
                and self._crypto_epoch is not None
                and (not self._crypto_switch_at or now_ms < self._crypto_switch_at)
            ):
                return self._crypto_key, self._crypto_epoch
            response = self.session.get(
                f"{API_ORIGIN}/client-crypto/v1/bootstrap",
                params={"buildId": self.build_id},
                headers=self.headers,
                timeout=20,
            )
            if response.status_code in {400, 404, 409}:
                self._discover_client_crypto_config()
                response = self.session.get(
                    f"{API_ORIGIN}/client-crypto/v1/bootstrap",
                    params={"buildId": self.build_id},
                    headers=self.headers,
                    timeout=20,
                )
            response.raise_for_status()
            bootstrap = response.json()
            part_b = base64.b64decode(str(bootstrap["partB"]))
            if len(part_b) < 32 or len(self.key_mask) < 32:
                raise RuntimeError("MKissa lieferte unvollständiges Crypto-Material.")
            self._crypto_key = bytes(
                part_b[index] ^ self.key_mask[index % len(self.key_mask)]
                for index in range(32)
            )
            self._crypto_epoch = int(bootstrap["epoch"])
            self._crypto_switch_at = int(bootstrap.get("switchAt") or 0)
            return self._crypto_key, self._crypto_epoch

    def _decrypt_payload(self, value: str) -> dict:
        raw = base64.b64decode(value)
        if len(raw) < 30 or raw[0] != 1:
            raise RuntimeError("Unbekannte MKissa-Verschlüsselung.")
        key, _epoch = self._crypto_material()
        decoded = AESGCM(key).decrypt(raw[1:13], raw[13:], None)
        return json.loads(decoded.decode("utf-8"))

    def _reset_crypto(self) -> None:
        with self._crypto_lock:
            self._crypto_key = None
            self._crypto_epoch = None
            self._crypto_switch_at = 0

    def _discover_client_crypto_config(self) -> None:
        """Ermittelt Build-ID und Mask aus dem aktuell ausgelieferten Web-Bundle."""
        html = self.session.get(
            BASE_URL,
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=20,
        ).text
        app_match = re.search(
            r'import\("(?P<url>https://[^"]+/entry/app\.[^"]+\.js)"\)',
            html,
        )
        if not app_match:
            raise RuntimeError("MKissa-App-Bundle nicht auffindbar.")
        app_url = app_match.group("url")
        app_bundle = self.session.get(app_url, timeout=20).text
        imports = re.findall(r'["\']([^"\']+\.js)["\']', app_bundle)
        for reference in imports[:40]:
            bundle_url = urljoin(app_url, reference)
            bundle = self.session.get(bundle_url, timeout=20).text
            if "client-crypto/v1/bootstrap?buildId=" not in bundle:
                continue
            match = re.search(
                r'\?"([0-9a-f]{64})":"",\w+=.*?\?"(\d{1,4})":""',
                bundle,
            )
            if match:
                self.key_mask = bytes.fromhex(match.group(1))
                self.build_id = match.group(2)
                self._reset_crypto()
                return
        raise RuntimeError("MKissa-Crypto-Konfiguration nicht ermittelbar.")

    def _parse_entries(self, raw_entries) -> List[MkissaAnime]:
        entries: List[MkissaAnime] = []
        seen: set[str] = set()
        for raw in raw_entries or []:
            entry = self._parse_entry(raw) if isinstance(raw, dict) else None
            if entry and entry.id not in seen:
                entries.append(entry)
                seen.add(entry.id)
        return entries

    def _parse_entry(self, show: dict) -> Optional[MkissaAnime]:
        if self._is_adult(show):
            return None
        anime_id = str(show.get("_id") or "").strip()
        title = next((
            str(show.get(key) or "").strip()
            for key in ("englishName", "name", "nativeName", "nameOnlyString")
            if str(show.get(key) or "").strip()
        ), "")
        if not anime_id or not title:
            return None
        available = show.get("availableEpisodes") or {}
        translations = {
            track: max(0, int(available.get(track) or 0))
            for track in ("dub", "sub", "raw")
            if int(available.get(track) or 0) > 0
        }
        if not translations:
            fallback_count = int(show.get("episodeCount") or 0)
            if fallback_count:
                translations["sub"] = fallback_count
        aired = show.get("airedStart") or {}
        year = str(aired.get("year") or "")
        description = BeautifulSoup(
            str(show.get("description") or ""),
            "html.parser",
        ).get_text(" ", strip=True)
        return MkissaAnime(
            id=anime_id,
            title=title,
            media_type=str(show.get("type") or "TV"),
            year=year,
            cover_url=self._image_url(show.get("thumbnail")),
            banner_url=self._image_url(show.get("banner")),
            description=description,
            genres=[
                str(genre).strip()
                for genre in show.get("genres") or []
                if str(genre).strip()
            ],
            rating=self._float_or_none(show.get("score") or show.get("averageScore")),
            translations=translations,
        )

    @staticmethod
    def _normalize_id(value: str) -> str:
        anime_id = str(value or "").strip()
        if anime_id.startswith(SOURCE_PREFIX):
            anime_id = anime_id[len(SOURCE_PREFIX):].split("|", 1)[0]
        if not re.fullmatch(r"[\w-]{6,80}", anime_id):
            raise ValueError("Ungültige MKissa-ID.")
        return anime_id

    @staticmethod
    def _is_adult(show: dict) -> bool:
        value = show.get("isAdult")
        return value is True or str(value).strip().casefold() in {"true", "1"}

    @staticmethod
    def _float_or_none(value) -> Optional[float]:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _image_url(value) -> str:
        image = str(value or "").strip()
        if not image:
            return ""
        if image.startswith("//"):
            return f"https:{image}"
        if image.startswith("http"):
            return image
        image = image.lstrip("/")
        if image.startswith("images"):
            return f"https://aln.youtube-anime.com/{image}?w=500"
        return f"https://aln.youtube-anime.com/images/{image}?w=500"

    def _resolve_source_url(self, value: str) -> str:
        source = str(value or "").strip()
        if source.startswith("--"):
            try:
                source = bytes(
                    int(pair, 16) ^ 56
                    for pair in re.findall(r"..", source[2:])
                ).decode("utf-8").strip()
            except (ValueError, UnicodeDecodeError):
                return ""
        if source.startswith("//"):
            return f"https:{source}"
        if source.startswith("/apivtwo/"):
            path = source.replace("/apivtwo/clock?", "/apivtwo/clock.json?")
            response = self.session.get(
                f"{CLOCK_URL}{path}",
                headers={
                    "Accept": "application/json",
                    "Origin": CLOCK_URL,
                    "Referer": f"{CLOCK_URL}/player.html",
                    "User-Agent": "Mozilla/5.0",
                },
                timeout=20,
            )
            if not response.ok:
                return ""
            for link in response.json().get("links") or []:
                if not isinstance(link, dict):
                    continue
                resolved = next((
                    str(link.get(key) or "").strip()
                    for key in ("link", "url", "sourceUrl", "file")
                    if str(link.get(key) or "").strip()
                ), "")
                if resolved:
                    return resolved
            return ""
        return source if source.startswith(("http://", "https://")) else ""

    @staticmethod
    def _host_label(url: str) -> str:
        match = re.match(r"https?://(?:www\.)?([^/]+)", url)
        return match.group(1).split(".", 1)[0].title() if match else "MKissa"


def anime_episode_slug(anime_id: str, translation: str, episode: int) -> str:
    translation = str(translation or "").strip().casefold()
    if translation not in {"sub", "dub", "raw"}:
        raise ValueError("Unbekannte Anime-Sprachspur.")
    if not re.fullmatch(r"[\w-]{6,80}", str(anime_id or "").strip()):
        raise ValueError("Ungültige MKissa-ID.")
    return (
        f"{SOURCE_PREFIX}{anime_id}|{translation}"
        f"-s01e{max(1, int(episode)):03d}"
    )


def anime_episode_page(
    anime: MkissaAnime,
    translation: str,
    page: int = 1,
    page_size: int = 100,
) -> dict:
    translation = str(translation or "").strip().casefold()
    count = int(anime.translations.get(translation) or 0)
    if count <= 0:
        raise ValueError("Diese Sprachspur ist nicht verfügbar.")
    page_size = max(1, min(100, int(page_size)))
    page_count = max(1, math.ceil(count / page_size))
    page = max(1, min(page_count, int(page)))
    start = (page - 1) * page_size + 1
    end = min(count, start + page_size - 1)
    return {
        "page": page,
        "page_count": page_count,
        "page_size": page_size,
        "total": count,
        "episodes": [
            {
                "number": episode,
                "label": f"Episode {episode}",
                "slug": anime_episode_slug(anime.id, translation, episode),
            }
            for episode in range(start, end + 1)
        ],
    }


def current_anime_season() -> str:
    month = date.today().month
    if month <= 3:
        return "Winter"
    if month <= 6:
        return "Spring"
    if month <= 9:
        return "Summer"
    return "Fall"
