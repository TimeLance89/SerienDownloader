"""
Scraper fuer moflix-stream.xyz.

Die Seite ist eine SPA und liefert Such-, Detail- und Watch-Daten in
window.bootstrapData. Wir lesen diese Daten ohne UI-Browser aus.
"""

import json
import logging
import re
from typing import Callable, Dict, List, Optional
from urllib.parse import quote

from bs4 import BeautifulSoup
from curl_cffi import requests as cr

from providers.models import (
    FilmpalastMovie,
    FilmpalastSearchResult,
    FilmpalastSeries,
    FilmpalastSeriesResult,
    HosterInfo,
    SeriesEpisode,
    parse_episode_slug,
)

logger = logging.getLogger(__name__)

BASE_URL = "https://moflix-stream.xyz"
API_URL = f"{BASE_URL}/api/v1"
SOURCE_PREFIX = "moflix:"
GENRES = [
    "Action", "Abenteuer", "Komödie", "Drama", "Thriller", "Horror",
    "Sci-Fi & Fantasy", "Sci-Fi", "Science Fiction", "Fantasy", "Krimi",
    "Mystery", "Animation", "Anime", "Dokumentarfilm", "Familie",
]
GENRE_QUERY = {
    "Sci-Fi & Fantasy": "Science Fiction,Fantasy",
}


class MoflixScraper:
    def __init__(self, progress_cb: Optional[Callable[[str], None]] = None):
        self._log = progress_cb or logger.info
        self.session = cr.Session(impersonate="chrome136")

    def search(self, query: str) -> List[FilmpalastSearchResult]:
        query = (query or "").strip()
        if not query:
            return []
        self._log(f"Moflix Suche: {query}")
        data = self._bootstrap(f"{BASE_URL}/search/{quote(query)}")
        titles = self._collect_titles(data)
        results = [self._result_from_title(t) for t in titles]
        self._log(f"  Moflix: {len(results)} Treffer")
        return results

    def list_movies(self, category: str = "new", page: int = 1) -> List[FilmpalastSearchResult]:
        # Moflix nutzt Infinite-Scroll; die erste Seite reicht als Startliste.
        if page != 1:
            return []
        url = f"{BASE_URL}/movies" if category != "top" else f"{BASE_URL}/movies"
        self._log(f"Moflix Liste: {url}")
        data = self._bootstrap(url)
        titles = [t for t in self._collect_titles(data) if not t.get("is_series")]
        return [self._result_from_title(t) for t in titles[:32]]

    def list_genres(self) -> List[str]:
        return list(GENRES)

    def list_by_genre(self, genre: str, page: int = 1) -> List[FilmpalastSearchResult]:
        if page != 1:
            return []
        genre = (genre or "").strip()
        if not genre:
            return []
        self._log(f"Moflix Genre: {genre}")
        resp = self.session.get(
            f"{API_URL}/moflix/recommendations",
            params={
                "route": "genre",
                "genres": GENRE_QUERY.get(genre, genre),
                "mediaType": "movie",
                "limit": 50,
            },
            timeout=25,
            headers={
                "Accept": "application/json, text/plain, */*",
                "Referer": f"{BASE_URL}/",
            },
        )
        resp.raise_for_status()
        data = resp.json()
        titles = [t for t in data.get("titles", []) if not t.get("is_series")]
        return [self._result_from_title(t) for t in titles]

    def get_movie(self, url_or_slug: str) -> Optional[FilmpalastMovie]:
        if url_or_slug.startswith(SOURCE_PREFIX):
            parsed_ep = parse_episode_slug(url_or_slug)
            if parsed_ep:
                base, season, episode = parsed_ep
                title_id = self._title_id(base)
                if not title_id:
                    return None
                return self._get_episode_movie(title_id, self._title_slug(base), season, episode)
        title_id = self._title_id(url_or_slug)
        if not title_id:
            return None
        slug = self._title_slug(url_or_slug)
        url = f"{BASE_URL}/titles/{title_id}/{slug or 'x'}"
        data = self._bootstrap(url)
        page = data.get("loaders", {}).get("titlePage", {})
        title = page.get("title") or self._find_title(data, title_id)
        if not title:
            return None

        hosters: List[HosterInfo] = []
        videos = self._collect_videos(title)
        if not videos:
            videos = self._collect_videos(page)
        if not videos:
            primary = title.get("primary_video")
            if primary:
                videos.append(primary)

        for video in videos:
            video_id = video.get("id")
            src = video.get("src")
            if not src and video_id:
                src = self._watch_src(video_id)
            if not src:
                continue
            name = self._hoster_name(src, video.get("name") or "Moflix")
            quality = str(video.get("quality") or "")
            hosters.append(HosterInfo(name=name, url=src, quality=quality))

        year = self._year(title.get("release_date") or title.get("year"))
        runtime = f"{title.get('runtime')} min" if title.get("runtime") else ""
        genres = []
        for g in title.get("genres") or []:
            if isinstance(g, dict) and g.get("name"):
                genres.append(g["name"])

        return FilmpalastMovie(
            title=title.get("name") or "Unbekannt",
            url=url,
            year=year,
            runtime=runtime,
            cover_url=title.get("poster") or "",
            description=title.get("description") or "",
            genres=genres,
            hosters=hosters,
        )

    def _get_episode_movie(self, title_id: int, slug: str, season: int, episode: int) -> Optional[FilmpalastMovie]:
        """Lädt eine einzelne Episode (Season-/Episode-Route) und liefert sie
        als FilmpalastMovie mit allen Hoster-Videos – analog zu get_movie(),
        aber für Episoden statt Filme."""
        slug_part = slug or "x"
        url = f"{BASE_URL}/titles/{title_id}/{slug_part}/season/{season}/episode/{episode}"
        data = self._bootstrap(url)
        page = data.get("loaders", {}).get("episodePage", {})
        ep = page.get("episode") or {}
        if not ep:
            return None
        title_obj = page.get("title") or {}

        hosters: List[HosterInfo] = []
        for video in ep.get("videos") or []:
            src = video.get("src")
            if not src:
                continue
            name = self._hoster_name(src, video.get("name") or "Moflix")
            hosters.append(HosterInfo(
                name=name, url=src,
                language=video.get("language") or "",
                quality=str(video.get("quality") or ""),
            ))

        series_title = title_obj.get("name") or "Unbekannte Serie"
        return FilmpalastMovie(
            title=f"{series_title} S{season:02d}E{episode:02d}",
            url=url,
            year=str(ep.get("year") or ""),
            cover_url=ep.get("poster") or title_obj.get("poster") or "",
            description=ep.get("description") or "",
            hosters=hosters,
        )

    def get_series(self, url_or_slug: str) -> Optional[FilmpalastSeries]:
        """Lädt eine Serie inkl. aller Staffeln/Episoden. Da Moflix pro
        Staffel eine eigene Route hat, braucht das einen Request für die
        Titelseite + einen weiteren pro Staffel (kein Einzel-Request wie
        bei filmpalast möglich)."""
        title_id = self._title_id(url_or_slug)
        if not title_id:
            return None
        slug = self._title_slug(url_or_slug)
        url = f"{BASE_URL}/titles/{title_id}/{slug or 'x'}"
        self._log(f"Lade Serie (Moflix): {url}")
        data = self._bootstrap(url)
        page = data.get("loaders", {}).get("titlePage", {})
        title_obj = page.get("title") or self._find_title(data, title_id)
        if not title_obj or not title_obj.get("is_series"):
            return None

        series_title = title_obj.get("name") or "Unbekannte Serie"
        series_slug = slug or self._slugify(series_title)
        genres = [g["name"] for g in title_obj.get("genres") or [] if isinstance(g, dict) and g.get("name")]

        seasons_data = page.get("seasons", {}).get("data", [])
        seasons: Dict[int, List[SeriesEpisode]] = {}
        for season_obj in seasons_data:
            number = season_obj.get("number")
            if number is None:
                continue
            season_url = f"{BASE_URL}/titles/{title_id}/{series_slug}/season/{number}"
            season_data = self._bootstrap(season_url)
            season_page = season_data.get("loaders", {}).get("seasonPage", {})
            episodes_data = season_page.get("episodes", {}).get("data", [])
            eps: List[SeriesEpisode] = []
            for ep in episodes_data:
                s_num, e_num = ep.get("season_number"), ep.get("episode_number")
                if s_num is None or e_num is None:
                    continue
                eps.append(SeriesEpisode(
                    season=s_num, episode=e_num,
                    slug=f"{SOURCE_PREFIX}{title_id}:{series_slug}-s{s_num:02d}e{e_num:02d}",
                    url=f"{BASE_URL}/titles/{title_id}/{series_slug}/season/{s_num}/episode/{e_num}",
                    release_name=ep.get("name") or "",
                ))
            if eps:
                eps.sort(key=lambda e: e.episode)
                seasons[number] = eps

        if not seasons:
            self._log("  Keine Staffeln/Episoden gefunden.")
            return None

        total_eps = sum(len(v) for v in seasons.values())
        self._log(f"  Serie (Moflix): «{series_title}» – {len(seasons)} Staffel(n), {total_eps} Episoden")

        return FilmpalastSeries(
            title=series_title, base_slug=f"{SOURCE_PREFIX}{title_id}:{series_slug}", url=url,
            cover_url=title_obj.get("poster") or "", description=title_obj.get("description") or "",
            genres=genres, seasons=seasons,
        )

    def search_series(self, query: str) -> List[FilmpalastSeriesResult]:
        query = (query or "").strip()
        if not query:
            return []
        self._log(f"Moflix Serien-Suche: {query}")
        data = self._bootstrap(f"{BASE_URL}/search/{quote(query)}")
        titles = [t for t in self._collect_titles(data) if t.get("is_series")]
        results = [self._series_result_from_title(t) for t in titles]
        self._log(f"  Moflix: {len(results)} Serie(n) gefunden")
        return results

    def list_series(self, page: int = 1) -> List[FilmpalastSeriesResult]:
        if page != 1:
            return []
        self._log("Moflix Serien-Katalog (neu)")
        data = self._bootstrap(f"{BASE_URL}/series")
        titles = [t for t in self._collect_titles(data) if t.get("is_series")]
        results = [self._series_result_from_title(t) for t in titles]
        self._log(f"  Moflix: {len(results)} Serie(n)")
        return results

    def _series_result_from_title(self, title: dict) -> FilmpalastSeriesResult:
        title_id = title["id"]
        name = title.get("name") or str(title_id)
        slug = self._slugify(name)
        return FilmpalastSeriesResult(
            title=f"{name}  [Moflix]",
            base_slug=f"{SOURCE_PREFIX}{title_id}:{slug}",
            sample_slug=f"{SOURCE_PREFIX}{title_id}:{slug}",
            sample_url=f"{BASE_URL}/titles/{title_id}/{slug}",
            year=self._year(title.get("release_date") or title.get("year")),
            cover_url=title.get("poster") or "",
        )

    def _watch_src(self, video_id: int) -> str:
        data = self._bootstrap(f"{BASE_URL}/watch/{video_id}")
        page = data.get("loaders", {}).get("watchPage", {})
        video = page.get("video") or {}
        return video.get("src") or ""

    def _bootstrap(self, url: str) -> dict:
        resp = self.session.get(
            url,
            timeout=25,
            allow_redirects=True,
            headers={"Accept": "text/html,application/xhtml+xml,*/*"},
        )
        resp.raise_for_status()
        html = resp.text
        m = re.search(r"window\.bootstrapData\s*=\s*(\{.*?\});\s*</script>", html, re.S)
        if not m:
            # Fallback: wenigstens HTML lesbar halten.
            soup = BeautifulSoup(html, "lxml")
            raise RuntimeError(f"Moflix Bootstrap fehlt: {soup.title.get_text(strip=True) if soup.title else url}")
        return json.loads(m.group(1))

    def _collect_titles(self, data: dict) -> List[dict]:
        found = []
        seen = set()

        def walk(x):
            if isinstance(x, dict):
                if x.get("model_type") == "title" and x.get("id") and x.get("name"):
                    if x["id"] not in seen:
                        seen.add(x["id"])
                        found.append(x)
                for v in x.values():
                    walk(v)
            elif isinstance(x, list):
                for v in x:
                    walk(v)

        walk(data.get("loaders", data))
        return found

    def _collect_videos(self, data: dict) -> List[dict]:
        videos = []
        seen = set()

        def walk(x):
            if isinstance(x, dict):
                if x.get("model_type") == "video" and x.get("id") and x["id"] not in seen:
                    seen.add(x["id"])
                    videos.append(x)
                for v in x.values():
                    walk(v)
            elif isinstance(x, list):
                for v in x:
                    walk(v)

        walk(data)
        return videos

    def _find_title(self, data: dict, title_id: int) -> Optional[dict]:
        for t in self._collect_titles(data):
            if t.get("id") == title_id:
                return t
        return None

    def _result_from_title(self, title: dict) -> FilmpalastSearchResult:
        title_id = title["id"]
        name = title.get("name") or str(title_id)
        return FilmpalastSearchResult(
            title=f"{name}  [Moflix]",
            slug=f"{SOURCE_PREFIX}{title_id}:{self._slugify(name)}",
            url=f"{BASE_URL}/titles/{title_id}/{self._slugify(name)}",
            year=self._year(title.get("release_date") or title.get("year")),
            is_movie=not bool(title.get("is_series")),
        )

    def _title_id(self, value: str) -> Optional[int]:
        value = str(value or "")
        if value.startswith(SOURCE_PREFIX):
            value = value[len(SOURCE_PREFIX):]
        m = re.search(r"(?:titles/)?(\d+)", value)
        return int(m.group(1)) if m else None

    def _title_slug(self, value: str) -> str:
        value = str(value or "")
        if value.startswith(SOURCE_PREFIX) and ":" in value[len(SOURCE_PREFIX):]:
            parts = value[len(SOURCE_PREFIX):].split(":", 1)
            return parts[1]
        m = re.search(r"/titles/\d+/([^/?#]+)", value)
        return m.group(1) if m else ""

    @staticmethod
    def _year(value) -> str:
        m = re.search(r"\b(19|20)\d{2}\b", str(value or ""))
        return m.group(0) if m else ""

    @staticmethod
    def _hoster_name(url: str, fallback: str) -> str:
        low = (url or "").lower()
        if "rpmplay" in low or "moflix-stream" in low or "moflix.upns" in low:
            return "Moflix"
        if "veev" in low:
            return "Veev"
        return fallback or "Moflix"

    @staticmethod
    def _slugify(text: str) -> str:
        text = (text or "").lower()
        text = text.replace("&", " und ")
        text = re.sub(r"[^a-z0-9äöüß]+", "-", text, flags=re.I)
        text = text.strip("-")
        return text or "titel"
