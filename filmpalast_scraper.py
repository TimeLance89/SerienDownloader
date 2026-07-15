"""
Scraper für filmpalast.to – Filme (keine Staffeln).

Die Hoster-Links sind DIREKT im HTML sichtbar
(kein Button-Klick, kein Redirect, kein JavaScript nötig).

URL-Schema:
  Suche:  https://filmpalast.to/search/title/<query>
  Film:   https://filmpalast.to/stream/<slug>

Wir sammeln ALLE Hoster (VOE, Streamtape, Doodstream, Vidoza, Vidmoly, ...)
und sortieren nach Sprache (Deutsch zuerst) + Qualität.
"""

import logging
import re
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple
from urllib.parse import quote

from bs4 import BeautifulSoup
from session_manager import SessionManager

logger = logging.getLogger(__name__)

BASE_URL = "https://filmpalast.to"

# Episode-Slugs sehen aus wie "the-bear-s05e01" -> Serie "the-bear", Staffel 5, Episode 1
EPISODE_SLUG_RE = re.compile(r"^(?P<base>.+)-s(?P<season>\d{1,2})e(?P<episode>\d{1,3})$", re.IGNORECASE)


def parse_episode_slug(slug: str) -> Optional[Tuple[str, int, int]]:
    """Zerlegt 'the-bear-s05e01' in ('the-bear', 5, 1). None wenn kein Episode-Slug."""
    m = EPISODE_SLUG_RE.match(slug or "")
    if not m:
        return None
    return m.group("base"), int(m.group("season")), int(m.group("episode"))


def strip_episode_suffix(title: str) -> str:
    """Entfernt ' S05E01' vom Ende eines Episoden-Titels -> reiner Serientitel."""
    return re.sub(r"\s*S\d{1,2}E\d{1,3}\s*$", "", title or "", flags=re.IGNORECASE).strip()


# Priorisierte Hoster-Liste – erstgenannter = bevorzugt
# Wer hier steht wird bei VOE-Ausfall automatisch versucht
# (in `extractor.HosterAwareExtractor`)
HOSTER_PRIORITY = [
    "voe",
    "vidara",
    "vidoza",
    "streamtape",
    "doodstream",
    "vidmoly",
    "vido",
    "speedfiles",
    "filemoon",
    "moonplayer",
    "upstream",
    "vidsonic",
    "flyfile",
]

# Hoster die in der Vergangenheit oft 404/irreparabel liefern
HOSTER_BLACKLIST = [
    "linkdee",  # meist Werbe-Redirects
]


@dataclass
class FilmpalastSearchResult:
    """Ein Treffer in der Suche (oft auch Serien-Episoden, wird gefiltert)."""
    title: str
    slug: str        # z.B. "undertone"
    url: str
    year: str = ""
    is_movie: bool = True  # Filmpalast mixt Filme + Serien-Episoden in der Suche


@dataclass
class HosterInfo:
    """Ein einzelner Hoster-Eintrag auf einer filmpalast-Filmseite."""
    name: str          # z.B. "VOE", "Streamtape", "Doodstream"
    url: str           # direkter Link zum Hoster
    language: str = "" # "Deutsch", "Englisch", "Original" etc.
    quality: str = ""  # "HD", "SD", "1080p", "720p" – wird aus dem Namen geparst

    @property
    def is_de(self) -> bool:
        return self.language.lower().startswith("deutsch") or self.language.lower() == "de"

    @property
    def is_hd(self) -> bool:
        q = (self.quality or "").upper()
        return "HD" in q or "1080" in q or "720" in q

    def __repr__(self):
        lang = f"[{self.language}]" if self.language else ""
        qual = f"({self.quality})" if self.quality else ""
        return f"HosterInfo({self.name}{lang}{qual})"


@dataclass
class FilmpalastMovie:
    """Ein einzelner Film mit allen verfügbaren Hostern."""
    title: str
    url: str
    year: str = ""
    runtime: str = ""      # z.B. "94 min"
    cover_url: str = ""    # URL zum Cover-Bild (filmpalast)
    description: str = ""  # Plot-Beschreibung (itemprop="description")
    genres: List[str] = field(default_factory=list)
    hosters: List[HosterInfo] = field(default_factory=list)

    # Backward-Compat: voe_url ist der erste VOE-Hoster
    @property
    def voe_url(self) -> Optional[str]:
        for h in self.hosters:
            if h.name.lower() == "voe":
                return h.url
        return None

    def has_hoster(self, name: str) -> bool:
        nl = name.lower()
        return any(h.name.lower() == nl for h in self.hosters)

    def get_hoster(self, name: str) -> Optional[HosterInfo]:
        nl = name.lower()
        for h in self.hosters:
            if h.name.lower() == nl:
                return h
        return None


@dataclass
class SeriesEpisode:
    """Eine einzelne Episode innerhalb einer Serie (noch ohne Hoster geladen)."""
    season: int
    episode: int
    slug: str
    url: str
    release_name: str = ""

    @property
    def label(self) -> str:
        return f"S{self.season:02d}E{self.episode:02d}"


@dataclass
class FilmpalastSeries:
    """Eine komplette Serie mit allen Staffeln/Episoden (Metadaten, keine Hoster)."""
    title: str
    base_slug: str
    url: str
    cover_url: str = ""
    description: str = ""
    genres: List[str] = field(default_factory=list)
    seasons: Dict[int, List[SeriesEpisode]] = field(default_factory=dict)

    @property
    def season_numbers(self) -> List[int]:
        return sorted(self.seasons.keys())

    @property
    def all_episodes(self) -> List[SeriesEpisode]:
        eps: List[SeriesEpisode] = []
        for s in self.season_numbers:
            eps.extend(self.seasons[s])
        return eps

    def episodes_in_seasons(self, seasons: List[int]) -> List[SeriesEpisode]:
        wanted = set(seasons)
        return [e for e in self.all_episodes if e.season in wanted]


@dataclass
class FilmpalastSeriesResult:
    """Ein gruppierter Serien-Treffer aus der Suche (viele Episoden-Treffer -> 1 Serie)."""
    title: str
    base_slug: str
    sample_slug: str  # Slug einer beliebigen Episode dieser Serie (fuer get_series())
    sample_url: str
    year: str = ""
    cover_url: str = ""


class FilmpalastScraper:
    def __init__(self, progress_cb: Optional[Callable[[str], None]] = None):
        self._log = progress_cb or logger.info
        self.session = SessionManager(
            target_domain="filmpalast.to",
            log_cb=self._log,
        )

    def _get_soup(self, url: str) -> BeautifulSoup:
        html = self.session.get(url)
        return BeautifulSoup(html, "lxml")

    @staticmethod
    def _abs_url(href: str) -> str:
        """Macht aus /files/... → https://filmpalast.to/files/..."""
        if not href:
            return ""
        if href.startswith("//"):
            return "https:" + href
        if href.startswith("/"):
            return BASE_URL + href
        if href.startswith("http"):
            return href
        return BASE_URL + "/" + href

    def list_genres(self) -> List[str]:
        soup = self._get_soup(BASE_URL + "/")
        genres = []
        seen = set()
        for a in soup.find_all("a", href=re.compile(r"/search/genre/")):
            name = a.get_text(strip=True)
            key = name.casefold()
            if name and key not in seen:
                seen.add(key)
                genres.append(name)
        return genres

    def list_by_genre(self, genre: str, page: int = 1) -> List[FilmpalastSearchResult]:
        genre = (genre or "").strip()
        if not genre:
            return []
        page = max(1, int(page))
        encoded = quote(genre)
        url = f"{BASE_URL}/search/genre/{encoded}" if page == 1 else f"{BASE_URL}/search/genre/{encoded}/{page}"
        self._log(f"Genre: {genre} Seite {page}")
        soup = self._get_soup(url)
        return self._parse_listing_soup(soup)

    def _parse_listing_soup(self, soup: BeautifulSoup) -> List[FilmpalastSearchResult]:
        results: List[FilmpalastSearchResult] = []
        seen: set = set()

        for article in soup.find_all("article", class_=lambda c: c and "liste" in c):
            h2 = article.find("h2")
            if not h2:
                continue
            a = h2.find("a", href=True)
            if not a:
                continue

            href = a.get("href", "")
            m = re.search(r"/stream/([^/?#\"']+)", href)
            if not m:
                continue
            slug = m.group(1)
            if slug in seen:
                continue
            seen.add(slug)

            title = a.get_text(strip=True) or a.get("title", "").strip() or slug
            full_url = BASE_URL + "/stream/" + slug if href.startswith("/") else href

            year = ""
            for span in article.find_all("span", class_="releaseTitleHome"):
                ym = re.search(r"\b(19|20)\d{2}\b", span.get_text())
                if ym:
                    year = ym.group(0)
                    break

            results.append(FilmpalastSearchResult(
                title=title, slug=slug, url=full_url, year=year,
            ))

        return results

    # ------------------------------------------------------------------
    # Suche
    # ------------------------------------------------------------------
    def search(self, query: str) -> List[FilmpalastSearchResult]:
        """
        Sucht nach <query> und gibt alle Treffer zurück.

        filmpalast liefert Suchergebnisse (und auch /movies/new, /movies/top)
        in einer einheitlichen Card-Struktur:

            <article class="liste glowliste rb">  (Suche, /movies/new, /movies/top)
              <h2 class="rb"><a href="//filmpalast.to/stream/<slug>">Title</a></h2>
              ...kein Jahr auf der Listing-Seite...

        Auf der Hauptseite (filmpalast.to/) sind die Klassen leicht anders
        ("liste pHome" + "h2-start" + Jahr in "releaseTitleHome"), aber
        /stream/-Link steckt immer in einem <h2>. Wir matchen daher auf
        <article class="*liste*"> und picken den ersten h2>a darin.
        """
        query = (query or "").strip()
        if not query:
            return []

        url = f"{BASE_URL}/search/title/{quote(query)}"
        self._log(f"Suche: {query}")
        soup = self._get_soup(url)

        results = self._parse_listing_soup(soup)
        self._log(f"  {len(results)} Treffer")
        return results

    # ------------------------------------------------------------------
    # Listen (Top / Neu) – gleiche Card-Struktur wie die Suche
    # ------------------------------------------------------------------
    def list_movies(self, category: str = "new", page: int = 1) -> List[FilmpalastSearchResult]:
        """
        Listet Filme aus einer der Kategorien:
          "new" → /movies/new
          "top" → /movies/top
        Optional `page` für Pagination (/page/N), default = 1.

        Liefert dasselbe Format wie search() (FilmpalastSearchResult).
        Praktisch wenn man einfach was Aktuelles anschauen will, ohne zu suchen.
        """
        category = (category or "new").lower()
        if category not in ("new", "top"):
            category = "new"
        page = max(1, int(page))
        if page == 1:
            url = f"{BASE_URL}/movies/{category}"
        else:
            url = f"{BASE_URL}/movies/{category}/page/{page}"
        self._log(f"Liste: {url}")

        soup = self._get_soup(url)
        results = self._parse_listing_soup(soup)
        self._log(f"  {len(results)} Filme (Seite {page})")
        return results

    def discover_max_page(self, category: str = "new", quick: bool = True) -> int:
        """
        Findet die höchste verfügbare Seitenzahl.

        filmpalast hat keinen "Seite X von Y"-Anzeiger, nur Page-Links 1-5
        am Ende der ersten Seite. Wir machen deshalb eine binäre Suche:
        probiere Seite N, wenn voll (32 Filme) gibt's noch mehr, sonst ist
        N-1 die letzte. log2(50) = 6 Requests = ~1s.

        quick=True: nur binär-suchen, max ~6 HTTP-Requests.
        quick=False: vollständiger Scan (genau aber langsam).
        """
        category = (category or "new").lower()
        if category not in ("new", "top"):
            category = "new"

        if quick:
            return self._discover_max_page_binary(category)
        else:
            return self._discover_max_page_linear(category)

    def _discover_max_page_linear(self, category: str) -> int:
        """Sequentielle Suche: langsam (~10s für 50 Seiten) aber sicher."""
        page = 1
        last_with_films = 1
        while page <= 200:  # Safety-Cap
            try:
                films = self.list_movies(category, page)
            except Exception:
                break
            if not films:
                break
            last_with_films = page
            if len(films) < 32:
                # Letzte Seite erreicht
                break
            page += 1
        return last_with_films

    def _discover_max_page_binary(self, category: str) -> int:
        """
        Binäre Suche: finde die letzte Seite die >=32 Filme hat.

        Annahmen:
          - filmpalast listet pro Seite 32 Filme, letzte Seite hat 1-31
          - filmpalast gibt für ungültige Seiten eine leere Liste oder HTML ohne Filme
        """
        # 1) Finde obere Schranke (Potenz von 2)
        upper = 1
        while upper <= 512:
            try:
                films = self.list_movies(category, upper)
            except Exception:
                break
            if not films:
                break
            if len(films) < 32:
                # Letzte Seite gefunden (obere Schranke)
                return upper
            upper *= 2
        # 2) Binäre Suche zwischen (upper/2, upper]
        lo = upper // 2
        hi = upper
        # Wenn lo == 0 (also upper fing bei 1 an und 1 war schon leer)
        if lo == 0:
            return 0
        while lo < hi:
            mid = (lo + hi + 1) // 2
            try:
                films = self.list_movies(category, mid)
            except Exception:
                hi = mid - 1
                continue
            if films and len(films) >= 32:
                lo = mid  # mid ist nicht die letzte, weitersuchen rechts
            else:
                hi = mid - 1  # mid ist die letzte oder darüber, links suchen
        return lo

    # ------------------------------------------------------------------
    # Film-Detail
    # ------------------------------------------------------------------
    def get_movie(self, url_or_slug: str) -> Optional[FilmpalastMovie]:
        """
        Lädt die Film-Seite und extrahiert Metadaten + VOE-URL.

        Akzeptiert:
          - volle URL (https://filmpalast.to/stream/...)
          - Slug     (undertone)
        """
        if url_or_slug.startswith("http"):
            url = url_or_slug
        else:
            slug = url_or_slug.lstrip("/")
            if not slug.startswith("stream/"):
                slug = "stream/" + slug
            url = f"{BASE_URL}/{slug}"

        self._log(f"Lade Film: {url}")
        soup = self._get_soup(url)

        # Titel
        title = ""
        h2 = soup.find("h2", class_="bgDark")
        if h2:
            title = h2.get_text(strip=True)
        if not title:
            t = soup.find("title")
            if t:
                # "Film Undertone Stream kostenlos..." → "Undertone"
                m = re.search(r"Film\s+(.+?)\s+Stream", t.get_text())
                if m:
                    title = m.group(1).strip()

        # Jahr / Laufzeit
        year, runtime = "", ""
        # Release-String z.B. "Undertone.2025.GERMAN.DL.1080p.WEB.H264-MGE"
        rel = soup.find("span", id="release_text")
        if rel:
            ym = re.search(r"\b(19|20)\d{2}\b", rel.get_text())
            if ym:
                year = ym.group(0)
        # Spielzeit: steht in <em>94 min</em> innerhalb der "Shortinfos"-Liste
        for em in soup.find_all("em"):
            txt = em.get_text(strip=True)
            if re.match(r"^\d+\s*min$", txt):
                runtime = txt
                break

        cover_url = self._extract_cover(soup)
        description = self._extract_description(soup)
        genres = self._extract_genres(soup)

        # Alle Hoster extrahieren
        hosters = self._extract_all_hosters(soup)
        if not hosters:
            self._log("  Keine Hoster auf der Seite gefunden.")
            return None

        # Sprach-Statistik loggen
        de_count = sum(1 for h in hosters if h.is_de)
        self._log(
            f"  Film: «{title}» ({year}) – {len(hosters)} Hoster, "
            f"{de_count} davon Deutsch"
        )

        return FilmpalastMovie(
            title=title or "Unbekannt",
            url=url,
            year=year,
            runtime=runtime,
            cover_url=cover_url,
            description=description,
            genres=genres,
            hosters=hosters,
        )

    def _extract_cover(self, soup: BeautifulSoup) -> str:
        """Cover-Bild (höchste verfügbare Auflösung: /files/movies/450/)."""
        cover_url = ""
        for img in soup.find_all("img", class_="cover2"):
            src = img.get("src") or img.get("data-src", "")
            if src and "movies" in src:
                cover_url = self._abs_url(src)
                if "movies/450" in src:
                    break
        if not cover_url:
            # Fallback: erstes Bild das wie ein Cover aussieht
            for img in soup.find_all("img", src=True):
                src = img["src"]
                if "/files/movies/" in src:
                    cover_url = self._abs_url(src)
                    break
        return cover_url

    @staticmethod
    def _extract_description(soup: BeautifulSoup) -> str:
        for span in soup.find_all("span", attrs={"itemprop": "description"}):
            txt = span.get_text(strip=True)
            if txt and len(txt) > 30:
                return txt
        # Fallback: das hidden-span mit der Beschreibung
        for span in soup.find_all("span", class_="hidden"):
            txt = span.get_text(strip=True)
            if txt and len(txt) > 80:
                return txt
        return ""

    @staticmethod
    def _extract_genres(soup: BeautifulSoup) -> List[str]:
        """Genres (in den Listen unter "Kategorien, Genre")."""
        genres: List[str] = []
        for li in soup.find_all("li"):
            txt = li.get_text(" ", strip=True)
            if txt.startswith("Kategorien, Genre") or "Kategorien" in txt:
                for a in li.find_all("a", href=re.compile(r"/search/genre/")):
                    g = a.get_text(strip=True)
                    if g and g not in genres:
                        genres.append(g)
                if genres:
                    break
        return genres

    # ------------------------------------------------------------------
    # Serien: komplette Staffel-/Episodenliste + gruppierte Suche
    # ------------------------------------------------------------------
    def get_series(self, url_or_slug: str) -> Optional[FilmpalastSeries]:
        """
        Lädt EINE Episoden-Seite (z.B. "the-bear-s05e01") und extrahiert
        daraus die komplette Staffel-/Episodenliste der Serie.

        Das funktioniert mit nur einem HTTP-Request, weil filmpalast auf
        jeder Episoden-Seite bereits ALLE Staffeln/Episoden im HTML einbettet
        (versteckt in <div class="staffelWrapperLoop ... hide">, sichtbar
        gemacht per JS-Klick – wir brauchen das JS nicht, BeautifulSoup sieht
        auch die "hide"-Blöcke).

        Akzeptiert volle URL oder Slug (z.B. 'the-bear-s05e01').
        """
        if url_or_slug.startswith("http"):
            url = url_or_slug
        else:
            slug = url_or_slug.lstrip("/")
            if not slug.startswith("stream/"):
                slug = "stream/" + slug
            url = f"{BASE_URL}/{slug}"

        self._log(f"Lade Serie: {url}")
        soup = self._get_soup(url)

        raw_title = ""
        h2 = soup.find("h2", class_="bgDark")
        if h2:
            raw_title = h2.get_text(strip=True)
        series_title = strip_episode_suffix(raw_title)
        if not series_title:
            series_title = raw_title or "Unbekannte Serie"

        cover_url = self._extract_cover(soup)
        description = self._extract_description(soup)
        genres = self._extract_genres(soup)

        seasons: Dict[int, List[SeriesEpisode]] = {}
        for ul in soup.find_all("ul", class_="staffelEpisodenList"):
            for a in ul.find_all("a", class_="getStaffelStream", href=True):
                href = a.get("href", "")
                m = re.search(r"/stream/([^/?#\"']+)", href)
                if not m:
                    continue
                ep_slug = m.group(1)
                parsed = parse_episode_slug(ep_slug)
                if not parsed:
                    continue
                _base, season, episode = parsed
                small = a.find("small")
                release_name = small.get_text(strip=True) if small else ""
                seasons.setdefault(season, []).append(SeriesEpisode(
                    season=season, episode=episode, slug=ep_slug,
                    url=self._abs_url(href), release_name=release_name,
                ))

        if not seasons:
            self._log("  Keine Staffel-/Episodenliste gefunden – keine Serie?")
            return None

        for season in seasons:
            seasons[season].sort(key=lambda e: e.episode)

        base_slug = ""
        first_season = min(seasons)
        parsed_base = parse_episode_slug(seasons[first_season][0].slug)
        if parsed_base:
            base_slug = parsed_base[0]

        total_eps = sum(len(v) for v in seasons.values())
        self._log(f"  Serie: «{series_title}» – {len(seasons)} Staffel(n), {total_eps} Episoden")

        return FilmpalastSeries(
            title=series_title, base_slug=base_slug, url=url,
            cover_url=cover_url, description=description, genres=genres,
            seasons=seasons,
        )

    @staticmethod
    def _group_episode_results(results: List[FilmpalastSearchResult]) -> List[FilmpalastSeriesResult]:
        """
        Gruppiert Episoden-Treffer (jede Episode ist auf filmpalast ein
        eigener /stream/-Treffer, z.B. "The Bear S05E01") zu jeweils EINEM
        Serien-Eintrag pro Slug-Basis (<serie>-sNNeMM).
        """
        grouped: "OrderedDict[str, FilmpalastSeriesResult]" = OrderedDict()
        for r in results:
            parsed = parse_episode_slug(r.slug)
            if not parsed:
                continue
            base_slug, season, episode = parsed
            if base_slug not in grouped:
                title = strip_episode_suffix(r.title)
                grouped[base_slug] = FilmpalastSeriesResult(
                    title=title or base_slug, base_slug=base_slug,
                    sample_slug=r.slug, sample_url=r.url, year=r.year,
                )
            else:
                # Niedrigste Staffel/Episode als Sample bevorzugen (stabiler für get_series)
                existing = grouped[base_slug]
                existing_parsed = parse_episode_slug(existing.sample_slug)
                if existing_parsed and (season, episode) < (existing_parsed[1], existing_parsed[2]):
                    existing.sample_slug = r.slug
                    existing.sample_url = r.url
        return list(grouped.values())

    def search_series(self, query: str) -> List[FilmpalastSeriesResult]:
        """Sucht nach Serien (Titel-Suche, dann Episoden-Treffer gruppiert)."""
        query = (query or "").strip()
        if not query:
            return []
        results = self.search(query)
        grouped = self._group_episode_results(results)
        self._log(f"  {len(grouped)} Serie(n) gefunden ({len(results)} Episoden-Treffer)")
        return grouped

    def list_series(self, page: int = 1) -> List[FilmpalastSeriesResult]:
        """
        Zuletzt aktualisierte Serien (neue Episoden zuerst) – zum Stöbern
        ohne einen Titel zu kennen. Quelle: filmpalast.to/serien/view.
        """
        page = max(1, int(page))
        url = f"{BASE_URL}/serien/view" if page == 1 else f"{BASE_URL}/serien/view/page/{page}"
        self._log(f"Serien-Katalog (neu): Seite {page}")
        soup = self._get_soup(url)
        results = self._parse_listing_soup(soup)
        grouped = self._group_episode_results(results)
        self._log(f"  {len(grouped)} Serie(n) (Seite {page})")
        return grouped

    def list_series_alpha(self, letter: str, page: int = 1) -> List[FilmpalastSeriesResult]:
        """
        Serien alphabetisch durchstöbern (z.B. 'A', '0-9'). Liefert je Serie
        nur die erste Episode (S01E01) – anders als list_series()/search_series()
        keine Duplikate durch "zuletzt aktualisiert".
        """
        letter = (letter or "A").strip()
        page = max(1, int(page))
        url = f"{BASE_URL}/search/serien/alpha/{quote(letter)}"
        if page > 1:
            url += f"/{page}"
        self._log(f"Serien-Katalog: '{letter}', Seite {page}")
        soup = self._get_soup(url)
        results = self._parse_listing_soup(soup)
        grouped = self._group_episode_results(results)
        self._log(f"  {len(grouped)} Serie(n) mit '{letter}' (Seite {page})")
        return grouped

    def _extract_all_hosters(self, soup: BeautifulSoup) -> List[HosterInfo]:
        """
        Sammelt ALLE Hoster auf der Filmseite.

        filmpalast gruppiert Hoster in <ul class="currentStreamLinks">.
        Pro Block:
          <p class="hostName">VOE HD</p>          ← Name + Qualität
          <a class="button iconPlay" href="...">   ← Link

        Manche Hoster haben einen language-Tag in einem data-Attribut,
        manche zeigen ihn nur im Text. Wir parsen Sprache aus dem Hoster-Text
        wenn möglich (z.B. "VOE HD Deutsch" → "Deutsch").
        """
        hosters: List[HosterInfo] = []
        seen_urls: set = set()

        for ul in soup.find_all("ul", class_="currentStreamLinks"):
            host_p = ul.find("p", class_="hostName")
            if not host_p:
                continue
            host_text = host_p.get_text(strip=True)
            if not host_text:
                continue

            # Blacklist-Check (Werbe-Redirects etc.)
            if any(b in host_text.lower() for b in HOSTER_BLACKLIST):
                continue

            name, quality, language = self._parse_hoster_text(host_text)

            a = ul.find("a", class_="iconPlay", href=True)
            if not a:
                a = ul.find("a", href=True)
            if not a:
                continue

            href = a.get("href", "").strip()
            if not href:
                continue
            if href.startswith("//"):
                href = "https:" + href
            elif href.startswith("/"):
                href = BASE_URL + href

            if href in seen_urls:
                continue
            seen_urls.add(href)

            hosters.append(HosterInfo(
                name=name, url=href, language=language, quality=quality,
            ))

        # Sortierung: Deutsch zuerst, dann HD, dann nach Hoster-Priorität
        def sort_key(h: HosterInfo):
            lang_prio = 0 if h.is_de else 1
            hd_prio = 0 if h.is_hd else 1
            name_l = h.name.lower()
            hoster_prio = (
                HOSTER_PRIORITY.index(name_l) if name_l in HOSTER_PRIORITY else 99
            )
            return (lang_prio, hd_prio, hoster_prio)

        hosters.sort(key=sort_key)
        return hosters

    @staticmethod
    def _parse_hoster_text(text: str) -> Tuple[str, str, str]:
        """
        Parst "VOE HD Deutsch" → ("VOE", "HD", "Deutsch")
        Pro Hoster kann der Text variieren, hier Heuristiken.
        """
        t = text.strip()
        # Qualität
        quality = ""
        for q in ["HD", "SD", "1080p", "720p", "480p", "4K", "CAM", "TS"]:
            if q.lower() in t.lower():
                quality = q
                break
        # Sprache
        language = ""
        for lang, markers in [
            ("Deutsch", ["deutsch", "german", "de "]),
            ("Englisch", ["englisch", "english", "en "]),
            ("Original", ["original"]),
        ]:
            if any(m in t.lower() for m in markers):
                language = lang
                break
        # Name = erster zusammenhängender Wortblock, ohne Qualität/Sprache
        name = t
        for strip_word in [quality, language, "HD", "SD", "1080p", "720p",
                           "Deutsch", "Englisch", "Original"]:
            name = re.sub(re.escape(strip_word), "", name, flags=re.IGNORECASE)
        name = re.sub(r"\s+", " ", name).strip()
        if not name:
            name = t.split()[0] if t.split() else "Unknown"
        return name, quality, language
