"""Gemeinsame Datenmodelle und Slug-Helfer aller Anbieteradapter."""

import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple


EPISODE_SLUG_RE = re.compile(
    r"^(?P<base>.+)-s(?P<season>\d{1,2})e(?P<episode>\d{1,3})$",
    re.IGNORECASE,
)


def parse_episode_slug(slug: str) -> Optional[Tuple[str, int, int]]:
    """Zerlegt einen Episoden-Slug in Basis, Staffel und Episode."""
    match = EPISODE_SLUG_RE.match(slug or "")
    if not match:
        return None
    return match.group("base"), int(match.group("season")), int(match.group("episode"))


def strip_episode_suffix(title: str) -> str:
    """Entfernt eine abschließende SxxExx-Kennung aus einem Titel."""
    return re.sub(
        r"\s*S\d{1,2}E\d{1,3}\s*$",
        "",
        title or "",
        flags=re.IGNORECASE,
    ).strip()


@dataclass
class FilmpalastSearchResult:
    """Normalisierter Suchtreffer eines Medienanbieters."""

    title: str
    slug: str
    url: str
    year: str = ""
    is_movie: bool = True
    provider: str = ""
    content_language: str = ""
    cover_url: str = ""


@dataclass
class HosterInfo:
    """Normalisierter Stream-Hoster eines Medieneintrags."""

    name: str
    url: str
    language: str = ""
    quality: str = ""

    @property
    def is_de(self) -> bool:
        return self.language.lower().startswith("deutsch") or self.language.lower() == "de"

    @property
    def is_hd(self) -> bool:
        quality = (self.quality or "").upper()
        return "HD" in quality or "1080" in quality or "720" in quality

    def __repr__(self):
        language = f"[{self.language}]" if self.language else ""
        quality = f"({self.quality})" if self.quality else ""
        return f"HosterInfo({self.name}{language}{quality})"


@dataclass
class FilmpalastMovie:
    """Normalisierter Film oder eine aufgelöste Episode mit Hostern."""

    title: str
    url: str
    year: str = ""
    runtime: str = ""
    cover_url: str = ""
    description: str = ""
    genres: List[str] = field(default_factory=list)
    hosters: List[HosterInfo] = field(default_factory=list)
    provider: str = ""
    content_language: str = ""

    @property
    def voe_url(self) -> Optional[str]:
        for hoster in self.hosters:
            if hoster.name.lower() == "voe":
                return hoster.url
        return None

    def has_hoster(self, name: str) -> bool:
        normalized = name.lower()
        return any(hoster.name.lower() == normalized for hoster in self.hosters)

    def get_hoster(self, name: str) -> Optional[HosterInfo]:
        normalized = name.lower()
        for hoster in self.hosters:
            if hoster.name.lower() == normalized:
                return hoster
        return None


@dataclass
class SeriesEpisode:
    """Normalisierte Episode innerhalb einer Serie."""

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
    """Normalisierte Serie mit gruppierten Staffeln und Episoden."""

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
        episodes: List[SeriesEpisode] = []
        for season in self.season_numbers:
            episodes.extend(self.seasons[season])
        return episodes

    def episodes_in_seasons(self, seasons: List[int]) -> List[SeriesEpisode]:
        wanted = set(seasons)
        return [episode for episode in self.all_episodes if episode.season in wanted]


@dataclass
class FilmpalastSeriesResult:
    """Normalisierter gruppierter Serientreffer."""

    title: str
    base_slug: str
    sample_slug: str
    sample_url: str
    year: str = ""
    cover_url: str = ""
