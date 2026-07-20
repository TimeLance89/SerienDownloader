"""
Royal Downloader – lokaler Webserver.

Ersetzt die frühere customtkinter-GUI (main.py) durch eine HTML/CSS/JS-
Oberfläche, die im Standardbrowser läuft. Anbieteradapter liegen gebündelt im
Paket ``providers``; dieser Server bildet die REST-/WebSocket-Schicht darüber.

Start: python server.py  (öffnet automatisch den Browser)
"""

import logging
import os
import re
import shutil
import threading
import time
import tempfile
import webbrowser
import base64
import ipaddress
import secrets
import socket
import sys
import requests
from contextlib import asynccontextmanager
from collections import Counter, OrderedDict, defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Callable, Dict, List, Optional
from urllib.parse import urlparse

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Response, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from starlette.concurrency import run_in_threadpool

from providers.filmpalast import FilmpalastScraper
from providers.models import (
    FilmpalastMovie, FilmpalastSearchResult,
    FilmpalastSeries, FilmpalastSeriesResult,
    parse_episode_slug, strip_episode_suffix,
)
from providers.catalog import (
    PROVIDER_CATALOG,
    normalize_content_language,
    provider_catalog_payload,
    provider_content_language,
    provider_for_source,
    provider_language_payload,
)
from extractor import (
    VOEBrowserPool, extract_stream_url, pre_check_voe, VOE_NOT_FOUND, extract_doodstream_url,
    extract_firestream_url, extract_vidara_url, extract_vidsonic_url,
)
from downloader import (
    DownloadJob, DownloadQueue, build_filename, build_movie_filename,
    probe_stream_url, validate_media_file, cleanup_stale_staging,
    _sanitize as sanitize_filename,
)
from session_manager import _cookie_file_for
from hoster_intel import HosterIntel
from providers.filmfrei24 import (
    BASE_URL as FILMFREI24_BASE_URL,
    FilmFrei24Scraper,
    SOURCE_PREFIX as FILMFREI24_PREFIX,
)
from providers.moflix import MoflixScraper, SOURCE_PREFIX as MOFLIX_PREFIX
from providers.einschalten import EinschaltenScraper, SOURCE_PREFIX as EINSCHALTEN_PREFIX
from providers.kinox import KinoxScraper, SOURCE_PREFIX as KINOX_PREFIX
from providers.kinoger import KinogerScraper, SOURCE_PREFIX as KINOGER_PREFIX
from providers.megakino import MegaKinoScraper, SOURCE_PREFIX as MEGAKINO_PREFIX
from providers.xcine import XcineScraper, SOURCE_PREFIX as XCINE_PREFIX
from providers.sflix import (
    BASE_URL as SFLIX_BASE_URL,
    SflixScraper,
    SOURCE_PREFIX as SFLIX_PREFIX,
)
from providers.ridomovies import (
    BASE_URL as RIDOMOVIES_BASE_URL,
    RidomoviesScraper,
    SOURCE_PREFIX as RIDOMOVIES_PREFIX,
)
from providers.mkissa import (
    BASE_URL as MKISSA_BASE_URL,
    MkissaScraper,
    SOURCE_PREFIX as MKISSA_PREFIX,
    anime_episode_page,
)
from providers.serienstream import SerienstreamScraper, SOURCE_PREFIX as SERIENSTREAM_PREFIX
from jellyfin_client import JellyfinClient
from jellyfin_recommender import (
    Config as JellyfinRecommenderConfig,
    ConfigurationError as JellyfinRecommenderConfigurationError,
    RecommenderError as JellyfinRecommenderError,
    run_once as run_jellyfin_recommender_once,
)
from tmdb_client import SERIES_CACHE_TTL, TMDBClient
from telegram_bot import TelegramBot
from seerr_client import SeerrClient, SeerrRequest
from update_checker import UpdateChecker
from self_updater import SelfUpdater
from ytdlp_updater import YtDlpRuntimeUpdater
from ui_translator import (
    SUPPORTED_UI_LANGUAGES,
    UITranslator,
    normalize_ui_language,
)
from watchlist_policy import (
    CLEANUP_MODE_KEEP,
    CLEANUP_MODE_LABELS,
    WATCH_MODE_DEFAULT,
    WATCH_MODE_LABELS,
    WATCH_MODE_NEXT_SEASON,
    normalize_cleanup_mode,
    normalize_episode_history,
    normalize_watch_mode,
    select_cleanup_items,
    select_missing_episode_slugs,
    serialize_episode_history,
)
import config as appconfig

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
for noisy_logger in ("websockets", "nodriver", "urllib3"):
    logging.getLogger(noisy_logger).setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# nodriver 0.50.3 liefert cdp/network.py mit ungültigem UTF-8 aus (siehe
# nodriver_patch). Auf frischen Installationen (Docker/NAS) scheitert sonst
# schon `import nodriver` → VOE-Extraktion tot. Einmal beim Start reparieren,
# BEVOR irgendein Codepfad nodriver importiert.
import nodriver_patch  # noqa: E402 - Reparatur muss vor dem ersten nodriver-Import laufen.
nodriver_patch.ensure_cdp_utf8()

APP_DIR = Path(__file__).parent
WEB_DIR = APP_DIR / "web"
APP_USERNAME = os.environ.get("APP_USERNAME", "").strip()
APP_PASSWORD = os.environ.get("APP_PASSWORD", "")
AUTH_ENABLED = bool(APP_USERNAME and APP_PASSWORD)
UPDATE_CHECKER = UpdateChecker(
    repository=os.environ.get("UPDATE_GITHUB_REPOSITORY", "TimeLance89/RoyalDownloader"),
    branch=os.environ.get("UPDATE_GITHUB_BRANCH", "main"),
    app_dir=APP_DIR,
)
UI_TRANSLATOR = UITranslator()
PROVIDER_LABELS = {
    key: definition.label
    for key, definition in PROVIDER_CATALOG.items()
}
MOVIE_BROWSE_PAGE_SIZE = 32
MOVIE_PAGINATED_PROVIDERS = frozenset({
    "filmpalast", "megakino", "kinoger", "xcine", "sflix", "ridomovies",
})
MOVIE_LIST_CACHE_TTL = 300
MOVIE_LIST_FAILURE_CACHE_TTL = 30
MOVIE_LIST_CACHE_MAX_ENTRIES = 1000
MOVIE_MAX_GLOBAL_PAGE = 50
MOVIE_MAX_SOURCE_PAGE = 50
MOVIE_MAX_COLD_WAVES_PER_REQUEST = 2
TMDB_MOVIE_BATCH_MAX_WORKERS = 8
MOVIE_GENRE_GROUPS = {
    "Animation": ("Animation", "Zeichentrick"),
    "Biografie": ("Biografie", "Biographie"),
    "Dokumentation": ("Dokumentation", "Dokumentarfilm"),
    "Geschichte": ("Geschichte", "Historie"),
    "Krieg": ("Krieg", "Kriegsfilm"),
    "Romantik": ("Romantik", "Romance", "Liebesfilm"),
    "Science-Fiction": ("Science-Fiction", "Science Fiction", "Sci-Fi"),
}
MOVIE_GENRE_CANONICAL_BY_KEY = {
    alias.casefold(): canonical
    for canonical, aliases in MOVIE_GENRE_GROUPS.items()
    for alias in aliases
}
SERIES_BROWSE_PAGE_SIZE = 32
SERIES_PAGINATED_PROVIDERS = frozenset({
    "filmpalast", "megakino", "kinoger", "xcine", "sflix", "ridomovies",
})
SERIES_ALPHA_PROVIDERS = frozenset({"serienstream", "filmpalast"})
SERIES_LIST_CACHE_TTL = 300
SERIES_LIST_FAILURE_CACHE_TTL = 30
SERIES_LIST_CACHE_MAX_ENTRIES = 500
SERIES_MAX_GLOBAL_PAGE = 50
SERIES_MAX_SOURCE_PAGE = 50
SERIES_MAX_COLD_WAVES_PER_REQUEST = 2


def _authorized_header(value: str) -> bool:
    if not AUTH_ENABLED:
        return True
    if not value or not value.startswith("Basic "):
        return False
    try:
        decoded = base64.b64decode(value[6:], validate=True).decode("utf-8")
        username, password = decoded.split(":", 1)
    except Exception:
        return False
    return secrets.compare_digest(username, APP_USERNAME) and secrets.compare_digest(password, APP_PASSWORD)


# ---------------------------------------------------------------------------
# App-State (Ein-Nutzer, in-memory – entspricht den Instanzvariablen der
# früheren tkinter-App-Klasse)
# ---------------------------------------------------------------------------
class AppState:
    def __init__(self):
        self.save_path: str = appconfig.load()              # Zielordner Filme
        self.series_path: str = appconfig.load_series_path()  # Zielordner Serien (getrennt)
        self.ui_language: str = appconfig.load_ui_language()
        self.ui_language_lock = threading.RLock()
        self.watchlist: List[dict] = appconfig.load_watchlist()
        self.watchlist_lock = threading.RLock()
        self.auto_download_lock = threading.Lock()
        self.hoster_intel = HosterIntel()

        self.jellyfin_cfg: dict = appconfig.load_jellyfin()
        self.tmdb_cfg: dict = appconfig.load_tmdb()
        self.tmdb_client = TMDBClient(**self.tmdb_cfg)
        self.telegram_cfg: dict = appconfig.load_telegram()
        self.seerr_cfg: dict = appconfig.load_seerr()
        self.seerr_requests: Dict[str, dict] = appconfig.load_seerr_requests()
        self.seerr_requests_lock = threading.RLock()
        self.seerr_jobs: Dict[str, List[dict]] = {}
        self.seerr_jobs_lock = threading.RLock()
        self.seerr_poll_lock = threading.Lock()
        self.seerr_last_poll: float = 0.0
        self.seerr_last_success: float = 0.0
        self.seerr_last_error: str = ""
        self.seerr_last_scan_retry: float = 0.0
        self.seerr_scan_retry_lock = threading.Lock()
        self.seerr_moonfin_configured: bool = False
        self.seerr_moonfin_error: str = ""
        # Automatik (24/7): Auto-Download abonnierter Serien + Zeitsteuerung.
        self.automation: dict = appconfig.load_automation()
        self.updater_cfg: dict = appconfig.load_updater()
        self.updater_config_lock = threading.RLock()
        self.updater_runtime_lock = threading.RLock()
        self.updater_runtime: dict = {
            "last_auto_check": None,
            "auto_update_state": "idle",
            "auto_update_message": "",
        }
        self.provider_priorities: dict = appconfig.load_provider_priorities()
        self.provider_enabled: dict = appconfig.load_provider_enabled()
        self.content_languages: set[str] = set(appconfig.load_content_languages())
        self.provider_priority_lock = threading.RLock()
        self.jellyfin_library: Optional[List[dict]] = None
        self.jellyfin_library_time: float = 0.0
        self.jellyfin_library_available: bool = False
        self.jellyfin_library_retry_after: float = 0.0
        self.jellyfin_episodes: Optional[List[dict]] = None
        self.jellyfin_episodes_time: float = 0.0
        self.jellyfin_episodes_available: bool = False
        self.jellyfin_episodes_retry_after: float = 0.0
        self.jellyfin_series: Optional[List[dict]] = None
        self.jellyfin_series_time: float = 0.0
        self.jellyfin_series_available: bool = False
        self.jellyfin_series_retry_after: float = 0.0
        self.jellyfin_user_episodes: Optional[List[dict]] = None
        self.jellyfin_user_episodes_time: float = 0.0
        self.jellyfin_user_episodes_available: bool = False
        self.jellyfin_user_episodes_retry_after: float = 0.0
        self.jellyfin_config_generation: int = 0
        self.jellyfin_movie_data_generation: int = 0
        self.jellyfin_episode_data_generation: int = 0
        self.jellyfin_cache_lock = threading.RLock()
        self.jellyfin_config_update_lock = threading.Lock()
        self.jellyfin_library_fetch_lock = threading.Lock()
        self.jellyfin_episodes_fetch_lock = threading.Lock()
        self.jellyfin_series_fetch_lock = threading.Lock()
        self.jellyfin_user_fetch_lock = threading.Lock()
        self.jellyfin_refresh_lock = threading.Lock()
        self.jellyfin_refresh_request_lock = threading.Lock()
        self.jellyfin_refresh_running = False
        self.jellyfin_refresh_pending = False

        self.fp_movies: Dict[str, FilmpalastMovie] = {}
        self.movie_list_cache: Dict[tuple, tuple] = {}
        self.movie_list_cache_lock = threading.Lock()
        self.series_list_cache: Dict[tuple, tuple] = {}
        self.series_list_cache_lock = threading.Lock()
        self.series_catalog_lock = threading.Lock()
        self.picked: set = set(appconfig.load_queue())
        self.queue_content_keys: Dict[str, str] = {}
        self.done_slugs: set = set()
        self.queue_claim_lock = threading.RLock()

        self.fp_scraper: Optional[FilmpalastScraper] = None
        self.fp_lock = threading.Lock()
        # Hoster-Auflösung nutzt gemeinsame Browser-/Session-Objekte und muss
        # auch bei parallelen Download-Fallbacks seriell bleiben.
        self.hoster_extract_lock = threading.Lock()

        # serienstream.to – eigener Singleton, damit SessionManager (Cookies /
        # Rate-Limiting / Captcha-Clearance) über alle Aufrufe erhalten bleibt.
        self.sto_scraper: Optional[SerienstreamScraper] = None
        self.sto_lock = threading.Lock()
        self.mkissa_scraper: Optional[MkissaScraper] = None
        self.mkissa_lock = threading.RLock()

        self.fp_provider_genres: set = set()
        self.filmfrei24_provider_genres: set = set()
        self.moflix_provider_genres: set = set()
        self.einschalten_provider_genres: set = set()
        self.kinox_provider_genres: set = set()
        self.kinoger_provider_genres: set = set()
        self.megakino_provider_genres: set = set()
        self.xcine_provider_genres: set = set()
        self.sflix_provider_genres: set = set()
        self.ridomovies_provider_genres: set = set()

        self.series_cache: Dict[str, FilmpalastSeries] = {}
        self.series_dir_cache: Dict[tuple, Path] = {}
        self.media_validation_cache: Dict[str, tuple] = {}
        self.media_validation_lock = threading.Lock()
        self.series_page_size_ref: int = 1

        # Pro Download-Lauf: gematchte Serie beim Fallback-Anbieter (Filmpalast/
        # Moflix), damit bei serienstream-Gate nicht jede Episode neu gesucht wird.
        # Key: "<provider>:<norm_title>", Value: FilmpalastSeries oder None.
        self.fallback_series_cache: Dict[str, Optional[FilmpalastSeries]] = {}

        self.watchlist_new_slugs: Dict[str, set] = {}

        self.voe_pool: Optional[VOEBrowserPool] = None
        self.embed_pool: Optional[VOEBrowserPool] = None

        self.dl_queue = DownloadQueue(max_parallel=2)
        self.download_state_lock = threading.Lock()
        self.queue_prepare_lock = threading.Lock()
        self.queue_lifecycle_lock = threading.RLock()
        self.total_jobs = 0
        self.done_jobs = 0
        self.counted_queue_slugs: set[str] = set()
        # True während zwischen Captcha-Wellen noch Episoden nachgezogen werden –
        # dann darf on_queue_done NICHT „fertig" melden / Browser-Pools schließen.
        self.gated_retry_pending = False
        self.gated_retry_slugs: set[str] = set()
        # Zentraler Cooldown-Puffer fuer serienstream-Episoden. Einzelne
        # _QueuePreparationJobs duerfen nicht je einen eigenen Retry-Thread
        # starten, sonst treffen nach dem Cooldown alle gleichzeitig erneut auf
        # das Redirect-Gate.
        self.gated_retry_jobs: Dict[str, dict] = {}
        self.gated_retry_worker_running = False
        self.ytdlp_update_active = False

        self.cover_cache: "OrderedDict[str, tuple]" = OrderedDict()
        # Fehlschläge nur kurz merken (Timestamp), damit transiente Fehler
        # nicht bis zum Neustart als 502 hängen bleiben.
        self.cover_fail_cache: "OrderedDict[str, float]" = OrderedDict()
        self.cover_cache_lock = threading.Lock()

        # Telegram-Anfragen werden über den Film-Slug bis zum Download-Ende
        # verfolgt, damit anschließend auf die Jellyfin-Erkennung gewartet wird.
        self.telegram_jobs: Dict[str, dict] = {}
        self.telegram_series_requests: Dict[str, dict] = {}
        self.telegram_series_choices: Dict[str, dict] = {}
        self.telegram_jobs_lock = threading.Lock()
        self.telegram_choices_lock = threading.Lock()
        self.telegram_choices_publish_lock = threading.Lock()
        self.telegram_request_lock = threading.Lock()


state = AppState()


# ---------------------------------------------------------------------------
# WebSocket-Broadcast (Log / Fortschritt / Queue-Events)
# ---------------------------------------------------------------------------
class WSManager:
    def __init__(self):
        self.clients: List[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.clients.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self.clients:
            self.clients.remove(ws)

    async def send_all(self, data: dict):
        dead = []
        for ws in self.clients:
            try:
                await ws.send_json(data)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)


ws_manager = WSManager()
_main_loop = None  # wird in lifespan gesetzt
_telegram_bot: Optional[TelegramBot] = None
_background_services_started = False
_background_services_lock = threading.Lock()
_recommender_stop_event = threading.Event()
_recommender_wake_event = threading.Event()
_recommender_thread: Optional[threading.Thread] = None
_seerr_stop_event = threading.Event()
_seerr_wake_event = threading.Event()
_seerr_thread: Optional[threading.Thread] = None
_updater_stop_event = threading.Event()
_updater_wake_event = threading.Event()
_updater_thread: Optional[threading.Thread] = None
_ytdlp_updater_stop_event = threading.Event()
_ytdlp_updater_thread: Optional[threading.Thread] = None


def broadcast(data: dict):
    if _main_loop is None:
        return
    import asyncio
    try:
        asyncio.run_coroutine_threadsafe(ws_manager.send_all(data), _main_loop)
    except Exception:
        pass


def log(msg: str, level: str = ""):
    logger.info(msg)
    broadcast({"type": "log", "message": msg, "level": level})


def _restart_after_update(queue_already_paused: bool = False) -> None:
    def _restart():
        preserved = 0 if queue_already_paused else _pause_downloads_for_update_restart()
        if preserved:
            log(
                f"Update-Neustart: {preserved} offene Queue-Einträge gespeichert; "
                "sie werden danach automatisch fortgesetzt."
            )
        time.sleep(1)
        os.chdir(APP_DIR)
        start_script = APP_DIR / "start.sh"
        bash = shutil.which("bash")
        if os.name != "nt" and Path("/.dockerenv").exists() and bash and start_script.is_file():
            os.execv(bash, [bash, str(start_script)])
        os.execv(sys.executable, [sys.executable, str(APP_DIR / "server.py")])

    threading.Thread(target=_restart, daemon=True).start()


UPDATE_INSTALLER = SelfUpdater(
    repository=UPDATE_CHECKER.repository,
    app_dir=APP_DIR,
    on_state=lambda payload: broadcast({"type": "updater_install", "installer": payload}),
    restart_callback=_restart_after_update,
)
YTDLP_UPDATER = YtDlpRuntimeUpdater()

AUTO_UPDATE_START_DELAY_SECONDS = 30
AUTO_UPDATE_DEFER_SECONDS = 5 * 60
AUTO_UPDATE_ERROR_RETRY_SECONDS = 15 * 60


def _bounded_env_int(name: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(os.environ.get(name, str(default)) or default)
    except (TypeError, ValueError):
        value = default
    return max(minimum, min(maximum, value))


YTDLP_UPDATE_START_DELAY_SECONDS = _bounded_env_int(
    "YTDLP_UPDATE_START_DELAY_SECONDS", 300, 30, 24 * 60 * 60,
)
YTDLP_UPDATE_INTERVAL_HOURS = _bounded_env_int(
    "YTDLP_UPDATE_INTERVAL_HOURS", 24, 1, 168,
)
YTDLP_AUTO_UPDATE = os.environ.get(
    "YTDLP_AUTO_UPDATE", "true",
).strip().casefold() not in {"0", "false", "no", "off"}


def _updater_config_payload() -> dict:
    with state.updater_config_lock:
        config = dict(state.updater_cfg)
    with state.updater_runtime_lock:
        runtime = dict(state.updater_runtime)
    return {**config, **runtime}


def _set_updater_runtime(result: str, message: str, *, checked: bool = False) -> None:
    with state.updater_runtime_lock:
        state.updater_runtime["auto_update_state"] = result
        state.updater_runtime["auto_update_message"] = str(message or "")[:500]
        if checked:
            state.updater_runtime["last_auto_check"] = time.time()
    broadcast({"type": "updater_config", "config": _updater_config_payload()})


def _update_block_reason_locked() -> str:
    if state.dl_queue.active_count() or state.dl_queue.pending_count():
        return "Laufende oder wartende Downloads"
    if state.queue_prepare_lock.locked():
        return "Downloadvorbereitung oder Wiederholungsversuch läuft"
    _reconcile_idle_queue_state_locked()
    if state.gated_retry_pending:
        return "Downloadvorbereitung oder Wiederholungsversuch läuft"
    return ""


def _start_update_when_idle(target_sha: str) -> dict:
    """Startet das Update auch bei aktiver Queue.

    Downloads dürfen während des Ladens weiterlaufen. Direkt vor dem Neustart
    werden alle noch offenen Slugs persistent gesichert und die Prozesse sauber
    gestoppt; der neue Server stellt sie automatisch wieder her.
    """
    with state.queue_lifecycle_lock:
        if state.ytdlp_update_active:
            raise RuntimeError("yt-dlp wird gerade aktualisiert")
        queued = bool(
            state.dl_queue.active_count()
            or state.dl_queue.pending_count()
            or state.gated_retry_pending
        )
        result = UPDATE_INSTALLER.start(target_sha)
    if queued:
        log("Update wird installiert; die aktive Queue wird erst zum Neustart pausiert.")
    return result


def _attempt_automatic_update() -> str:
    with state.updater_config_lock:
        if state.updater_cfg.get("update_mode") != appconfig.UPDATE_MODE_AUTOMATIC:
            return "manual"

    try:
        update = UPDATE_CHECKER.check(True)
    except Exception as exc:
        message = f"GitHub-Prüfung fehlgeschlagen: {exc}"
        _set_updater_runtime("error", message, checked=True)
        log(f"Automatische Updateprüfung fehlgeschlagen: {exc}", "warn")
        return "error"

    if update.get("error"):
        message = str(update.get("error"))
        _set_updater_runtime("error", message, checked=True)
        log(f"Automatische Updateprüfung fehlgeschlagen: {message}", "warn")
        return "error"
    if update.get("update_available") is not True:
        if update.get("comparison") in {"identical", "behind"}:
            _set_updater_runtime("current", "Kein Update verfügbar.", checked=True)
            return "current"
        _set_updater_runtime(
            "unavailable",
            "Lokaler Build konnte nicht sicher mit GitHub verglichen werden.",
            checked=True,
        )
        return "unavailable"
    if update.get("comparison") != "ahead":
        _set_updater_runtime(
            "manual_required",
            "Lokaler und GitHub-Stand sind verzweigt; manuelle Bestätigung erforderlich.",
            checked=True,
        )
        return "manual_required"

    target_sha = str(update.get("latest_sha") or "").strip()
    if not target_sha:
        _set_updater_runtime("error", "GitHub lieferte keine installierbare Revision.", checked=True)
        return "error"

    try:
        with state.updater_config_lock:
            if state.updater_cfg.get("update_mode") != appconfig.UPDATE_MODE_AUTOMATIC:
                _set_updater_runtime("manual", "Automatische Installation wurde deaktiviert.", checked=True)
                return "manual"
        _start_update_when_idle(target_sha)
    except (RuntimeError, ValueError) as exc:
        message = str(exc)
        result = "deferred" if "zurückgestellt" in message else "error"
        _set_updater_runtime(result, message, checked=True)
        if result == "error":
            log(f"Automatisches Update konnte nicht gestartet werden: {message}", "warn")
        return result

    _set_updater_runtime("installing", "Update wird automatisch installiert.", checked=True)
    log(f"Automatisches Update auf Build {target_sha[:8]} gestartet.")
    return "installing"


def automatic_update_loop() -> None:
    if _updater_stop_event.wait(AUTO_UPDATE_START_DELAY_SECONDS):
        return
    _updater_wake_event.clear()
    while not _updater_stop_event.is_set():
        with state.updater_config_lock:
            config = dict(state.updater_cfg)
        if config.get("update_mode") == appconfig.UPDATE_MODE_AUTOMATIC:
            result = _attempt_automatic_update()
            if result == "deferred":
                delay = AUTO_UPDATE_DEFER_SECONDS
            elif result == "error":
                delay = AUTO_UPDATE_ERROR_RETRY_SECONDS
            else:
                delay = int(config.get("auto_update_interval_hours") or 6) * 60 * 60
        else:
            delay = 60 * 60
        _updater_wake_event.wait(max(1, delay))
        _updater_wake_event.clear()


def _attempt_ytdlp_runtime_update() -> str:
    """Aktualisiert yt-dlp stabil und erhält dabei alle Queue-Claims."""
    if not YTDLP_AUTO_UPDATE:
        return "disabled"
    if UPDATE_INSTALLER.is_active() or state.ytdlp_update_active:
        return "busy"
    try:
        update = YTDLP_UPDATER.check()
    except Exception as exc:
        log(f"yt-dlp-Updateprüfung fehlgeschlagen: {exc}", "warn")
        return "error"
    if not update.get("update_available"):
        logger.info("yt-dlp ist aktuell (%s).", update.get("current") or "unbekannt")
        return "current"

    current = str(update.get("current") or "nicht installiert")
    latest = str(update.get("latest") or "")
    log(f"yt-dlp-Update verfügbar: {current} → {latest}; Paket wird vorbereitet.")
    paused = False
    try:
        with tempfile.TemporaryDirectory(prefix="seriendownloader-ytdlp-") as tmp:
            wheel = YTDLP_UPDATER.download_wheel(latest, Path(tmp))
            with state.queue_lifecycle_lock:
                if UPDATE_INSTALLER.is_active() or state.ytdlp_update_active:
                    return "busy"
                state.ytdlp_update_active = True
            preserved = _pause_downloads_for_update_restart()
            paused = True
            if preserved:
                log(
                    f"yt-dlp-Update: {preserved} offene Queue-Einträge gespeichert; "
                    "Fortsetzung nach Neustart."
                )
            YTDLP_UPDATER.install_wheel(wheel)
    except Exception as exc:
        if state.ytdlp_update_active:
            # Auch bei einem pip-/Pause-Fehler neu starten, damit die Queue mit
            # der bisherigen Version weiterläuft.
            _restart_after_update(queue_already_paused=paused)
        log(f"yt-dlp-Update fehlgeschlagen: {exc}", "warn")
        return "error"

    log(f"yt-dlp {latest} installiert – Server startet neu.")
    _restart_after_update(queue_already_paused=True)
    return "restarting"


def ytdlp_runtime_update_loop() -> None:
    if not YTDLP_AUTO_UPDATE:
        return
    if _ytdlp_updater_stop_event.wait(YTDLP_UPDATE_START_DELAY_SECONDS):
        return
    while not _ytdlp_updater_stop_event.is_set():
        result = _attempt_ytdlp_runtime_update()
        delay = (
            60 * 60
            if result in {"busy", "error"}
            else YTDLP_UPDATE_INTERVAL_HOURS * 60 * 60
        )
        if _ytdlp_updater_stop_event.wait(delay):
            return


# ---------------------------------------------------------------------------
# Hilfsfunktionen (1:1 Logik aus der früheren main.py)
# ---------------------------------------------------------------------------
def get_fp_scraper() -> FilmpalastScraper:
    if state.fp_scraper is None:
        state.fp_scraper = FilmpalastScraper(progress_cb=log)
    return state.fp_scraper


def get_sto_scraper() -> SerienstreamScraper:
    if state.sto_scraper is None:
        state.sto_scraper = SerienstreamScraper(progress_cb=log)
    return state.sto_scraper


def get_mkissa_scraper() -> MkissaScraper:
    if state.mkissa_scraper is None:
        state.mkissa_scraper = MkissaScraper(progress_cb=log)
    return state.mkissa_scraper


def get_jellyfin_client() -> JellyfinClient:
    with state.jellyfin_cache_lock:
        cfg = dict(state.jellyfin_cfg)
    return JellyfinClient(cfg.get("url", ""), cfg.get("api_key", ""))


def _build_recommender_config() -> JellyfinRecommenderConfig:
    """Baut die Laufkonfiguration aus der persistenten settings.ini."""
    jellyfin = appconfig.load_jellyfin()
    env = {
        "JELLYFIN_URL": jellyfin.get("url", ""),
        "JELLYFIN_API_KEY": jellyfin.get("api_key", ""),
        "JELLYFIN_USER_ID": jellyfin.get("user_id", ""),
        "COLLECTION_NAME": os.environ.get("COLLECTION_NAME", "Für dich empfohlen"),
        "TOP_N": os.environ.get("TOP_N", "20"),
        "RECENCY_HALF_LIFE_DAYS": os.environ.get("RECENCY_HALF_LIFE_DAYS", "180"),
        "REQUEST_TIMEOUT_SECONDS": os.environ.get("REQUEST_TIMEOUT_SECONDS", "120"),
        "PAGE_SIZE": os.environ.get("PAGE_SIZE", "100"),
        # Das Intervall steuert der Server-Worker, nicht das Standalone-Script.
        "RUN_INTERVAL_SECONDS": "0",
    }
    return JellyfinRecommenderConfig.from_env(env)


def _run_recommender_once() -> bool:
    try:
        config = _build_recommender_config()
    except JellyfinRecommenderConfigurationError as exc:
        logger.info("Jellyfin-Empfehlungen übersprungen: %s", exc)
        return False

    try:
        recommendations = run_jellyfin_recommender_once(config)
    except JellyfinRecommenderError as exc:
        logger.warning("Jellyfin-Empfehlungen fehlgeschlagen: %s", exc)
        return False
    except Exception:
        logger.exception("Unerwarteter Fehler bei den Jellyfin-Empfehlungen")
        return False

    logger.info(
        "Jellyfin-Empfehlungen aktualisiert: %d Eintrag/Einträge",
        len(recommendations),
    )
    return True


def _recommender_interval_seconds() -> int:
    raw = os.environ.get("RECOMMENDER_INTERVAL_SECONDS", "86400").strip()
    try:
        interval = int(raw)
    except ValueError:
        logger.warning(
            "RECOMMENDER_INTERVAL_SECONDS=%r ist ungültig; nutze 86400", raw,
        )
        return 86400
    if interval < 60:
        logger.warning("RECOMMENDER_INTERVAL_SECONDS muss mindestens 60 sein; nutze 60")
        return 60
    return interval


def jellyfin_recommender_loop() -> None:
    while not _recommender_stop_event.is_set():
        successful = _run_recommender_once()
        regular_interval = _recommender_interval_seconds()
        interval = regular_interval if successful else min(regular_interval, 900)
        logger.info("Nächster Jellyfin-Empfehlungslauf in %d Sekunden", interval)
        _recommender_wake_event.wait(interval)
        _recommender_wake_event.clear()


def stop_jellyfin_recommender() -> None:
    _recommender_stop_event.set()
    _recommender_wake_event.set()
    thread = _recommender_thread
    if thread is not None and thread.is_alive() and thread is not threading.current_thread():
        thread.join(timeout=5)


def _set_runtime_jellyfin_config(cfg: dict) -> None:
    """Wechselt Konfiguration und Cache als eine atomare Generation."""
    normalized_cfg = dict(cfg)
    normalized_cfg["cleanup_default"] = normalize_cleanup_mode(
        normalized_cfg.get("cleanup_default")
    )
    with state.jellyfin_cache_lock:
        state.jellyfin_cfg = normalized_cfg
        state.jellyfin_config_generation += 1
        state.jellyfin_movie_data_generation += 1
        state.jellyfin_episode_data_generation += 1
        state.jellyfin_library = None
        state.jellyfin_library_time = 0.0
        state.jellyfin_library_available = False
        state.jellyfin_library_retry_after = 0.0
        state.jellyfin_episodes = None
        state.jellyfin_episodes_time = 0.0
        state.jellyfin_episodes_available = False
        state.jellyfin_episodes_retry_after = 0.0
        state.jellyfin_series = None
        state.jellyfin_series_time = 0.0
        state.jellyfin_series_available = False
        state.jellyfin_series_retry_after = 0.0
        state.jellyfin_user_episodes = None
        state.jellyfin_user_episodes_time = 0.0
        state.jellyfin_user_episodes_available = False
        state.jellyfin_user_episodes_retry_after = 0.0
        with state.watchlist_lock:
            for entry in state.watchlist:
                entry["check_generation"] = int(entry.get("check_generation", 0)) + 1
                entry["last_error"] = "Jellyfin-Konfiguration wird geprüft"


def get_tmdb_client() -> TMDBClient:
    return state.tmdb_client


def get_tmdb_series(title: str, tmdb_id="", force: bool = False) -> Optional[dict]:
    """Eine gespeicherte TMDB-ID bleibt autoritativ; Titelsuche nur initial."""
    client = get_tmdb_client()
    if tmdb_id:
        return client.series_by_id(tmdb_id, title, force=force)
    return client.series(title, force=force)


JELLYFIN_CACHE_TTL = 300  # Sekunden – wie lange die komplette Filmliste gecacht wird
JELLYFIN_ERROR_RETRY_SECONDS = 30


def get_jellyfin_library(force: bool = False) -> Optional[List[dict]]:
    """Liefert alle Filme aus Jellyfin (gecacht), damit auch Neu/Top/Genre-Listen
    ohne einen Live-Request pro Aufruf auf Duplikate geprüft werden können."""
    with state.jellyfin_library_fetch_lock:
        with state.jellyfin_cache_lock:
            jf_client = get_jellyfin_client()
            generation = state.jellyfin_config_generation
            now = time.time()
            needs_fetch = (
                force
                or state.jellyfin_library is None
                or (now - state.jellyfin_library_time) > JELLYFIN_CACHE_TTL
            )
            if not jf_client.configured:
                return None
            if not force and now < state.jellyfin_library_retry_after:
                return state.jellyfin_library
            needs_fetch = needs_fetch or not state.jellyfin_library_available
            if not needs_fetch:
                return state.jellyfin_library
            state.jellyfin_movie_data_generation += 1
        fresh = jf_client.list_movies()
        with state.jellyfin_cache_lock:
            if generation != state.jellyfin_config_generation:
                return state.jellyfin_library
            state.jellyfin_movie_data_generation += 1
            if fresh is not None:
                state.jellyfin_library = fresh
                state.jellyfin_library_time = time.time()
                state.jellyfin_library_available = True
                state.jellyfin_library_retry_after = 0.0
            else:
                state.jellyfin_library_available = False
                state.jellyfin_library_retry_after = time.time() + JELLYFIN_ERROR_RETRY_SECONDS
            return state.jellyfin_library


def get_jellyfin_episodes(force: bool = False) -> Optional[List[dict]]:
    """Liefert alle Serien-Episoden aus Jellyfin (gecacht) – damit die
    Watchlist-Prüfung weiß, ob eine neu gescrapete Episode tatsächlich
    noch fehlt oder bereits in der Bibliothek liegt."""
    with state.jellyfin_episodes_fetch_lock:
        with state.jellyfin_cache_lock:
            jf_client = get_jellyfin_client()
            generation = state.jellyfin_config_generation
            now = time.time()
            needs_fetch = (
                force
                or state.jellyfin_episodes is None
                or (now - state.jellyfin_episodes_time) > JELLYFIN_CACHE_TTL
            )
            if not jf_client.configured:
                return None
            if not force and now < state.jellyfin_episodes_retry_after:
                return state.jellyfin_episodes
            needs_fetch = needs_fetch or not state.jellyfin_episodes_available
            if not needs_fetch:
                return state.jellyfin_episodes
            state.jellyfin_episode_data_generation += 1
        fresh = jf_client.list_episodes()
        with state.jellyfin_cache_lock:
            if generation != state.jellyfin_config_generation:
                return state.jellyfin_episodes
            state.jellyfin_episode_data_generation += 1
            if fresh is not None:
                state.jellyfin_episodes = fresh
                state.jellyfin_episodes_time = time.time()
                state.jellyfin_episodes_available = True
                state.jellyfin_episodes_retry_after = 0.0
            else:
                state.jellyfin_episodes_available = False
                state.jellyfin_episodes_retry_after = time.time() + JELLYFIN_ERROR_RETRY_SECONDS
            return state.jellyfin_episodes


def get_jellyfin_series(force: bool = False) -> Optional[List[dict]]:
    """Liefert Jellyfin-Serien inklusive Provider-IDs für stabiles Matching."""
    with state.jellyfin_series_fetch_lock:
        with state.jellyfin_cache_lock:
            jf_client = get_jellyfin_client()
            generation = state.jellyfin_config_generation
            now = time.time()
            needs_fetch = (
                force
                or state.jellyfin_series is None
                or (now - state.jellyfin_series_time) > JELLYFIN_CACHE_TTL
            )
            if not jf_client.configured:
                return None
            if not force and now < state.jellyfin_series_retry_after:
                return state.jellyfin_series
            needs_fetch = needs_fetch or not state.jellyfin_series_available
            if not needs_fetch:
                return state.jellyfin_series
            state.jellyfin_episode_data_generation += 1
        fresh = jf_client.list_series()
        with state.jellyfin_cache_lock:
            if generation != state.jellyfin_config_generation:
                return state.jellyfin_series
            state.jellyfin_episode_data_generation += 1
            if fresh is not None:
                state.jellyfin_series = fresh
                state.jellyfin_series_time = time.time()
                state.jellyfin_series_available = True
                state.jellyfin_series_retry_after = 0.0
            else:
                state.jellyfin_series_available = False
                state.jellyfin_series_retry_after = time.time() + JELLYFIN_ERROR_RETRY_SECONDS
            return state.jellyfin_series


def get_jellyfin_user_episodes(force: bool = False) -> Optional[List[dict]]:
    """Liefert Episoden mit Gesehen-Status des konfigurierten Benutzers."""
    with state.jellyfin_user_fetch_lock:
        with state.jellyfin_cache_lock:
            jf_client = get_jellyfin_client()
            generation = state.jellyfin_config_generation
            user_id = state.jellyfin_cfg.get("user_id", "").strip()
            now = time.time()
            needs_fetch = (
                force
                or state.jellyfin_user_episodes is None
                or (now - state.jellyfin_user_episodes_time) > JELLYFIN_CACHE_TTL
            )
            if not jf_client.configured or not user_id:
                return None
            if not force and now < state.jellyfin_user_episodes_retry_after:
                return state.jellyfin_user_episodes
            needs_fetch = needs_fetch or not state.jellyfin_user_episodes_available
            if not needs_fetch:
                return state.jellyfin_user_episodes
            state.jellyfin_episode_data_generation += 1
        items = jf_client.list_episodes_with_user_data(user_id)
        with state.jellyfin_cache_lock:
            if generation != state.jellyfin_config_generation:
                return state.jellyfin_user_episodes
            state.jellyfin_episode_data_generation += 1
            if items is None:
                state.jellyfin_user_episodes_available = False
                state.jellyfin_user_episodes_retry_after = (
                    time.time() + JELLYFIN_ERROR_RETRY_SECONDS
                )
                return state.jellyfin_user_episodes
            state.jellyfin_user_episodes = items
            state.jellyfin_user_episodes_time = time.time()
            state.jellyfin_user_episodes_available = True
            state.jellyfin_user_episodes_retry_after = 0.0
            return state.jellyfin_user_episodes


def strip_source_suffix(title: str) -> str:
    """Entfernt die UI-Markierung " [Anbieter]" (Moflix/Einschalten/Kinox) für
    den Jellyfin-Titelabgleich."""
    return re.sub(r"\s*\[[^\]]+\]\s*$", "", title or "")


def provider_order(media_type: str) -> List[str]:
    defaults = {
        "movies": appconfig.MOVIE_PROVIDER_DEFAULTS,
        "series": appconfig.SERIES_PROVIDER_DEFAULTS,
        "anime": appconfig.ANIME_PROVIDER_DEFAULTS,
    }.get(media_type, ())
    with state.provider_priority_lock:
        configured = state.provider_priorities.get(media_type, defaults)
        return appconfig.normalize_provider_order(configured, defaults)


def provider_priority(media_type: str) -> List[str]:
    """Aktive, sprachlich passende Quellen in Benutzer-Reihenfolge."""
    ordered = provider_order(media_type)
    with state.provider_priority_lock:
        configured = state.provider_enabled.get(media_type, ordered)
        enabled = set(appconfig.normalize_provider_selection(configured, ordered))
        languages = set(state.content_languages)
    matching = [
        provider
        for provider in ordered
        if provider_content_language(provider) in languages
    ]
    active = [provider for provider in matching if provider in enabled]
    if media_type == "anime":
        return active
    return active or matching[:1] or ordered[:1]


def provider_for_value(value: str) -> str:
    """Erkennt die Katalogquelle an den zentral hinterlegten Merkmalen."""
    return provider_for_source(value)


def _apply_provider_metadata(item, provider: str):
    """Ergänzt normalisierte Medienobjekte um Quelle und Standardsprache."""
    if item is None:
        return None
    key = str(provider or "").strip().casefold()
    if hasattr(item, "provider"):
        item.provider = key
    if hasattr(item, "content_language"):
        item.content_language = provider_content_language(key)
    return item


def _apply_provider_metadata_many(items, provider: str) -> list:
    return [
        _apply_provider_metadata(item, provider)
        for item in (items or [])
        if item is not None
    ]


def _movie_provider(movie: Optional[FilmpalastMovie], fallback: str = "") -> str:
    stored = str(getattr(movie, "provider", "") or "").strip().casefold()
    if stored in PROVIDER_CATALOG:
        return stored
    value = getattr(movie, "url", "") if movie is not None else fallback
    return provider_for_value(value or fallback)


def _movie_content_language(
    movie: Optional[FilmpalastMovie],
    hoster_language: str = "",
    fallback: str = "",
) -> str:
    explicit = normalize_content_language(hoster_language)
    if explicit:
        return explicit
    if movie is not None:
        stored = normalize_content_language(
            str(getattr(movie, "content_language", "") or "")
        )
        if stored:
            return stored
    return provider_content_language(_movie_provider(movie, fallback))


def _ordered_episode_sources(movies: List[FilmpalastMovie]) -> List[FilmpalastMovie]:
    positions = {provider: index for index, provider in enumerate(provider_priority("series"))}
    return sorted(
        movies,
        key=lambda movie: positions.get(provider_for_value(movie.url), len(positions)),
    )


def clean_genre(value: str) -> str:
    return " ".join(str(value or "").split())


def canonical_movie_genre(value: str) -> str:
    genre = clean_genre(value)
    return MOVIE_GENRE_CANONICAL_BY_KEY.get(genre.casefold(), genre)


def movie_genre_aliases(value: str) -> tuple[str, ...]:
    canonical = canonical_movie_genre(value)
    return MOVIE_GENRE_GROUPS.get(canonical, (canonical,))


def watchlist_lookup(base_slug: str) -> Optional[dict]:
    return next((w for w in state.watchlist if w["base_slug"] == base_slug), None)


def load_movie_for_slug(slug: str) -> Optional[FilmpalastMovie]:
    provider = provider_for_value(slug)
    if slug.startswith(FILMFREI24_PREFIX):
        movie = FilmFrei24Scraper(progress_cb=log).get_movie(slug)
    elif slug.startswith(SERIENSTREAM_PREFIX):
        with state.sto_lock:
            movie = get_sto_scraper().get_movie(slug)
    elif slug.startswith(MOFLIX_PREFIX):
        movie = MoflixScraper(progress_cb=log).get_movie(slug)
    elif slug.startswith(EINSCHALTEN_PREFIX):
        movie = EinschaltenScraper(progress_cb=log).get_movie(slug)
    elif slug.startswith(KINOX_PREFIX):
        movie = KinoxScraper(progress_cb=log).get_movie(slug)
    elif slug.startswith(KINOGER_PREFIX):
        movie = KinogerScraper(progress_cb=log).get_movie(slug)
    elif slug.startswith(MEGAKINO_PREFIX):
        movie = MegaKinoScraper(progress_cb=log).get_movie(slug)
    elif slug.startswith(XCINE_PREFIX):
        movie = XcineScraper(progress_cb=log).get_movie(slug)
    elif slug.startswith(SFLIX_PREFIX):
        movie = SflixScraper(progress_cb=log).get_movie(slug)
    elif slug.startswith(RIDOMOVIES_PREFIX):
        movie = RidomoviesScraper(progress_cb=log).get_movie(slug)
    elif slug.startswith(MKISSA_PREFIX):
        with state.mkissa_lock:
            movie = get_mkissa_scraper().get_episode(slug)
    else:
        if slug.lower().startswith(("http://", "https://")):
            host = (urlparse(slug).hostname or "").casefold()
            if host != "filmpalast.to" and not host.endswith(".filmpalast.to"):
                raise ValueError("Direkte URLs sind nur für Filmpalast erlaubt.")
        scraper = get_fp_scraper()
        with state.fp_lock:
            movie = scraper.get_movie(slug)
    return _apply_provider_metadata(movie, provider)


def search_movie_candidates(query: str) -> List[FilmpalastSearchResult]:
    """Durchsucht alle Filmanbieter; gemeinsame Basis für Web und Telegram."""
    q = query.strip()
    if not q:
        return []
    def _fp():
        with state.fp_lock:
            return list(get_fp_scraper().search(q))

    searches = {
        "filmfrei24": lambda: FilmFrei24Scraper(progress_cb=log).search(q),
        "filmpalast": _fp,
        "moflix": lambda: MoflixScraper(progress_cb=log).search(q),
        "einschalten": lambda: EinschaltenScraper(progress_cb=log).search(q),
        "kinox": lambda: KinoxScraper(progress_cb=log).search(q),
        "kinoger": lambda: KinogerScraper(progress_cb=log).search(q),
        "megakino": lambda: MegaKinoScraper(progress_cb=log).search(q),
        "xcine": lambda: XcineScraper(progress_cb=log).search(q),
        "sflix": lambda: SflixScraper(progress_cb=log).search(q),
        "ridomovies": lambda: RidomoviesScraper(progress_cb=log).search(q),
    }
    tasks = [
        (key, PROVIDER_LABELS[key], searches[key])
        for key in provider_priority("movies")
    ]
    results: List[FilmpalastSearchResult] = []
    with ThreadPoolExecutor(max_workers=len(tasks)) as pool:
        futures = [(key, name, pool.submit(fn)) for key, name, fn in tasks]
        for key, name, future in futures:
            try:
                results.extend(_apply_provider_metadata_many(future.result(), key))
            except Exception as exc:
                log(f"{name} Suche übersprungen: {exc}", "warn")
    return results


class MovieCatalogColdLoadLimit(RuntimeError):
    """Verhindert teure Sprünge über viele noch ungecachte Quellseiten."""


def _cached_movie_provider_page(cache_key: tuple) -> Optional[List[FilmpalastSearchResult]]:
    with state.movie_list_cache_lock:
        cached = state.movie_list_cache.get(cache_key)
        ttl = cached[2] if cached and len(cached) > 2 else MOVIE_LIST_CACHE_TTL
        if cached and time.time() - cached[0] < ttl:
            return list(cached[1])
        if cached:
            state.movie_list_cache.pop(cache_key, None)
    return None


def _cache_movie_provider_page(
    cache_key: tuple,
    results: List[FilmpalastSearchResult],
    ttl: int = MOVIE_LIST_CACHE_TTL,
) -> None:
    now = time.time()
    with state.movie_list_cache_lock:
        expired = [
            key for key, cached in state.movie_list_cache.items()
            if now - cached[0] >= (
                cached[2] if len(cached) > 2 else MOVIE_LIST_CACHE_TTL
            )
        ]
        for key in expired:
            state.movie_list_cache.pop(key, None)
        while len(state.movie_list_cache) >= MOVIE_LIST_CACHE_MAX_ENTRIES:
            oldest = min(state.movie_list_cache, key=lambda key: state.movie_list_cache[key][0])
            state.movie_list_cache.pop(oldest, None)
        state.movie_list_cache[cache_key] = (now, list(results), ttl)


def _fetch_movie_provider_page(
    provider: str,
    mode: str,
    genre: str,
    source_page: int,
) -> List[FilmpalastSearchResult]:
    """Lädt genau eine Quellseite; nur markierte Anbieter paginieren."""
    if provider not in MOVIE_PAGINATED_PROVIDERS and source_page != 1:
        return []
    provider_genre = _movie_genre_for_provider(provider, genre) if mode == "genre" else genre

    if provider == "filmpalast":
        with state.fp_lock:
            scraper = get_fp_scraper()
            if mode == "genre":
                results = scraper.list_by_genre(provider_genre, source_page)
            else:
                results = scraper.list_movies(mode, source_page)
        return _apply_provider_metadata_many(results, provider)

    scraper_classes = {
        "filmfrei24": FilmFrei24Scraper,
        "moflix": MoflixScraper,
        "einschalten": EinschaltenScraper,
        "kinox": KinoxScraper,
        "kinoger": KinogerScraper,
        "megakino": MegaKinoScraper,
        "xcine": XcineScraper,
        "sflix": SflixScraper,
        "ridomovies": RidomoviesScraper,
    }
    scraper_class = scraper_classes.get(provider)
    if scraper_class is None:
        return []
    scraper = scraper_class(progress_cb=log)
    if mode == "genre":
        results = scraper.list_by_genre(provider_genre, source_page)
    else:
        results = scraper.list_movies(mode, source_page)
    return _apply_provider_metadata_many(results, provider)


def _load_movie_provider_pages(
    mode: str,
    genre: str,
    requests_to_load: List[tuple[str, int]],
    cold_wave_budget: Optional[List[int]] = None,
) -> Dict[tuple[str, int], List[FilmpalastSearchResult]]:
    """Lädt mehrere Quellseiten parallel und cached sie unabhängig voneinander."""
    loaded: Dict[tuple[str, int], List[FilmpalastSearchResult]] = {}
    missing: List[tuple[str, int, tuple]] = []
    genre_key = clean_genre(genre).casefold()

    for provider, source_page in dict.fromkeys(requests_to_load):
        cache_key = ("provider", mode, genre_key, provider, int(source_page))
        cached = _cached_movie_provider_page(cache_key)
        if cached is None:
            missing.append((provider, source_page, cache_key))
        else:
            loaded[(provider, source_page)] = cached

    if not missing:
        return loaded
    if cold_wave_budget is not None:
        if cold_wave_budget[0] <= 0:
            raise MovieCatalogColdLoadLimit(
                "Dieser Katalogabschnitt wird noch vorbereitet. Bitte kurz warten und erneut versuchen."
            )
        cold_wave_budget[0] -= 1

    with ThreadPoolExecutor(max_workers=min(len(missing), len(PROVIDER_LABELS))) as pool:
        futures = [
            (
                provider,
                source_page,
                cache_key,
                pool.submit(_fetch_movie_provider_page, provider, mode, genre, source_page),
            )
            for provider, source_page, cache_key in missing
        ]
        for provider, source_page, cache_key, future in futures:
            try:
                results = list(future.result())
            except Exception as exc:
                label = PROVIDER_LABELS.get(provider, provider)
                log(f"{label} Liste (Quellseite {source_page}) übersprungen: {exc}", "warn")
                results = []
                _cache_movie_provider_page(
                    cache_key, results, ttl=MOVIE_LIST_FAILURE_CACHE_TTL,
                )
            else:
                _cache_movie_provider_page(cache_key, results)
            loaded[(provider, source_page)] = results
    return loaded


def _movie_provider_genres(provider: str) -> set:
    return {
        "filmfrei24": state.filmfrei24_provider_genres,
        "filmpalast": state.fp_provider_genres,
        "moflix": state.moflix_provider_genres,
        "einschalten": state.einschalten_provider_genres,
        "kinox": state.kinox_provider_genres,
        "kinoger": state.kinoger_provider_genres,
        "megakino": state.megakino_provider_genres,
        "xcine": state.xcine_provider_genres,
        "sflix": state.sflix_provider_genres,
        "ridomovies": state.ridomovies_provider_genres,
    }.get(provider, set())


def _movie_genre_for_provider(provider: str, genre: str) -> str:
    known_by_key = {
        clean_genre(item).casefold(): clean_genre(item)
        for item in _movie_provider_genres(provider)
    }
    for alias in movie_genre_aliases(genre):
        match = known_by_key.get(alias.casefold())
        if match:
            return match
    return clean_genre(genre)


def _provider_supports_movie_genre(provider: str, genre: str) -> bool:
    known_genres = _movie_provider_genres(provider)
    # Vor dem ersten Genre-Abruf sind die Mengen leer. Dann optimistisch laden;
    # der jeweilige Scraper kann ein unbekanntes Genre günstig mit [] ablehnen.
    if not known_genres:
        return True
    known_keys = {clean_genre(item).casefold() for item in known_genres}
    return any(alias.casefold() in known_keys for alias in movie_genre_aliases(genre))


def _movie_result_identity(
    result: FilmpalastSearchResult,
    provider: str,
    years_by_title: Dict[str, set[str]],
) -> tuple:
    title_key = _norm_title(strip_source_suffix(result.title))
    if not title_key:
        return ("source", provider, str(result.slug or result.url))
    year = str(result.year or "").strip()
    known_years = years_by_title.get(title_key, set())
    # Fehlt bei nur einer Quelle das Jahr, kann sie sicher dem einzigen bekannten
    # Jahr zugeordnet werden. Bei Remakes bleiben jahrlose Treffer separat.
    if not year and len(known_years) == 1:
        year = next(iter(known_years))
    return ("movie", title_key, year)


def _mix_movie_provider_results(
    provider_results: Dict[str, List[FilmpalastSearchResult]],
    priority: List[str],
    claimed_identities: Optional[set[tuple]] = None,
) -> List[tuple[str, FilmpalastSearchResult]]:
    """Dedupliziert eine Quellwelle und mischt sie fair im Round-Robin."""
    years_by_title: Dict[str, set[str]] = defaultdict(set)
    for results in provider_results.values():
        for result in results:
            title_key = _norm_title(strip_source_suffix(result.title))
            year = str(result.year or "").strip()
            if title_key and year:
                years_by_title[title_key].add(year)

    filtered: Dict[str, List[FilmpalastSearchResult]] = {provider: [] for provider in priority}
    seen_identities = claimed_identities if claimed_identities is not None else set()
    for provider in priority:
        for result in provider_results.get(provider, []):
            identity = _movie_result_identity(result, provider, years_by_title)
            if identity in seen_identities:
                continue
            seen_identities.add(identity)
            filtered[provider].append(result)

    mixed: List[tuple[str, FilmpalastSearchResult]] = []
    longest = max((len(results) for results in filtered.values()), default=0)
    for index in range(longest):
        for provider in priority:
            results = filtered[provider]
            if index < len(results):
                mixed.append((provider, results[index]))
    return mixed


def movie_catalog_page(mode: str, page: int = 1, genre: str = "") -> dict:
    """Erzeugt eine stabile globale 32er-Seite aus allen Filmkatalogen.

    Einseitige Anbieter speisen ihren gesamten Startbestand in die globalen
    Seiten ein. Weitere Quellseiten werden jeweils als abgeschlossene Welle
    gemischt und nur hinten angehängt, damit frühere Seitengrenzen stabil bleiben.
    """
    page = max(1, min(int(page), MOVIE_MAX_GLOBAL_PAGE))
    mode = "genre" if mode == "genre" else mode if mode in {"new", "top"} else "new"
    genre = canonical_movie_genre(genre)
    priority = provider_priority("movies")
    active = [
        provider for provider in priority
        if mode != "genre" or _provider_supports_movie_genre(provider, genre)
    ]
    provider_seen: Dict[str, set[str]] = {provider: set() for provider in priority}

    def unique_page(
        provider: str,
        results: List[FilmpalastSearchResult],
    ) -> List[FilmpalastSearchResult]:
        unique: List[FilmpalastSearchResult] = []
        for result in results:
            source_key = str(result.slug or result.url or result.title or "").strip()
            key = f"{source_key}\0{str(result.year or '').strip()}"
            if key in provider_seen[provider]:
                continue
            provider_seen[provider].add(key)
            unique.append(result)
        return unique

    cold_wave_budget = [MOVIE_MAX_COLD_WAVES_PER_REQUEST]
    first_pages = _load_movie_provider_pages(
        mode, genre, [(provider, 1) for provider in active], cold_wave_budget,
    )
    first_wave = {
        provider: unique_page(provider, first_pages.get((provider, 1), []))
        for provider in active
    }
    # Priorität entscheidet innerhalb derselben Quellwelle. Bereits katalogisierte
    # Filme werden von späteren Wellen nicht ersetzt; sonst würden Seiten springen.
    claimed_identities: set[tuple] = set()
    catalog_entries = _mix_movie_provider_results(
        first_wave, priority, claimed_identities,
    )

    paginated = [provider for provider in active if provider in MOVIE_PAGINATED_PROVIDERS]
    exhausted = {provider for provider in paginated if not first_wave[provider]}
    duplicate_only_pages = {provider: 0 for provider in paginated}
    target_end = page * MOVIE_BROWSE_PAGE_SIZE
    next_source_page = 2
    has_more_unverified = False

    while len(catalog_entries) <= target_end and next_source_page <= MOVIE_MAX_SOURCE_PAGE:
        pending = [provider for provider in paginated if provider not in exhausted]
        if not pending:
            break
        try:
            next_pages = _load_movie_provider_pages(
                mode, genre, [(provider, next_source_page) for provider in pending],
                cold_wave_budget,
            )
        except MovieCatalogColdLoadLimit:
            if len(catalog_entries) < target_end:
                raise
            # Die angeforderte Seite ist vollständig. Der nächste Klick darf die
            # preiswerte Folgeseiten-Prüfung in einem neuen Request fortsetzen.
            has_more_unverified = True
            break
        wave: Dict[str, List[FilmpalastSearchResult]] = {}
        for provider in pending:
            results = next_pages.get((provider, next_source_page), [])
            wave[provider] = unique_page(provider, results)
            if not results:
                exhausted.add(provider)
            elif not wave[provider]:
                duplicate_only_pages[provider] += 1
                if duplicate_only_pages[provider] >= 2:
                    exhausted.add(provider)
            else:
                duplicate_only_pages[provider] = 0
        catalog_entries.extend(_mix_movie_provider_results(
            wave, priority, claimed_identities,
        ))
        next_source_page += 1

    start = (page - 1) * MOVIE_BROWSE_PAGE_SIZE
    page_entries = catalog_entries[start:target_end]
    source_counts = Counter(provider for provider, _result in page_entries)
    sources = [
        {
            "key": provider,
            "label": PROVIDER_LABELS[provider],
            "content_language": provider_content_language(provider),
            "language_label": PROVIDER_CATALOG[provider].language_label,
            "count": source_counts[provider],
        }
        for provider in priority
        if source_counts[provider]
    ]
    return {
        "results": [result for _provider, result in page_entries],
        "page": page,
        "has_more": page < MOVIE_MAX_GLOBAL_PAGE and (
            len(catalog_entries) > target_end or has_more_unverified
        ),
        "sources": sources,
    }


def list_movie_candidates(mode: str, page: int = 1) -> List[FilmpalastSearchResult]:
    """Kompatibler Listen-Zugriff auf die globale, gemischte Katalogseite."""
    return list(movie_catalog_page(mode, page)["results"])


def warm_home_movie_cache():
    """Bereitet Film- und Serien-Startansicht vor dem ersten Browser vor."""
    try:
        movies = list_movie_candidates("new", 1)
        tmdb = get_tmdb_client()
        if not tmdb.configured or not movies:
            return

        unique = {}
        for movie in movies:
            title = strip_source_suffix(movie.title)
            unique.setdefault((_norm_title(title), str(movie.year or "")), (title, movie.year or ""))
        values = list(unique.values())
        # Das erste sichtbare Detail hat Vorrang. Erst danach den Rest mit
        # geringer Parallelität laden, damit die Startansicht nicht verhungert.
        tmdb.movie_summary(*values[0])
        remaining = values[1:]
        if remaining:
            with ThreadPoolExecutor(max_workers=min(3, len(remaining))) as pool:
                futures = [pool.submit(tmdb.movie_summary, title, year) for title, year in remaining]
                for future in futures:
                    try:
                        future.result()
                    except Exception as exc:
                        log(f"TMDB-Startcache: {exc}", "warn")
        log(f"Startansicht vorbereitet: {len(movies)} neue Filme.")
    except Exception as exc:
        log(f"Startansicht konnte nicht vorab geladen werden: {exc}", "warn")
    finally:
        warm_home_series_cache()


# --- Serienanbieter ----------------------------------------------------------
def _sto_get_series(value: str) -> Optional[FilmpalastSeries]:
    with state.sto_lock:
        return get_sto_scraper().get_series(value)


def _sto_search_series(query: str) -> List[FilmpalastSeriesResult]:
    with state.sto_lock:
        return get_sto_scraper().search_series(query)


def _search_series_for_provider(provider: str, query: str) -> List[FilmpalastSeriesResult]:
    if provider == "serienstream":
        return _sto_search_series(query)
    if provider == "filmpalast":
        with state.fp_lock:
            return get_fp_scraper().search_series(query)
    if provider == "moflix":
        return MoflixScraper(progress_cb=log).search_series(query)
    if provider == "kinoger":
        return KinogerScraper(progress_cb=log).search_series(query)
    if provider == "megakino":
        return MegaKinoScraper(progress_cb=log).search_series(query)
    if provider == "xcine":
        return XcineScraper(progress_cb=log).search_series(query)
    if provider == "sflix":
        return SflixScraper(progress_cb=log).search_series(query)
    if provider == "ridomovies":
        return RidomoviesScraper(progress_cb=log).search_series(query)
    return []


def _load_series_for_provider(provider: str, value: str) -> Optional[FilmpalastSeries]:
    if provider == "serienstream":
        return _sto_get_series(value)
    if provider == "filmpalast":
        with state.fp_lock:
            return get_fp_scraper().get_series(value)
    if provider == "moflix":
        return MoflixScraper(progress_cb=log).get_series(value)
    if provider == "kinoger":
        return KinogerScraper(progress_cb=log).get_series(value)
    if provider == "megakino":
        return MegaKinoScraper(progress_cb=log).get_series(value)
    if provider == "xcine":
        return XcineScraper(progress_cb=log).get_series(value)
    if provider == "sflix":
        return SflixScraper(progress_cb=log).get_series(value)
    if provider == "ridomovies":
        return RidomoviesScraper(progress_cb=log).get_series(value)
    return None


def _search_series_provider_results(
    query: str,
) -> Dict[str, List[FilmpalastSeriesResult]]:
    """Durchsucht alle Serienkataloge parallel und trennt die Treffer je Quelle."""
    q = query.strip()
    if not q:
        return {}
    priority = provider_priority("series")
    tasks = [
        (provider, lambda key=provider: _search_series_for_provider(key, q))
        for provider in priority
    ]
    provider_results: Dict[str, List[FilmpalastSeriesResult]] = {}
    with ThreadPoolExecutor(max_workers=len(tasks)) as pool:
        futures = [(provider, pool.submit(fn)) for provider, fn in tasks]
        for provider, future in futures:
            try:
                provider_results[provider] = list(future.result())
            except Exception as exc:
                log(f"{PROVIDER_LABELS[provider]} Seriensuche übersprungen: {exc}", "warn")
                provider_results[provider] = []
    return provider_results


def search_series_candidates(query: str) -> List[FilmpalastSeriesResult]:
    """Durchsucht alle Serienkataloge und behält die konfigurierte Reihenfolge."""
    provider_results = _search_series_provider_results(query)
    results: List[FilmpalastSeriesResult] = []
    for provider in provider_priority("series"):
        results.extend(provider_results.get(provider, []))
    return results


@dataclass(frozen=True)
class _SeriesCatalogEntry:
    """Ein sichtbarer Serientreffer mit bevorzugter und alternativen Quellen."""

    provider: str
    result: FilmpalastSeriesResult
    providers: tuple[str, ...]


class SeriesCatalogColdLoadLimit(RuntimeError):
    """Verhindert teure Sprünge über viele noch ungecachte Serienseiten."""


def _series_result_identity(
    result: FilmpalastSeriesResult,
    provider: str,
    years_by_title: Dict[str, set[str]],
) -> tuple:
    title_key = _norm_title(strip_source_suffix(result.title))
    if not title_key:
        return ("source", provider, str(result.base_slug or result.sample_slug or result.sample_url))
    year = str(result.year or "").strip()
    known_years = years_by_title.get(title_key, set())
    if not year and len(known_years) == 1:
        year = next(iter(known_years))
    return ("series", title_key, year)


def _claim_series_identity(identity: tuple, claimed: set[tuple]) -> bool:
    """Reserviert eine Identität; True bedeutet, dass sie bereits sichtbar ist."""
    if identity in claimed:
        return True
    if len(identity) != 3 or identity[0] != "series":
        claimed.add(identity)
        return False

    _kind, title_key, year = identity
    unknown = ("series", title_key, "")
    known = {
        item for item in claimed
        if len(item) == 3 and item[0] == "series" and item[1] == title_key and item[2]
    }
    if year and unknown in claimed:
        # Ein früher jahrsloser Treffer wird durch den ersten eindeutigen
        # Jahrgang konkretisiert. Weitere Remakes dürfen danach sichtbar bleiben.
        claimed.remove(unknown)
        claimed.add(identity)
        return True
    if not year and len(known) == 1:
        return True
    claimed.add(identity)
    return False


def _mix_series_provider_results(
    provider_results: Dict[str, List[FilmpalastSeriesResult]],
    priority: List[str],
    claimed_identities: Optional[set[tuple]] = None,
) -> List[_SeriesCatalogEntry]:
    """Dedupliziert Serien und mischt die Leitquelle im Verhältnis 2:1 ein.

    Die erste konfigurierte Quelle erhält zwei Plätze je Runde. So bleibt die
    stärkste Quelle prägend, während jeder weitere Anbieter regelmäßig sichtbar
    wird. Identische Titel werden als eine Serie mit mehreren Quellen geführt.
    """
    years_by_title: Dict[str, set[str]] = defaultdict(set)
    for results in provider_results.values():
        for result in results:
            title_key = _norm_title(strip_source_suffix(result.title))
            year = str(result.year or "").strip()
            if title_key and year:
                years_by_title[title_key].add(year)

    grouped: Dict[tuple, List[tuple[str, FilmpalastSeriesResult]]] = OrderedDict()
    for provider in priority:
        for result in provider_results.get(provider, []):
            identity = _series_result_identity(result, provider, years_by_title)
            grouped.setdefault(identity, []).append((provider, result))

    seen = claimed_identities if claimed_identities is not None else set()
    per_provider: Dict[str, List[_SeriesCatalogEntry]] = {provider: [] for provider in priority}
    for identity, matches in grouped.items():
        if _claim_series_identity(identity, seen):
            continue
        primary_provider, primary_result = matches[0]
        source_set = {provider for provider, _result in matches}
        sources = tuple(provider for provider in priority if provider in source_set)

        # Fehlende Listenmetadaten dürfen von einer alternativen Quelle ergänzt
        # werden, ohne die bevorzugte, klickbare Quelle auszutauschen.
        year = str(primary_result.year or "").strip()
        cover_url = str(primary_result.cover_url or "").strip()
        if not year:
            year = next((str(result.year).strip() for _provider, result in matches if result.year), "")
        if not cover_url:
            cover_url = next(
                (str(result.cover_url).strip() for _provider, result in matches if result.cover_url),
                "",
            )
        visible_result = replace(primary_result, year=year, cover_url=cover_url)
        per_provider[primary_provider].append(_SeriesCatalogEntry(
            provider=primary_provider,
            result=visible_result,
            providers=sources or (primary_provider,),
        ))

    mixed: List[_SeriesCatalogEntry] = []
    positions = {provider: 0 for provider in priority}
    while True:
        progressed = False
        for index, provider in enumerate(priority):
            quota = 2 if index == 0 else 1
            entries = per_provider[provider]
            start = positions[provider]
            end = min(start + quota, len(entries))
            if end > start:
                mixed.extend(entries[start:end])
                positions[provider] = end
                progressed = True
        if not progressed:
            break
    return mixed


def _interleave_series_lists(
    *lists: List[FilmpalastSeriesResult],
) -> List[FilmpalastSeriesResult]:
    """Verzahnt mehrere Signallisten stabil und entfernt Quell-Dubletten."""
    merged: List[FilmpalastSeriesResult] = []
    seen: set[str] = set()
    longest = max((len(items) for items in lists), default=0)
    for index in range(longest):
        for items in lists:
            if index >= len(items):
                continue
            result = items[index]
            key = str(result.base_slug or result.sample_slug or result.sample_url or result.title)
            if key in seen:
                continue
            seen.add(key)
            merged.append(result)
    return merged


def _series_provider_is_paginated(provider: str, mode: str) -> bool:
    if mode == "alpha":
        return provider in SERIES_ALPHA_PROVIDERS
    return provider in SERIES_PAGINATED_PROVIDERS


def _cached_series_provider_page(
    cache_key: tuple,
) -> Optional[List[FilmpalastSeriesResult]]:
    with state.series_list_cache_lock:
        cached = state.series_list_cache.get(cache_key)
        ttl = cached[2] if cached and len(cached) > 2 else SERIES_LIST_CACHE_TTL
        if cached and time.time() - cached[0] < ttl:
            return list(cached[1])
        if cached:
            state.series_list_cache.pop(cache_key, None)
    return None


def _cache_series_provider_page(
    cache_key: tuple,
    results: List[FilmpalastSeriesResult],
    ttl: int = SERIES_LIST_CACHE_TTL,
) -> None:
    now = time.time()
    with state.series_list_cache_lock:
        expired = [
            key for key, cached in state.series_list_cache.items()
            if now - cached[0] >= (
                cached[2] if len(cached) > 2 else SERIES_LIST_CACHE_TTL
            )
        ]
        for key in expired:
            state.series_list_cache.pop(key, None)
        while len(state.series_list_cache) >= SERIES_LIST_CACHE_MAX_ENTRIES:
            oldest = min(
                state.series_list_cache,
                key=lambda key: state.series_list_cache[key][0],
            )
            state.series_list_cache.pop(oldest, None)
        state.series_list_cache[cache_key] = (now, list(results), ttl)


def _fetch_series_provider_page(
    provider: str,
    mode: str,
    letter: str,
    source_page: int,
) -> List[FilmpalastSeriesResult]:
    """Lädt eine Serien-Quellseite passend zum gewünschten Entdeckungsmodus."""
    if not _series_provider_is_paginated(provider, mode) and source_page != 1:
        return []

    if provider == "serienstream":
        with state.sto_lock:
            scraper = get_sto_scraper()
            if mode == "alpha":
                return list(scraper.list_series_alpha(letter, source_page))
            if source_page != 1:
                return []
            if mode == "new":
                return list(scraper.list_new(1))
            if mode == "trending":
                return list(scraper.list_trending(1))
            return _interleave_series_lists(
                list(scraper.list_trending(1)),
                list(scraper.list_new(1)),
            )

    if provider == "filmpalast":
        with state.fp_lock:
            scraper = get_fp_scraper()
            if mode == "alpha":
                return list(scraper.list_series_alpha(letter, source_page))
            return list(scraper.list_series(source_page))

    if mode == "alpha":
        return []
    scraper_classes = {
        "moflix": MoflixScraper,
        "kinoger": KinogerScraper,
        "megakino": MegaKinoScraper,
        "xcine": XcineScraper,
        "sflix": SflixScraper,
        "ridomovies": RidomoviesScraper,
    }
    scraper_class = scraper_classes.get(provider)
    if scraper_class is None:
        return []
    return list(scraper_class(progress_cb=log).list_series(source_page))


def _load_series_provider_pages(
    mode: str,
    letter: str,
    requests_to_load: List[tuple[str, int]],
    cold_wave_budget: Optional[List[int]] = None,
) -> Dict[tuple[str, int], List[FilmpalastSeriesResult]]:
    loaded: Dict[tuple[str, int], List[FilmpalastSeriesResult]] = {}
    missing: List[tuple[str, int, tuple]] = []
    letter_key = str(letter or "").strip().upper()
    for provider, source_page in dict.fromkeys(requests_to_load):
        cache_mode = (
            "updates"
            if provider != "serienstream" and mode in {"discover", "new"}
            else mode
        )
        cache_key = ("series-provider", cache_mode, letter_key, provider, int(source_page))
        cached = _cached_series_provider_page(cache_key)
        if cached is None:
            missing.append((provider, source_page, cache_key))
        else:
            loaded[(provider, source_page)] = cached

    if not missing:
        return loaded
    if cold_wave_budget is not None:
        if cold_wave_budget[0] <= 0:
            raise SeriesCatalogColdLoadLimit(
                "Dieser Serienabschnitt wird noch vorbereitet. Bitte kurz warten und erneut versuchen."
            )
        cold_wave_budget[0] -= 1

    with ThreadPoolExecutor(max_workers=min(len(missing), len(PROVIDER_LABELS))) as pool:
        futures = [
            (
                provider,
                source_page,
                cache_key,
                pool.submit(
                    _fetch_series_provider_page,
                    provider,
                    mode,
                    letter,
                    source_page,
                ),
            )
            for provider, source_page, cache_key in missing
        ]
        for provider, source_page, cache_key, future in futures:
            try:
                results = list(future.result())
            except Exception as exc:
                log(
                    f"{PROVIDER_LABELS.get(provider, provider)} Serienliste "
                    f"(Quellseite {source_page}) übersprungen: {exc}",
                    "warn",
                )
                results = []
                _cache_series_provider_page(
                    cache_key,
                    results,
                    ttl=SERIES_LIST_FAILURE_CACHE_TTL,
                )
            else:
                _cache_series_provider_page(cache_key, results)
            loaded[(provider, source_page)] = results
    return loaded


def _series_catalog_sources(entries: List[_SeriesCatalogEntry], priority: List[str]) -> List[dict]:
    counts = Counter(provider for entry in entries for provider in entry.providers)
    return [
        {
            "key": provider,
            "label": PROVIDER_LABELS[provider],
            "content_language": provider_content_language(provider),
            "language_label": PROVIDER_CATALOG[provider].language_label,
            "count": counts[provider],
        }
        for provider in priority
        if counts[provider]
    ]


def _series_entry_to_dict(entry: _SeriesCatalogEntry) -> dict:
    payload = asdict(entry.result)
    payload["title"] = strip_source_suffix(entry.result.title)
    payload["provider"] = entry.provider
    payload["provider_label"] = PROVIDER_LABELS.get(entry.provider, entry.provider)
    payload["content_language"] = provider_content_language(entry.provider)
    payload["language_label"] = PROVIDER_CATALOG[entry.provider].language_label
    payload["sources"] = [
        {
            "key": provider,
            "label": PROVIDER_LABELS.get(provider, provider),
            "content_language": provider_content_language(provider),
        }
        for provider in entry.providers
    ]
    return payload


def _series_catalog_page_locked(mode: str, page: int = 1, letter: str = "") -> dict:
    """Erzeugt eine stabile, gemischte Serienseite aus den verfügbaren Katalogen."""
    page = max(1, min(int(page), SERIES_MAX_GLOBAL_PAGE))
    mode = mode if mode in {"discover", "new", "trending", "alpha"} else "discover"
    priority = provider_priority("series")
    if mode == "trending":
        # Nur Serienstream liefert ein echtes Popularitätssignal. Andere
        # Aktualitätslisten werden bewusst nicht als „angesagt“ ausgegeben.
        active = [provider for provider in priority if provider == "serienstream"]
    elif mode == "alpha":
        active = [provider for provider in priority if provider in SERIES_ALPHA_PROVIDERS]
    else:
        active = list(priority)

    provider_seen: Dict[str, set[str]] = {provider: set() for provider in priority}

    def unique_page(
        provider: str,
        results: List[FilmpalastSeriesResult],
    ) -> List[FilmpalastSeriesResult]:
        unique: List[FilmpalastSeriesResult] = []
        for result in results:
            source_key = str(
                result.base_slug or result.sample_slug or result.sample_url or result.title
            ).strip()
            key = f"{source_key}\0{str(result.year or '').strip()}"
            if key in provider_seen[provider]:
                continue
            provider_seen[provider].add(key)
            unique.append(result)
        return unique

    cold_wave_budget = [SERIES_MAX_COLD_WAVES_PER_REQUEST]
    first_pages = _load_series_provider_pages(
        mode,
        letter,
        [(provider, 1) for provider in active],
        cold_wave_budget,
    )
    first_wave = {
        provider: unique_page(provider, first_pages.get((provider, 1), []))
        for provider in active
    }
    claimed_identities: set[tuple] = set()
    catalog_entries = _mix_series_provider_results(
        first_wave,
        priority,
        claimed_identities,
    )

    paginated = [
        provider for provider in active if _series_provider_is_paginated(provider, mode)
    ]
    exhausted = {provider for provider in paginated if not first_wave[provider]}
    duplicate_only_pages = {provider: 0 for provider in paginated}
    target_end = page * SERIES_BROWSE_PAGE_SIZE
    next_source_page = 2
    has_more_unverified = False

    while len(catalog_entries) <= target_end and next_source_page <= SERIES_MAX_SOURCE_PAGE:
        pending = [provider for provider in paginated if provider not in exhausted]
        if not pending:
            break
        try:
            next_pages = _load_series_provider_pages(
                mode,
                letter,
                [(provider, next_source_page) for provider in pending],
                cold_wave_budget,
            )
        except SeriesCatalogColdLoadLimit:
            if len(catalog_entries) < target_end:
                raise
            has_more_unverified = True
            break
        wave: Dict[str, List[FilmpalastSeriesResult]] = {}
        for provider in pending:
            results = next_pages.get((provider, next_source_page), [])
            wave[provider] = unique_page(provider, results)
            if not results:
                exhausted.add(provider)
            elif not wave[provider]:
                duplicate_only_pages[provider] += 1
                if duplicate_only_pages[provider] >= 2:
                    exhausted.add(provider)
            else:
                duplicate_only_pages[provider] = 0
        catalog_entries.extend(_mix_series_provider_results(
            wave,
            priority,
            claimed_identities,
        ))
        next_source_page += 1

    start = (page - 1) * SERIES_BROWSE_PAGE_SIZE
    page_entries = catalog_entries[start:target_end]
    return {
        "entries": page_entries,
        "page": page,
        "has_more": page < SERIES_MAX_GLOBAL_PAGE and (
            len(catalog_entries) > target_end or has_more_unverified
        ),
        "sources": _series_catalog_sources(page_entries, priority),
    }


def series_catalog_page(mode: str, page: int = 1, letter: str = "") -> dict:
    """Single-Flight-Wrapper für Warmup und gleichzeitig öffnende Browser."""
    with state.series_catalog_lock:
        return _series_catalog_page_locked(mode, page, letter)


def series_search_catalog(query: str) -> dict:
    """Gruppiert die freie Suche nach Titel und zeigt alternative Quellen an."""
    priority = provider_priority("series")
    entries = _mix_series_provider_results(
        _search_series_provider_results(query),
        priority,
    )
    wanted = _norm_title(query)
    entries.sort(key=lambda entry: (
        _norm_title(entry.result.title) != wanted,
        wanted not in _norm_title(entry.result.title),
        abs(len(_norm_title(entry.result.title)) - len(wanted)),
        strip_source_suffix(entry.result.title).casefold(),
    ))
    return {
        "entries": entries,
        "page": 1,
        "has_more": False,
        "sources": _series_catalog_sources(entries, priority),
    }


def warm_home_series_cache() -> None:
    """Bereitet die gemischte Serien-Startansicht im Hintergrund vor."""
    try:
        catalog = series_catalog_page("discover", 1)
        log(f"Serien-Startansicht vorbereitet: {len(catalog['entries'])} Serien.")
    except Exception as exc:
        log(f"Serien-Startansicht konnte nicht vorab geladen werden: {exc}", "warn")


def _norm_title(title: str) -> str:
    """Titel für Matching normalisieren: Provider-Suffix + Sonderzeichen weg."""
    t = re.sub(r"\s*\[[^\]]+\]\s*$", "", title or "")
    return re.sub(r"[^a-z0-9]+", "", t.casefold())


def _series_search_title(value: str) -> str:
    """Leitet aus einem Serien-Wert (Slug/URL) einen Such-Titel ab – auch aus
    Alt-/Fremdwerten (Moflix/Filmpalast), damit alte Watchlist-Einträge auf
    serienstream.to gematcht werden können."""
    v = value or ""
    is_kinoger = v.startswith(KINOGER_PREFIX) or "kinoger.com" in v.casefold()
    is_megakino = v.startswith(MEGAKINO_PREFIX) or "megakino.org" in v.casefold()
    is_xcine = v.startswith(XCINE_PREFIX) or "xcine.ru" in v.casefold()
    for pfx in (
        SERIENSTREAM_PREFIX, MOFLIX_PREFIX, EINSCHALTEN_PREFIX, KINOX_PREFIX,
        KINOGER_PREFIX, MEGAKINO_PREFIX, XCINE_PREFIX,
        SFLIX_PREFIX, RIDOMOVIES_PREFIX,
    ):
        if v.startswith(pfx):
            v = v[len(pfx):]
            break
    if ":" in v and v.split(":", 1)[0].isdigit():   # moflix "123:the-bear"
        v = v.split(":", 1)[1]
    if is_megakino:
        v = re.sub(r"^[0-9a-f]{24}:", "", v, flags=re.I)
    if v.startswith("http"):
        m = re.search(r"/(?:serie|stream|titles|watch)/(?:stream/|\d+/)?([^/?#]+)", v)
        v = m.group(1) if m else v
    if is_kinoger:
        v = re.sub(r"^\d+-", "", v)
        v = re.sub(r"\.html$", "", v, flags=re.I)
    if is_xcine and ":" in v:
        v = v.split(":", 1)[1]
    parsed = parse_episode_slug(v)
    if parsed:
        v = parsed[0]
    return v.replace("-", " ").strip()


def _episode_placeholder(slug: str, series_title: str = "") -> FilmpalastMovie:
    """Behält eine vorübergehend nicht ladbare Episode als Queue-Job."""
    parsed = parse_episode_slug(slug)
    if not parsed:
        raise ValueError(f"Kein Episoden-Slug: {slug}")
    base_slug, season, episode = parsed
    if not series_title:
        with state.watchlist_lock:
            entry = watchlist_lookup(base_slug)
            if entry:
                series_title = str(entry.get("title") or "")
    if not series_title:
        cached = state.series_cache.get(base_slug)
        if cached:
            series_title = cached.title
    if not series_title:
        series_title = _series_search_title(base_slug).title() or "Unbekannte Serie"
    return FilmpalastMovie(
        title=f"{series_title} S{season:02d}E{episode:02d}",
        url=slug,
        hosters=[],
    )


def _best_title_match(title: str, results: List[FilmpalastSeriesResult]) -> Optional[FilmpalastSeriesResult]:
    want = _norm_title(title)
    if not want or not results:
        return None
    exact = [r for r in results if _norm_title(r.title) == want]
    if exact:
        return exact[0]
    partial = [r for r in results if want in _norm_title(r.title) or _norm_title(r.title) in want]
    return partial[0] if partial else None


def _find_series_by_title(
    value: str, providers: Optional[List[str]] = None,
) -> Optional[FilmpalastSeries]:
    """Sucht und lädt dieselbe Serie nach konfigurierter Anbieterpriorität."""
    title = _series_search_title(value)
    if not title:
        return None
    for provider in providers or provider_priority("series"):
        label = PROVIDER_LABELS[provider]
        log(f"Serie nicht direkt ladbar – suche «{title}» bei {label} …")
        try:
            results = _search_series_for_provider(provider, title)
            best = _best_title_match(title, results)
            series = _load_series_for_provider(provider, best.sample_slug) if best else None
        except Exception as exc:
            log(f"  {label}-Suche/Laden fehlgeschlagen: {exc}", "warn")
            continue
        if series and series.seasons:
            log(f"  Gefunden bei {label} ({len(series.all_episodes)} Episoden).")
            return series
    return None


def _sto_find_by_title(value: str) -> Optional[FilmpalastSeries]:
    """Kompatibilitätshelfer für gezielte Serienstream-Suche."""
    return _find_series_by_title(value, ["serienstream"])


def get_series_for_value(value: str) -> Optional[FilmpalastSeries]:
    """Lädt eine explizite Quelle direkt, danach greifen die Prioritäts-Fallbacks."""
    provider = provider_for_value(value)
    try:
        series = _load_series_for_provider(provider, value)
    except Exception as exc:
        log(f"{PROVIDER_LABELS[provider]} Serien-Laden fehlgeschlagen: {exc}", "warn")
        series = None
    if series and series.seasons:
        return series
    fallbacks = [key for key in provider_priority("series") if key != provider]
    if provider in appconfig.SERIES_PROVIDER_DEFAULTS:
        fallbacks.append(provider)
    return _find_series_by_title(value, fallbacks)


def movie_to_dict(movie: FilmpalastMovie) -> dict:
    ranked = state.hoster_intel.rank(movie.hosters) if movie.hosters else []
    provider = _movie_provider(movie)
    content_language = _movie_content_language(movie)
    payload = {
        "title": movie.title, "url": movie.url, "year": movie.year,
        "runtime": movie.runtime, "cover_url": movie.cover_url,
        "description": movie.description, "genres": movie.genres,
        "provider": provider,
        "provider_label": PROVIDER_LABELS.get(provider, provider),
        "content_language": content_language,
        "language_label": PROVIDER_CATALOG[provider].language_label,
        "hosters": [asdict(h) for h in movie.hosters],
        "hoster_label": state.hoster_intel.best_label(movie.hosters) if movie.hosters else "kein Hoster",
        "hoster_route": state.hoster_intel.route_text(movie.hosters) if movie.hosters else "keine Route",
        "hoster_score": round(state.hoster_intel.score(ranked[0])) if ranked else None,
        "hoster_fallback_count": max(0, len(ranked) - 1) if ranked else 0,
        "metadata_source": "Anbieter",
    }
    tmdb = get_tmdb_client().movie(strip_source_suffix(movie.title), movie.year)
    if tmdb:
        for field in (
            "title", "year", "runtime", "cover_url", "backdrop_url", "description", "genres",
            "original_title", "release_date", "rating", "vote_count", "tagline",
            "certification", "certification_country", "status", "original_language",
            "spoken_languages", "countries", "directors", "writers", "cast",
            "production_companies", "keywords", "collection", "budget", "revenue",
            "trailer", "tmdb_url",
        ):
            if tmdb.get(field):
                payload[field] = tmdb[field]
        payload["metadata_source"] = "TMDB"
        payload["tmdb_id"] = tmdb["tmdb_id"]
    return payload


def _series_folder_key(name: str) -> str:
    """Vergleichsschlüssel für vorhandene Serienordner.

    Linux unterscheidet Groß-/Kleinschreibung. Ohne diese Normalisierung würde
    z.B. neben "The rookie" ein zweiter Ordner "The Rookie" entstehen.
    """
    without_year = re.sub(r"\s*[\(\[]?(?:19|20)\d{2}[\)\]]?\s*$", "", name or "")
    return re.sub(r"[^a-z0-9]+", "", without_year.casefold())


def _existing_series_dir(out_root: Path, desired_name: str) -> Path:
    desired = out_root / desired_name
    if not out_root.is_dir():
        return desired
    wanted = _series_folder_key(desired_name)
    cache_key = (str(out_root.resolve()), wanted)
    cached = state.series_dir_cache.get(cache_key)
    if cached is not None and cached.is_dir():
        return cached
    try:
        matches = [
            child for child in out_root.iterdir()
            if child.is_dir() and _series_folder_key(child.name) == wanted
        ]
    except OSError:
        return desired
    if not matches:
        return desired

    # Gibt es durch eine frühere Groß-/Kleinschreibungs-Abweichung bereits zwei
    # Ordner, gewinnt der etablierte Ordner mit den meisten Mediendateien.
    video_suffixes = {".mp4", ".mkv", ".webm", ".avi", ".mov", ".m4v"}

    def _content_score(path: Path) -> tuple:
        videos = 0
        dirs = 0
        try:
            for child in path.rglob("*"):
                if child.is_dir():
                    dirs += 1
                elif child.suffix.casefold() in video_suffixes:
                    videos += 1
        except OSError:
            pass
        return videos, dirs, path.name == desired_name

    chosen = max(matches, key=_content_score)
    state.series_dir_cache[cache_key] = chosen
    return chosen


def _season_output_dir(series_dir: Path, season: int) -> Path:
    """Übernimmt die vorhandene Staffelstruktur einer Serie.

    Unterstützt "Staffel 8", "Staffel 08", "Season 08" und "S08". Liegen
    vorhandene Episoden flach im Serienordner, bleibt auch die neue Episode dort.
    """
    preferred = series_dir / f"Staffel {season:02d}"
    if preferred.exists() or not series_dir.is_dir():
        return preferred

    season_re = re.compile(r"^(?:staffel|season|s)\s*0*(\d+)\b", re.IGNORECASE)
    season_dirs: List[tuple] = []
    has_flat_episodes = False
    episode_re = re.compile(r"(?:^|[. _-])s\d{1,2}e\d{1,3}(?:$|[. _-])", re.IGNORECASE)
    try:
        for child in series_dir.iterdir():
            if child.is_dir():
                match = season_re.match(child.name.strip())
                if match:
                    season_dirs.append((int(match.group(1)), child))
            elif child.is_file() and episode_re.search(child.stem):
                has_flat_episodes = True
    except OSError:
        return preferred

    for number, folder in season_dirs:
        if number == season:
            return folder
    if has_flat_episodes and not season_dirs:
        return series_dir
    return preferred


def series_episode_out_path(series_title: str, season: int, episode: int) -> Path:
    # Serien landen im SEPARATEN Serien-Ordner (state.series_path), Filme im
    # Film-Ordner (state.save_path). Vorhandene NAS-Strukturen werden bewahrt.
    out_root = Path(state.series_path)
    desired_name = sanitize_filename(series_title).strip() or "Serie"
    series_dir = _existing_series_dir(out_root, desired_name)
    season_dir = _season_output_dir(series_dir, season)
    return season_dir / build_filename(series_title, season, episode)


def _valid_media_cached(path: Path) -> tuple[bool, str]:
    """Validiert lokale Medien nur erneut, wenn Größe oder mtime sich ändern."""
    try:
        stat = path.stat()
        signature = (stat.st_size, stat.st_mtime_ns)
    except OSError as exc:
        return False, f"Datei nicht lesbar: {exc}"
    key = str(path.resolve(strict=False))
    with state.media_validation_lock:
        cached = state.media_validation_cache.get(key)
        if cached and cached[:2] == signature:
            return bool(cached[2]), str(cached[3])
    valid, detail = validate_media_file(path)
    with state.media_validation_lock:
        state.media_validation_cache[key] = (*signature, valid, detail)
    return valid, detail


def compute_downloaded_episodes(series: FilmpalastSeries) -> set:
    """Scannt den Serienordner einmal statt eines NAS-Glob pro Episode."""
    out_root = Path(state.series_path)
    desired_name = sanitize_filename(series.title).strip() or "Serie"
    series_dir = _existing_series_dir(out_root, desired_name)
    if not series_dir.is_dir():
        return set()

    video_suffixes = {".mp4", ".mkv", ".webm", ".avi", ".mov", ".m4v"}
    candidates: List[tuple[Path, tuple[int, int]]] = []
    try:
        for path in series_dir.rglob("*"):
            if not path.is_file() or path.suffix.casefold() not in video_suffixes:
                continue
            match = re.search(r"(?:^|[. _-])s(\d{1,2})e(\d{1,3})(?:$|[. _-])", path.stem, re.I)
            if match:
                candidates.append((path, (int(match.group(1)), int(match.group(2)))))
    except OSError:
        pass

    existing: set[tuple[int, int]] = set()
    if candidates:
        with ThreadPoolExecutor(max_workers=min(4, len(candidates))) as pool:
            futures = [(pair, pool.submit(_valid_media_cached, path)) for path, pair in candidates]
            for pair, future in futures:
                try:
                    valid, _detail = future.result()
                except Exception:
                    valid = False
                if valid:
                    existing.add(pair)

    return {
        ep.slug for ep in series.all_episodes
        if (ep.season, ep.episode) in existing
    }


def series_to_dict(
    series: FilmpalastSeries,
    refresh_jellyfin: bool = False,
    defer_checks: bool = False,
) -> dict:
    """Serialisiert eine Serie, optional ohne blockierende Verfügbarkeitschecks.

    Beim ersten Öffnen werden damit Staffel- und Episodenstruktur sofort nach
    dem Anbieterabruf ausgeliefert. Lokaler Bestand, TMDB und Jellyfin dürfen
    anschließend in einem getrennten Request nachziehen.
    """
    downloaded = set() if defer_checks else compute_downloaded_episodes(series)
    with state.watchlist_lock:
        stored_entry = watchlist_lookup(series.base_slug)
        watchlist_entry = dict(stored_entry) if stored_entry else None
    stored_tmdb_id = watchlist_entry.get("tmdb_id") if watchlist_entry else ""
    tmdb_client = get_tmdb_client()
    tmdb = None if defer_checks else get_tmdb_series(series.title, stored_tmdb_id)
    aliases = list(dict.fromkeys(filter(None, (
        watchlist_entry.get("title", "") if watchlist_entry else "",
        *(watchlist_entry.get("aliases", []) if watchlist_entry else []),
        tmdb.get("title", "") if tmdb else "",
        tmdb.get("original_title", "") if tmdb else "",
    ))))
    tmdb_id = stored_tmdb_id or (tmdb or {}).get("tmdb_id")
    season_episode_counts = (tmdb or {}).get("season_episode_counts") or (
        watchlist_entry.get("season_episode_counts", {}) if watchlist_entry else {}
    )
    season_counts_checked_at = (tmdb or {}).get("season_counts_checked_at") or (
        watchlist_entry.get("season_counts_checked_at", 0) if watchlist_entry else 0
    )
    jf_client = get_jellyfin_client()
    jellyfin_pending = bool(defer_checks and jf_client.configured)
    jf_identity_available: Optional[bool] = None if jellyfin_pending else True
    jf_existing: set[tuple[int, int]] = set()
    if jf_client.configured and not jellyfin_pending:
        with state.jellyfin_cache_lock:
            jf_config_generation = state.jellyfin_config_generation
        jf_episodes = get_jellyfin_episodes(force=refresh_jellyfin)
        jf_series = get_jellyfin_series(force=refresh_jellyfin)
        with state.jellyfin_cache_lock:
            jf_data_generation = state.jellyfin_episode_data_generation
        jf_series_ids = jf_client.series_ids_for(
            series.title, tmdb_id=tmdb_id, aliases=aliases, items=jf_series,
        ) if jf_series is not None else set()
        jf_identity_available = bool(
            jf_episodes is not None
            and state.jellyfin_episodes_available
            and jf_series is not None
            and state.jellyfin_series_available
            and jf_series_ids is not None
        )
        jf_existing = (
            jf_client.episodes_for_series(
                series.title, items=jf_episodes, aliases=aliases, series_ids=jf_series_ids,
            )
            if jf_identity_available and jf_episodes is not None else set()
        )
        with state.jellyfin_cache_lock:
            if (
                jf_config_generation != state.jellyfin_config_generation
                or jf_data_generation != state.jellyfin_episode_data_generation
            ):
                jf_identity_available = False
                jf_existing = set()
    seasons = []
    for s in series.season_numbers:
        episodes = []
        for ep in series.seasons[s]:
            in_jellyfin = (ep.season, ep.episode) in jf_existing
            episodes.append({
                "season": ep.season, "episode": ep.episode, "slug": ep.slug,
                "url": ep.url, "release_name": ep.release_name,
                "queued": ep.slug in state.picked,
                "downloaded": ep.slug in downloaded,
                "in_jellyfin": in_jellyfin,
            })
        seasons.append({"season": s, "episodes": episodes})
    provider = provider_for_value(series.url or series.base_slug)
    payload = {
        "title": series.title, "base_slug": series.base_slug, "url": series.url,
        "cover_url": series.cover_url, "description": series.description,
        "genres": series.genres, "seasons": seasons,
        "provider": provider,
        "provider_label": PROVIDER_LABELS.get(provider, provider),
        "content_language": provider_content_language(provider),
        "language_label": PROVIDER_CATALOG[provider].language_label,
        "episode_count": len(series.all_episodes),
        "watchlisted": watchlist_entry is not None,
        "availability_pending": defer_checks,
        "enrichment_pending": bool(
            defer_checks and (tmdb_client.configured or jf_client.configured)
        ),
        "jellyfin_configured": jf_client.configured,
        "jellyfin_pending": jellyfin_pending,
        "jellyfin_available": jf_identity_available,
        "watch_mode": normalize_watch_mode(
            watchlist_entry.get("download_mode") if watchlist_entry else None
        ),
        "cleanup_mode": normalize_cleanup_mode(
            watchlist_entry.get("cleanup_mode") if watchlist_entry else None
        ),
        "metadata_source": "Anbieter",
    }
    if tmdb:
        for field in ("title", "year", "runtime", "cover_url", "description", "genres"):
            if tmdb.get(field):
                payload[field] = tmdb[field]
        payload["metadata_source"] = "TMDB"
        payload["tmdb_id"] = tmdb["tmdb_id"]
    if tmdb_id:
        payload["tmdb_id"] = tmdb_id
    if aliases:
        payload["aliases"] = aliases
    if season_episode_counts:
        payload["season_episode_counts"] = season_episode_counts
    if season_counts_checked_at:
        payload["season_counts_checked_at"] = season_counts_checked_at
    return payload


def queue_group_name(slug: str) -> str:
    parsed = parse_episode_slug(slug)
    if not parsed:
        return "Filme"
    movie = state.fp_movies.get(slug)
    if movie and movie.title:
        stripped = strip_episode_suffix(movie.title)
        if stripped:
            return stripped
    return parsed[0]


def queue_content_key(slug: str, movie: Optional[FilmpalastMovie] = None) -> str:
    """Provider-unabhängiger Schlüssel gegen doppelte logische Downloads."""
    movie = movie or state.fp_movies.get(slug)
    if movie is None:
        return ""
    parsed = parse_episode_slug(slug)
    if parsed:
        base_slug, season, episode = parsed
        title = strip_episode_suffix(movie.title) or movie.title
        with state.watchlist_lock:
            entry = watchlist_lookup(base_slug)
            tmdb_id = str((entry or {}).get("tmdb_id") or "")
        if not tmdb_id:
            tmdb = get_tmdb_series(title)
            tmdb_id = str((tmdb or {}).get("tmdb_id") or "")
        identity = f"tmdb:{tmdb_id}" if tmdb_id else f"title:{_norm_title(title)}"
        return f"series:{identity}:s{season}:e{episode}"
    title = strip_source_suffix(movie.title)
    tmdb = get_tmdb_client().movie_summary(title, movie.year)
    tmdb_id = str((tmdb or {}).get("tmdb_id") or "")
    identity = f"tmdb:{tmdb_id}" if tmdb_id else f"title:{_norm_title(title)}:{movie.year or ''}"
    return f"movie:{identity}"


def episode_sort_key(slug: str):
    parsed = parse_episode_slug(slug)
    return (parsed[1], parsed[2]) if parsed else (0, 0)


def _persist_queue_state() -> None:
    with state.queue_claim_lock:
        snapshot = set(state.picked)
        # Lock bis nach dem atomaren Replace halten. Sonst kann ein älterer
        # Snapshot einen neueren Abschluss nachträglich überschreiben.
        if not appconfig.save_queue(snapshot):
            log("Queue-Zustand konnte nicht gespeichert werden.", "warn")


def _queue_slug_claimed(slug: str) -> bool:
    with state.queue_claim_lock:
        return slug in state.picked


def build_queue_payload() -> dict:
    with state.queue_claim_lock:
        slugs = sorted(state.picked)
    if not slugs:
        return {"count": 0, "groups": []}
    groups: "OrderedDict[str, List[str]]" = OrderedDict()
    for slug in slugs:
        groups.setdefault(queue_group_name(slug), []).append(slug)
    result_groups = []
    for name, gslugs in groups.items():
        items = []
        for slug in sorted(gslugs, key=episode_sort_key):
            movie = state.fp_movies.get(slug)
            title = movie.title if movie else slug
            label = state.hoster_intel.best_label(movie.hosters) if movie and movie.hosters else "—"
            provider = _movie_provider(movie, slug)
            items.append({
                "slug": slug, "title": title, "hoster_label": label,
                "provider": provider,
                "content_language": _movie_content_language(movie, fallback=slug),
                "done": slug in state.done_slugs,
            })
        result_groups.append({"name": name, "items": items})
    return {"count": len(slugs), "groups": result_groups}


def watchlist_payload() -> dict:
    items = []
    with state.queue_claim_lock, state.watchlist_lock:
        for w in state.watchlist:
            pending = set(state.watchlist_new_slugs.get(w["base_slug"], set()))
            queued_count = len(pending & state.picked)
            failures = w.get("failed_downloads") if isinstance(w.get("failed_downloads"), dict) else {}
            failed_count = len(set(failures) & pending)
            mode = normalize_watch_mode(w.get("download_mode"))
            cleanup_mode = normalize_cleanup_mode(w.get("cleanup_mode"))
            error = str(w.get("last_error") or "")
            if error:
                status = "blocked"
            elif failed_count:
                status = "failed"
            elif queued_count:
                status = "queued"
            elif pending and state.automation.get("auto_download") and not is_within_download_window():
                status = "waiting_window"
            elif pending:
                status = "missing"
            else:
                status = "current"
            items.append({
                **w,
                "download_mode": mode,
                "download_mode_label": WATCH_MODE_LABELS[mode],
                "cleanup_mode": cleanup_mode,
                "cleanup_mode_label": CLEANUP_MODE_LABELS[cleanup_mode],
                "cleanup_mode_ready": (
                    cleanup_mode == CLEANUP_MODE_KEEP
                    or bool(
                        state.jellyfin_cfg.get("url", "").strip()
                        and state.jellyfin_cfg.get("api_key", "").strip()
                        and state.jellyfin_cfg.get("user_id", "").strip()
                        and state.jellyfin_user_episodes_available
                        and not str(w.get("cleanup_last_error") or "")
                    )
                ),
                "download_mode_ready": (
                    mode != WATCH_MODE_NEXT_SEASON
                    or bool(
                        state.jellyfin_cfg.get("url", "").strip()
                        and state.jellyfin_cfg.get("api_key", "").strip()
                        and state.jellyfin_cfg.get("user_id", "").strip()
                        and state.jellyfin_user_episodes_available
                        and not error
                    )
                ),
                "new_count": len(pending),
                "queued_count": queued_count,
                "failed_count": failed_count,
                "status": status,
            })
    return {"watchlist": items}


# ---------------------------------------------------------------------------
# Download-Pipeline (1:1 aus main.py._build_and_start_queue portiert)
# ---------------------------------------------------------------------------
def on_job_progress(pct: float, msg: str, label: str):
    payload = {"type": "progress", "label": label, "msg": msg}
    if pct >= 0:
        payload["pct"] = pct
    broadcast(payload)


def _failure_record(previous, message: str) -> dict:
    attempts = int(previous.get("attempts", 0)) if isinstance(previous, dict) else 0
    attempts += 1
    retry_delay = min(6 * 60 * 60, 5 * 60 * (2 ** min(attempts - 1, 6)))
    return {
        "message": str(message)[:240],
        "attempts": attempts,
        "next_retry": time.time() + retry_delay,
    }


def _watchlist_retry_allowed(slug: str) -> bool:
    with state.watchlist_lock:
        for entry in state.watchlist:
            failure = (entry.get("failed_downloads") or {}).get(slug)
            if isinstance(failure, dict):
                return time.time() >= float(failure.get("next_retry", 0) or 0)
    return True


def on_job_done(ok: bool, msg: str, label: str, out_path: Path, hoster_url: str = "", slug: str = ""):
    # Der Counter-Eintrag ist das einmalige Abschlusstoken. Entfernen/Abbruch
    # kann es vor einem verspäteten Callback konsumieren; dieser wird dann
    # vollständig ignoriert und kann done/total nicht mehr verfälschen.
    with state.queue_claim_lock:
        with state.download_state_lock:
            if slug and slug not in state.counted_queue_slugs:
                return False
            if slug:
                state.gated_retry_jobs.pop(slug, None)
                state.gated_retry_slugs.discard(slug)
                state.gated_retry_pending = bool(state.gated_retry_slugs)
            if ok and slug:
                state.done_slugs.add(slug)
            state.done_jobs += 1
            if slug:
                state.counted_queue_slugs.discard(slug)
                state.picked.discard(slug)
            done_jobs = state.done_jobs
            total_jobs = state.total_jobs
            successful_jobs = len(state.done_slugs)
            failed_jobs = max(0, done_jobs - successful_jobs)
    if hoster_url:
        state.hoster_intel.record_download(hoster_url, ok)
    if ok:
        log(f"Fertig: {label} -> {out_path}")
    else:
        log(f"Fehler {label}: {msg}", "err")
    if slug:
        # `picked` bildet ausschließlich noch offene Warteschlangen-Einträge ab.
        # Erst hier entfernen: Laufzeit-Fallbacks erreichen diese Funktion erst
        # nach Erfolg oder nachdem wirklich alle Anbieter ausgeschöpft sind.
        _persist_queue_state()
        watchlist_changed = False
        with state.watchlist_lock:
            for entry in state.watchlist:
                base_slug = entry.get("base_slug", "")
                pending = state.watchlist_new_slugs.get(base_slug, set())
                failures = entry.get("failed_downloads")
                if not isinstance(failures, dict):
                    failures = {}
                    entry["failed_downloads"] = failures
                if slug not in pending and slug not in failures:
                    continue
                if ok:
                    pending.discard(slug)
                    failures.pop(slug, None)
                    if not pending:
                        state.watchlist_new_slugs.pop(base_slug, None)
                elif msg != "Abgebrochen":
                    failures[slug] = _failure_record(failures.get(slug), msg)
                else:
                    failures.pop(slug, None)
                watchlist_changed = True
            if watchlist_changed:
                appconfig.save_watchlist(state.watchlist)
        if watchlist_changed:
            broadcast({"type": "watchlist_update", **watchlist_payload()})
        with state.telegram_jobs_lock:
            telegram_job = state.telegram_jobs.pop(slug, None)
        if telegram_job:
            if telegram_job.get("kind") == "series":
                _telegram_series_job_result(telegram_job, slug, ok, msg, out_path)
            else:
                threading.Thread(
                    target=_telegram_finish_job,
                    args=(telegram_job, ok, msg, out_path),
                    daemon=True,
                ).start()
        with state.seerr_jobs_lock:
            seerr_jobs = state.seerr_jobs.pop(slug, [])
        for seerr_job in seerr_jobs:
            _seerr_job_result(seerr_job, slug, ok, msg, out_path)
    broadcast({
        "type": "job_done", "ok": ok, "label": label, "slug": slug, "msg": msg,
        "done_jobs": done_jobs, "total_jobs": total_jobs,
        "successful_jobs": successful_jobs, "failed_jobs": failed_jobs,
        "active": state.dl_queue.active_count(), "pending": state.dl_queue.pending_count(),
    })
    return True


def _refresh_jellyfin_after_download_once():
    """Scan anstoßen und den UI-Cache während des Jellyfin-Imports erneuern."""
    if not state.jellyfin_refresh_lock.acquire(blocking=False):
        log("Jellyfin-Aktualisierung läuft bereits.")
        return
    try:
        with state.jellyfin_cache_lock:
            jf_client = get_jellyfin_client()
            generation = state.jellyfin_config_generation
            user_id = state.jellyfin_cfg.get("user_id", "").strip()
        if not jf_client.configured:
            return
        if not jf_client.refresh_library():
            log("Jellyfin-Bibliotheksscan konnte nicht gestartet werden.", "warn")
            return
        log("Jellyfin-Bibliotheksscan gestartet.")
        started = time.monotonic()
        for deadline in (5, 15, 30, 60, 120):
            time.sleep(max(0.0, deadline - (time.monotonic() - started)))
            withdrawn_slugs: set[str] = set()
            with state.jellyfin_cache_lock:
                if generation != state.jellyfin_config_generation:
                    log("Jellyfin-Aktualisierung verworfen: Konfiguration wurde geändert.", "warn")
                    return

            get_jellyfin_library(force=True)
            # Der globale Bestand und der benutzerspezifische Gesehen-Status
            # dürfen sich nicht gegenseitig überschreiben.
            get_jellyfin_episodes(force=True)
            get_jellyfin_series(force=True)
            if user_id:
                get_jellyfin_user_episodes(force=True)
            with state.jellyfin_cache_lock:
                if generation != state.jellyfin_config_generation:
                    log("Jellyfin-Aktualisierung verworfen: Konfiguration wurde geändert.", "warn")
                    return
                global_episodes = state.jellyfin_episodes
                global_series = state.jellyfin_series
                global_available = state.jellyfin_episodes_available
                global_series_available = state.jellyfin_series_available
                user_episodes = state.jellyfin_user_episodes if user_id else None
                user_available = state.jellyfin_user_episodes_available if user_id else False
                data_generation = state.jellyfin_episode_data_generation

            # NAS-Scan/Policy außerhalb des Watchlist-Locks berechnen. Sonst
            # blockieren Bell, Abo-Aktionen und fertige Download-Callbacks.
            with state.watchlist_lock:
                snapshots = []
                for entry in state.watchlist:
                    entry["check_generation"] = int(entry.get("check_generation", 0)) + 1
                    entry["last_error"] = "Prüfung läuft – Auto-Download pausiert"
                    snapshots.append((
                        entry,
                        dict(entry),
                        state.series_cache.get(entry["base_slug"]),
                        entry["check_generation"],
                    ))
            calculated_updates = []
            for entry, snapshot, series, revision in snapshots:
                needs_user = normalize_watch_mode(snapshot.get("download_mode")) == WATCH_MODE_NEXT_SEASON
                if global_episodes is None or not global_available:
                    calculated_updates.append((entry, revision, None, "Jellyfin nicht erreichbar – Auto-Download pausiert"))
                elif global_series is None or not global_series_available:
                    calculated_updates.append((entry, revision, None, "Jellyfin-Serienindex nicht verfügbar"))
                elif needs_user and (not user_id or user_episodes is None or not user_available):
                    calculated_updates.append((entry, revision, None, "Jellyfin-Benutzerstatus nicht verfügbar"))
                elif series is not None:
                    try:
                        calculated = _calculate_watchlist_entry_state(
                            snapshot, series, jf_client, global_episodes, user_episodes,
                            global_series,
                        )
                        calculated_updates.append((entry, revision, calculated, ""))
                    except Exception as exc:
                        calculated_updates.append((entry, revision, None, str(exc)[:240]))
            with state.jellyfin_cache_lock:
                data_is_current = (
                    generation == state.jellyfin_config_generation
                    and data_generation == state.jellyfin_episode_data_generation
                )
                with state.watchlist_lock:
                    if data_is_current:
                        for entry, revision, calculated, error in calculated_updates:
                            if not any(current is entry for current in state.watchlist):
                                continue
                            if int(entry.get("check_generation", 0)) != revision:
                                continue
                            if error:
                                entry["last_checked"] = time.time()
                                entry["last_error"] = error
                            elif calculated is not None:
                                withdrawn_slugs.update(
                                    _apply_watchlist_entry_state(entry, calculated)
                                )
                    appconfig.save_watchlist(state.watchlist)
            if withdrawn_slugs:
                _cancel_withdrawn_watchlist_slugs(
                    withdrawn_slugs,
                    "In Jellyfin vorhanden oder nicht mehr Teil der Abo-Regel",
                )
            broadcast({"type": "jellyfin_update", **watchlist_payload()})
    finally:
        state.jellyfin_refresh_lock.release()


def refresh_jellyfin_after_download():
    """Fasst parallele Scan-Anforderungen zusammen, ohne eine zu verlieren."""
    with state.jellyfin_refresh_request_lock:
        state.jellyfin_refresh_pending = True
        if state.jellyfin_refresh_running:
            log("Jellyfin-Aktualisierung wurde vorgemerkt.")
            return
        state.jellyfin_refresh_running = True
    try:
        while True:
            with state.jellyfin_refresh_request_lock:
                state.jellyfin_refresh_pending = False
            _refresh_jellyfin_after_download_once()
            with state.jellyfin_refresh_request_lock:
                if state.jellyfin_refresh_pending:
                    continue
                state.jellyfin_refresh_running = False
                return
    except Exception:
        with state.jellyfin_refresh_request_lock:
            state.jellyfin_refresh_running = False
        raise


def on_queue_done():
    with state.queue_lifecycle_lock:
        _on_queue_done_locked()


def _reconcile_idle_queue_state_locked() -> int:
    """Beendet verwaiste Zaehltoken und entfernt alte Gate-Sperrmarker."""
    if (
        state.dl_queue.active_count()
        or state.dl_queue.pending_count()
        or state.queue_prepare_lock.locked()
    ):
        return 0

    with state.queue_claim_lock:
        with state.download_state_lock:
            counted = set(state.counted_queue_slugs)
        claimed = set(state.picked)
        queued_retries = set(state.gated_retry_jobs) & counted & claimed
        running_retries = set()
        if state.gated_retry_worker_running:
            running_retries = set(state.gated_retry_slugs) & counted & claimed
        valid_retries = queued_retries | running_retries

        for slug in set(state.gated_retry_jobs) - valid_retries:
            state.gated_retry_jobs.pop(slug, None)
        state.gated_retry_slugs.intersection_update(valid_retries)
        state.gated_retry_pending = bool(valid_retries)
        orphaned = counted - valid_retries
        restart_retry_worker = bool(queued_retries) and not state.gated_retry_worker_running

    if restart_retry_worker:
        _ensure_gated_retry_worker()

    for slug in sorted(orphaned):
        movie = state.fp_movies.get(slug)
        label = movie.title if movie is not None else slug
        on_job_done(
            False,
            "Downloadvorbereitung ohne Abschluss beendet",
            label,
            Path(""),
            slug=slug,
        )
    return len(orphaned)


def _on_queue_done_locked():
    # Ein alter Scheduler kann auslaufen, während bereits ein neuer
    # Vorbereitungsjob eingereiht wurde. Dann gehört dieses Done-Ereignis noch
    # nicht zum tatsächlichen Ende der gemeinsamen Auto-Queue.
    if state.dl_queue.active_count() or state.dl_queue.pending_count():
        return
    _reconcile_idle_queue_state_locked()
    # Zwischen Captcha-Wellen: noch nicht „fertig" melden und Browser-Pools offen
    # lassen (die nächste Welle zieht die verzögerten Episoden gleich nach).
    if state.gated_retry_pending:
        log("Welle abgeschlossen – warte auf serienstream-Cooldown für die nächste …")
        return
    if state.voe_pool is not None:
        log("Schließe Browser-Pool …")
        try:
            state.voe_pool.close()
        except Exception as exc:
            log(f"Browser-Close Fehler: {exc}", "warn")
        finally:
            state.voe_pool = None
    if state.embed_pool is not None:
        log("Schließe Embed-Pool …")
        try:
            state.embed_pool.close()
        except Exception as exc:
            log(f"Embed-Close Fehler: {exc}", "warn")
        finally:
            state.embed_pool = None
    successful_jobs = len(state.done_slugs)
    failed_jobs = max(0, state.done_jobs - successful_jobs)
    log(f"Downloadlauf beendet: {successful_jobs} erfolgreich, {failed_jobs} fehlgeschlagen.")
    if successful_jobs:
        threading.Thread(target=refresh_jellyfin_after_download, daemon=True).start()
    broadcast({
        "type": "queue_done",
        "done_jobs": state.done_jobs,
        "total_jobs": state.total_jobs,
        "successful_jobs": successful_jobs,
        "failed_jobs": failed_jobs,
    })
    _updater_wake_event.set()


state.dl_queue.on_queue_done = on_queue_done


def _pause_downloads_for_update_restart() -> int:
    """Stoppt die physische Queue, ohne ihre persistenten Claims zu verlieren."""
    with state.queue_lifecycle_lock:
        with state.queue_claim_lock:
            preserved = set(state.picked)
            with state.download_state_lock:
                # Abbruch-Callbacks dürfen die gespeicherten Slugs nicht als
                # fachlich abgeschlossen verbuchen.
                state.counted_queue_slugs.clear()
            state.gated_retry_jobs.clear()
            state.gated_retry_slugs.clear()
            state.gated_retry_pending = False
            _persist_queue_state()
        state.dl_queue.cancel_all()

    # Laufende yt-dlp-Prozesse und Browser-Tabs möglichst sauber beenden, bevor
    # execv den Server ersetzt. Nach spätestens 20 Sekunden übernimmt der
    # Prozessneustart; die Queue-Claims sind zu diesem Zeitpunkt bereits sicher.
    deadline = time.monotonic() + 20
    while state.dl_queue.active_count() and time.monotonic() < deadline:
        time.sleep(0.1)

    if state.hoster_extract_lock.acquire(timeout=10):
        try:
            for attr in ("voe_pool", "embed_pool"):
                pool = getattr(state, attr)
                if pool is None:
                    continue
                try:
                    pool.close()
                except Exception as exc:
                    log(f"Browser-Close vor Update fehlgeschlagen: {exc}", "warn")
                finally:
                    setattr(state, attr, None)
        finally:
            state.hoster_extract_lock.release()
    return len(preserved)


def _canonical_hoster_name(provider_name: str, resolved_url: str) -> str:
    """Bestimmt den Extraktor-Zweig (voe/doodstream/…) aus Provider-Label +
    aufgelöster Domain. VOE nutzt rotierende Mirror-Domains, daher zählt hier
    zuerst das Label."""
    p = (provider_name or "").lower()
    dom = urlparse(resolved_url or "").netloc.lower()
    if "voe" in p or "voe" in dom:
        return "voe"
    if "dood" in p or any(k in dom for k in ("dood", "vide0", "d000d", "d0o0d", "dooood", "ds2play")):
        return "doodstream"
    if "vidara" in p or any(key in dom for key in (
        "vidara", "vidmatrix", "vidchamp", "vidachamp", "vidavaca",
        "viewdara", "thebesthost",
    )):
        return "vidara"
    if "vidsonic" in p or "vidsonic" in dom:
        return "vidsonic"
    if "firestream" in p or "firestream" in dom:
        return "firestream"
    if (
        "fsst" in p or "vidhide" in p or "embed4me" in p or "seekplays" in p
        or any(key in dom for key in (
            "fsst", "incvideo", "kinoger.be", "embed4me", "seekplays",
        ))
    ):
        return "kinoger"
    return p


# Automatische Wiederholung für am serienstream-Captcha hängende Episoden.
SERIES_MAX_WAVES = 3            # max. Anzahl Wellen (Sicherheitskappe)
SERIES_WAVE_COOLDOWN = 90      # zusätzl. Pause (s) nach Leeren der Queue, bevor
                               # die nächste Welle das Rate-Fenster erneut testet

def _gated_retry_worker() -> None:
    """Fuehrt alle Gate-Retries seriell nach echter Queue-Ruhe aus."""
    try:
        while True:
            # Der Cooldown beginnt erst, wenn weder Vorbereitungen noch echte
            # Downloads laufen. Neue Queue-Aktivitaet startet ihn erneut.
            while state.dl_queue.active_count() or state.dl_queue.pending_count():
                time.sleep(1)
            with state.queue_claim_lock:
                if not state.gated_retry_jobs:
                    return

            deadline = time.monotonic() + SERIES_WAVE_COOLDOWN
            restart_cooldown = False
            while time.monotonic() < deadline:
                with state.queue_claim_lock:
                    if not state.gated_retry_jobs:
                        return
                if state.dl_queue.active_count() or state.dl_queue.pending_count():
                    restart_cooldown = True
                    break
                time.sleep(min(1, max(0.05, deadline - time.monotonic())))
            if restart_cooldown:
                continue

            with state.queue_claim_lock:
                pending = list(state.gated_retry_jobs.values())
                state.gated_retry_jobs.clear()
            if not pending:
                continue

            if state.sto_scraper is not None:
                state.sto_scraper.reset_gate()
            log(f"🔄 serienstream-Cooldown beendet: {len(pending)} Episode(n) erneut versuchen.")

            for item in pending:
                movie = item["movie"]
                slug = item["slug"]
                with state.queue_claim_lock:
                    claimed = (
                        slug in state.picked
                        and slug in state.counted_queue_slugs
                        and slug in state.gated_retry_slugs
                    )
                if not claimed:
                    continue
                try:
                    run_download_queue(
                        [(movie, slug)],
                        item["out_root"],
                        wave=item["wave"],
                        movie_fallbacks=item["movie_fallbacks"],
                    )
                except Exception as exc:
                    log(f"Gate-Retry fuer «{slug}» fehlgeschlagen: {exc}", "warn")
                    if not _defer_gated_episode(
                        movie,
                        slug,
                        item["out_root"],
                        item["wave"],
                        item["movie_fallbacks"],
                    ):
                        on_job_done(
                            False,
                            f"Gate-Retry fehlgeschlagen: {exc}",
                            movie.title,
                            Path(""),
                            slug=slug,
                        )
    finally:
        with state.queue_claim_lock:
            state.gated_retry_worker_running = False
            restart = bool(state.gated_retry_jobs)
        if restart:
            _ensure_gated_retry_worker()


def _ensure_gated_retry_worker() -> None:
    with state.queue_claim_lock:
        if state.gated_retry_worker_running or not state.gated_retry_jobs:
            return
        state.gated_retry_worker_running = True
    threading.Thread(target=_gated_retry_worker, daemon=True).start()


def _defer_gated_episode(
    movie: FilmpalastMovie,
    slug: str,
    out_root: Path,
    wave: int,
    movie_fallbacks: Optional[Dict[str, List[FilmpalastMovie]]] = None,
) -> bool:
    """Merkt eine Episode fuer die naechste Gate-Welle vor."""
    if wave >= SERIES_MAX_WAVES:
        return False
    with state.queue_claim_lock:
        if slug not in state.picked or slug not in state.counted_queue_slugs:
            return False
        next_wave = wave + 1
        existing = state.gated_retry_jobs.get(slug)
        if existing:
            next_wave = min(next_wave, int(existing.get("wave", next_wave)))
        state.gated_retry_jobs[slug] = {
            "movie": movie,
            "slug": slug,
            "out_root": Path(out_root),
            "wave": next_wave,
            "movie_fallbacks": movie_fallbacks,
        }
        state.gated_retry_slugs.add(slug)
        state.gated_retry_pending = True
    _ensure_gated_retry_worker()
    return True


def _episode_fallback_aliases(movie_slug: str, title: str) -> tuple[str, ...]:
    """Liefert alternative Katalogtitel fuer eine Episode.

    serienstream zeigt haeufig den deutschen Titel, waehrend ein Backup den
    Originaltitel fuehrt. Der Serien-Slug, Watchlist-Aliase und TMDB schliessen
    diese Luecke, ohne unscharfe Episodenmatches zuzulassen.
    """
    values: List[str] = []
    parsed = parse_episode_slug(movie_slug)
    base_slug = parsed[0] if parsed else movie_slug
    slug_title = _series_search_title(base_slug)
    if slug_title:
        values.append(slug_title)

    tmdb_id = ""
    with state.watchlist_lock:
        entry = watchlist_lookup(base_slug)
        if entry:
            tmdb_id = str(entry.get("tmdb_id") or "")
            values.append(str(entry.get("title") or ""))
            values.extend(str(value or "") for value in entry.get("aliases") or [])
    try:
        tmdb = get_tmdb_series(title, tmdb_id)
    except Exception as exc:
        log(f"  TMDB-Aliase fuer Serien-Fallback nicht ladbar: {exc}", "warn")
        tmdb = None
    if tmdb:
        values.extend((
            str(tmdb.get("title") or ""),
            str(tmdb.get("original_title") or ""),
        ))

    seen = {_norm_title(title)}
    aliases: List[str] = []
    for value in values:
        value = " ".join(value.split()).strip()
        key = _norm_title(value)
        if not key or key in seen:
            continue
        seen.add(key)
        aliases.append(value)
    return tuple(aliases)


def _fallback_get_series(provider: str, title: str) -> Optional[FilmpalastSeries]:
    """Sucht die Serie «title» beim Fallback-Anbieter per Titel-Match und lädt sie.
    Ergebnis (auch None) wird pro Download-Lauf gecacht, damit nicht jede Episode
    denselben Anbieter erneut durchsucht."""
    key = f"{provider}:{_norm_title(title)}"
    if key in state.fallback_series_cache:
        return state.fallback_series_cache[key]
    series: Optional[FilmpalastSeries] = None
    matched = False
    try:
        results = _search_series_for_provider(provider, title)
        best = _best_title_match(title, results)
        matched = best is not None
        series = _load_series_for_provider(provider, best.sample_slug) if best else None
    except Exception as exc:
        log(f"  {provider}-Fallback-Suche fehlgeschlagen: {exc}", "warn")
        # Netzwerk-/Cloudflare-Fehler sind kein bestaetigtes "nicht vorhanden".
        # Kein Negativcache, damit eine spaetere Episode/Welle erneut versucht.
        return None
    if series and not series.seasons:
        return None
    if matched and series is None:
        # Treffer vorhanden, Detailseite aber temporaer nicht ladbar.
        return None
    state.fallback_series_cache[key] = series
    return series


# Nur als überschreibbarer Kompatibilitätspunkt für bestehende Integrationen;
# None bedeutet: immer die live konfigurierte Reihenfolge verwenden.
SERIES_FALLBACK_PROVIDERS: Optional[tuple[str, ...]] = None


def find_episode_fallbacks(
    title: str,
    season: int,
    episode: int,
    aliases: tuple[str, ...] = (),
    source_slug: str = "",
) -> List[FilmpalastMovie]:
    """Lädt dieselbe Episode bei allen passenden Fallback-Katalogen.

    Die vollständige Liste wird benötigt, damit auch ein späterer Laufzeitfehler
    des ersten Fallback-Hosters noch zur nächsten Quelle wechseln kann.
    """
    movies: List[FilmpalastMovie] = []
    seen_urls: set[str] = set()
    search_titles: List[str] = []
    seen_titles: set[str] = set()
    for candidate in (title, *aliases):
        candidate = " ".join(str(candidate or "").split()).strip()
        key = _norm_title(candidate)
        if not key or key in seen_titles:
            continue
        seen_titles.add(key)
        search_titles.append(candidate)

    source_provider = provider_for_value(source_slug) if source_slug else ""
    fallback_providers = SERIES_FALLBACK_PROVIDERS or tuple(provider_priority("series"))
    for provider in fallback_providers:
        if provider == source_provider:
            continue
        series = None
        for search_title in search_titles:
            series = _fallback_get_series(provider, search_title)
            if series:
                break
        if not series:
            continue
        ep = next((e for e in series.seasons.get(season, []) if e.episode == episode), None)
        if not ep:
            log(f"  {provider}: S{season:02d}E{episode:02d} nicht im Katalog", "warn")
            continue
        log(f"  → Fallback {provider}: S{season:02d}E{episode:02d} gefunden, lade Hoster …")
        try:
            movie = load_movie_for_slug(ep.slug)
        except Exception as exc:
            log(f"  {provider}-Fallback Laden fehlgeschlagen: {exc}", "warn")
            movie = None
        if movie and movie.hosters and movie.url not in seen_urls:
            seen_urls.add(movie.url)
            movies.append(movie)
            continue
        log(f"  {provider}: keine nutzbaren Hoster für die Episode", "warn")
    return movies


class _HosterResult:
    """Ergebnis eines Hoster-Extraktionsversuchs für genau einen Movie/Episode."""
    __slots__ = (
        "stream_info", "hoster_used", "hoster_url_used", "source_hoster_url",
        "referer", "origin", "gated", "provider", "content_language",
    )

    def __init__(self):
        self.stream_info = None
        self.hoster_used = ""
        self.hoster_url_used = ""
        self.source_hoster_url = ""
        self.referer = "https://filmpalast.to/"
        self.origin = ""
        self.gated = False   # serienstream Captcha-Gate war aktiv
        self.provider = ""
        self.content_language = ""


def _extract_from_movie(
    movie: FilmpalastMovie,
    unsupported_domains: set,
    excluded_hoster_urls: Optional[set] = None,
) -> _HosterResult:
    """Probiert der Reihe nach die Hoster eines Movies (nach hoster_intel-Ranking)
    durch, löst serienstream-Redirects lazy auf und liefert den ersten nutzbaren
    Stream. Funktioniert für alle konfigurierten Katalogquellen, da Nicht-s.to-
    Hoster einfach ihre direkte URL verwenden."""
    res = _HosterResult()
    res.provider = _movie_provider(movie)
    res.content_language = _movie_content_language(movie)
    session = state.fp_scraper.session._curl if state.fp_scraper else None
    excluded_hoster_urls = excluded_hoster_urls or set()

    for hoster in state.hoster_intel.rank(movie.hosters):
        if not hoster.url:
            continue
        if hoster.url in excluded_hoster_urls:
            log(f"  Überspringe {hoster.name}: Download zuvor fehlgeschlagen", "warn")
            continue
        name = hoster.name.lower()
        if hoster.url in unsupported_domains:
            log(f"  Überspringe {hoster.name}: Link nicht unterstützt", "warn")
            continue
        cooldown, _reason = state.hoster_intel.cooldown(
            hoster.url, hoster_name=hoster.name,
        )
        if cooldown:
            minutes = max(1, (cooldown + 59) // 60)
            log(
                f"  Überspringe {hoster.name}: nach Ausfällen noch "
                f"{minutes} Min. pausiert",
                "warn",
            )
            continue
        res.hoster_used = hoster.name
        res.source_hoster_url = hoster.url
        res.content_language = _movie_content_language(
            movie,
            str(getattr(hoster, "language", "") or ""),
        )
        log(f"  Versuche Hoster: {hoster.name}")

        # serienstream.to liefert Hoster als lazy /r?t=-Redirect. Erst JETZT,
        # für genau diesen Versuch, zur echten Embed-URL auflösen. So bleibt
        # die Zahl der s.to-Requests minimal (meist genau 1) und das Captcha
        # wird gar nicht erst provoziert. Fällt ein Hoster durch, wird nur der
        # nächste aufgelöst.
        was_sto = SerienstreamScraper.is_redirect_url(hoster.url)
        play_url = hoster.url
        if was_sto:
            sto = get_sto_scraper()
            # Ist das Captcha-Gate aktiv, sind ALLE Hoster blockiert – nicht
            # weiter hämmern (das vertieft nur den IP-Flag), sofort abbrechen.
            if sto.gated:
                res.gated = True
                break
            play_url = sto.resolve_play_url(hoster.url, referer=movie.url)
            if not play_url:
                if sto.gated:
                    res.gated = True
                    break
                log(f"  {hoster.name}: S.to-Link nicht auflösbar – nächster Hoster", "warn")
                continue
            name = _canonical_hoster_name(hoster.name, play_url)
            if play_url in unsupported_domains:
                log(f"  Überspringe {hoster.name}: Link nicht unterstützt", "warn")
                continue
        name = _canonical_hoster_name(hoster.name, play_url)
        res.hoster_url_used = play_url
        cooldown, _reason = state.hoster_intel.cooldown(
            play_url, hoster_name=hoster.name,
        )
        if cooldown:
            minutes = max(1, (cooldown + 59) // 60)
            log(
                f"  Überspringe {hoster.name}: Zielhost noch "
                f"{minutes} Min. pausiert",
                "warn",
            )
            continue

        if name == "voe":
            if state.voe_pool is None:
                log("Starte Browser-Pool für VOE-Fallback …")
                try:
                    state.voe_pool = VOEBrowserPool(log_cb=log)
                except Exception as exc:
                    log(f"Browser-Pool konnte nicht starten: {exc}", "warn")
                    state.voe_pool = None
                    continue
            check = pre_check_voe(play_url, session=session)
            if check == VOE_NOT_FOUND:
                log("  VOE 404 – nächster Hoster", "warn")
                continue
            try:
                res.stream_info = extract_stream_url(
                    play_url, session=session, log_cb=log, pool=state.voe_pool,
                )
            except Exception as exc:
                log(f"  VOE-Extraktion fehlgeschlagen: {exc}", "warn")
                res.stream_info = None
            parsed = urlparse(play_url)
            res.referer = play_url
            res.origin = f"{parsed.scheme}://{parsed.netloc}" if parsed.netloc else "https://voe.sx"
        elif (
            name.startswith("filmfrei24")
            or provider_for_value(movie.url) == "filmfrei24"
        ):
            # Eigener öffentlicher VOD-HLS-Stream; kein Embed- oder
            # Browser-Extraktor nötig. Der Scraper liefert zuerst den offiziellen
            # Proxy und danach den direkten TV-Endpunkt als Ausweichroute.
            res.stream_info = (play_url, "hls")
            res.referer = movie.url or f"{FILMFREI24_BASE_URL}/"
            res.origin = FILMFREI24_BASE_URL
        elif name in ("moflix", "veev"):
            embed_referer = (
                movie.url if provider_for_value(movie.url) == "megakino"
                else "https://moflix-stream.xyz/"
            )
            try:
                res.stream_info = extract_stream_url(
                    play_url, session=session, log_cb=log, pool=None,
                    referer=embed_referer,
                )
                if res.stream_info is None:
                    if state.embed_pool is None:
                        log("Starte Browser-Pool für Embed-Fallback …")
                        try:
                            state.embed_pool = VOEBrowserPool(log_cb=log, setup_voe=False)
                        except Exception as exc:
                            log(f"Browser-Pool konnte nicht starten: {exc}", "warn")
                            state.embed_pool = None
                            continue
                    res.stream_info = extract_stream_url(
                        play_url, session=session, log_cb=log, pool=state.embed_pool,
                        referer=embed_referer,
                        browser_wait_seconds=12,
                    )
            except Exception as exc:
                log(f"  Embed-Extraktion fehlgeschlagen: {exc}", "warn")
                res.stream_info = None
            parsed = urlparse(play_url)
            res.referer = play_url
            res.origin = f"{parsed.scheme}://{parsed.netloc}" if parsed.netloc else ""
        elif name == "kinoger":
            referer = movie.url or "https://kinoger.com/"
            try:
                res.stream_info = extract_stream_url(
                    play_url, session=session, log_cb=log, pool=None,
                    referer=referer,
                )
                if res.stream_info is None:
                    if state.embed_pool is None:
                        log("Starte Browser-Pool für KinoGer-Mirror …")
                        try:
                            state.embed_pool = VOEBrowserPool(log_cb=log, setup_voe=False)
                        except Exception as exc:
                            log(f"Browser-Pool konnte nicht starten: {exc}", "warn")
                            state.embed_pool = None
                            continue
                    res.stream_info = extract_stream_url(
                        play_url, session=session, log_cb=log, pool=state.embed_pool,
                        referer=referer,
                        browser_wait_seconds=12,
                    )
            except Exception as exc:
                log(f"  KinoGer-Mirror fehlgeschlagen: {exc}", "warn")
                res.stream_info = None
            parsed = urlparse(play_url)
            res.referer = play_url
            res.origin = f"{parsed.scheme}://{parsed.netloc}" if parsed.netloc else ""
        elif name == "doodstream":
            try:
                res.stream_info = extract_doodstream_url(play_url, session=session, log_cb=log)
            except Exception as exc:
                log(f"  Doodstream-Extraktion fehlgeschlagen: {exc}", "warn")
                res.stream_info = None
            parsed = urlparse(play_url)
            res.referer = f"{parsed.scheme}://{parsed.netloc}/"
            res.origin = f"{parsed.scheme}://{parsed.netloc}"
        elif name == "vidara":
            # VIDARA (vidmatrixa.com u.a.) – von yt-dlp nicht unterstützt, eigener
            # Extraktor (POST /api/stream → streaming_url, HLS).
            try:
                res.stream_info = extract_vidara_url(play_url, session=session, log_cb=log)
            except Exception as exc:
                log(f"  VIDARA-Extraktion fehlgeschlagen: {exc}", "warn")
                res.stream_info = None
            parsed = urlparse(play_url)
            res.referer = f"{parsed.scheme}://{parsed.netloc}/"
            res.origin = f"{parsed.scheme}://{parsed.netloc}"
        elif name == "vidsonic":
            # Vidsonic (vidsonic.net) – von yt-dlp nicht unterstützt, eigener
            # Extraktor (hex-kodierte + umgekehrte URL im HTML, HLS).
            try:
                res.stream_info = extract_vidsonic_url(play_url, session=session, log_cb=log)
            except Exception as exc:
                log(f"  Vidsonic-Extraktion fehlgeschlagen: {exc}", "warn")
                res.stream_info = None
            parsed = urlparse(play_url)
            res.referer = f"{parsed.scheme}://{parsed.netloc}/"
            res.origin = f"{parsed.scheme}://{parsed.netloc}"
        elif name == "firestream":
            try:
                res.stream_info = extract_firestream_url(play_url, session=session, log_cb=log)
            except Exception as exc:
                log(f"  FireStream-Extraktion fehlgeschlagen: {exc}", "warn")
                res.stream_info = None
            parsed = urlparse(play_url)
            res.referer = f"{parsed.scheme}://{parsed.netloc}/"
            res.origin = f"{parsed.scheme}://{parsed.netloc}"
        elif provider_for_value(movie.url) == "megakino":
            # MegaKino nimmt regelmaessig neue Player-Domains auf. Erst wird
            # ohne Browser nach direkten HLS-/MP4-Quellen gesucht, danach faengt
            # der gemeinsame Embed-Pool Medienrequests ab. Als letzter Weg darf
            # yt-dlp die unveraenderte Player-URL versuchen.
            referer = movie.url or "https://megakino.org/"
            try:
                res.stream_info = extract_stream_url(
                    play_url, session=session, log_cb=log, pool=None,
                    referer=referer,
                )
                if res.stream_info is None:
                    if state.embed_pool is None:
                        log("Starte Browser-Pool für MegaKino-Hoster …")
                        try:
                            state.embed_pool = VOEBrowserPool(log_cb=log, setup_voe=False)
                        except Exception as exc:
                            log(f"Browser-Pool konnte nicht starten: {exc}", "warn")
                            state.embed_pool = None
                    if state.embed_pool is not None:
                        res.stream_info = extract_stream_url(
                            play_url, session=session, log_cb=log, pool=state.embed_pool,
                            referer=referer,
                            browser_wait_seconds=10,
                        )
            except Exception as exc:
                log(f"  MegaKino-Hoster fehlgeschlagen: {exc}", "warn")
                res.stream_info = None
            if res.stream_info is None:
                res.stream_info = (play_url, "web")
            parsed = urlparse(play_url)
            res.referer = play_url
            res.origin = f"{parsed.scheme}://{parsed.netloc}" if parsed.netloc else ""
        elif provider_for_value(movie.url) == "sflix":
            # Die SFlix-Player (UpCloud/Vidsrc/…) sind generische Embed-Seiten.
            # Direkte Regex-Auflösung bleibt billig; der gemeinsame Browser-Pool
            # fängt als Fallback den signierten HLS-Request des Players ab.
            referer = movie.url or f"{SFLIX_BASE_URL}/"
            try:
                res.stream_info = extract_stream_url(
                    play_url,
                    session=session,
                    log_cb=log,
                    pool=None,
                    referer=referer,
                )
                if res.stream_info is None:
                    if state.embed_pool is None:
                        log("Starte Browser-Pool für SFlix-Hoster …")
                        try:
                            state.embed_pool = VOEBrowserPool(
                                log_cb=log,
                                setup_voe=False,
                            )
                        except Exception as exc:
                            log(
                                f"Browser-Pool konnte nicht starten: {exc}",
                                "warn",
                            )
                            state.embed_pool = None
                    if state.embed_pool is not None:
                        res.stream_info = extract_stream_url(
                            play_url,
                            session=session,
                            log_cb=log,
                            pool=state.embed_pool,
                            referer=referer,
                            browser_wait_seconds=12,
                        )
            except Exception as exc:
                log(f"  SFlix-Hoster fehlgeschlagen: {exc}", "warn")
                res.stream_info = None
            if res.stream_info is None:
                res.stream_info = (play_url, "web")
            res.referer = referer
            res.origin = SFLIX_BASE_URL
        elif provider_for_value(movie.url) == "ridomovies":
            # Closeload/Rapidrame sind generische Embed-Player. Erst die
            # günstige HTML-Auflösung probieren, dann den gemeinsamen
            # Browser-Pool für signierte Medienrequests verwenden.
            referer = movie.url or f"{RIDOMOVIES_BASE_URL}/"
            try:
                res.stream_info = extract_stream_url(
                    play_url,
                    session=session,
                    log_cb=log,
                    pool=None,
                    referer=referer,
                )
                if res.stream_info is None:
                    if state.embed_pool is None:
                        log("Starte Browser-Pool für Ridomovies-Hoster …")
                        try:
                            state.embed_pool = VOEBrowserPool(
                                log_cb=log,
                                setup_voe=False,
                            )
                        except Exception as exc:
                            log(
                                f"Browser-Pool konnte nicht starten: {exc}",
                                "warn",
                            )
                            state.embed_pool = None
                    if state.embed_pool is not None:
                        res.stream_info = extract_stream_url(
                            play_url,
                            session=session,
                            log_cb=log,
                            pool=state.embed_pool,
                            referer=referer,
                            browser_wait_seconds=12,
                        )
            except Exception as exc:
                log(f"  Ridomovies-Hoster fehlgeschlagen: {exc}", "warn")
                res.stream_info = None
            if res.stream_info is None:
                res.stream_info = (play_url, "web")
            res.referer = referer
            res.origin = RIDOMOVIES_BASE_URL
        elif provider_for_value(movie.url) == "mkissa":
            # MKissa liefert direkte Streams und generische Anime-Embeds.
            # Direkte Medien bleiben unangetastet; Embed-Player durchlaufen
            # zunächst die billige Extraktion und danach den Browser-Pool.
            referer = f"{MKISSA_BASE_URL}/"
            parsed = urlparse(play_url)
            if parsed.path.casefold().endswith((".m3u8", ".mp4")):
                res.stream_info = (play_url, "web")
            else:
                try:
                    res.stream_info = extract_stream_url(
                        play_url,
                        session=session,
                        log_cb=log,
                        pool=None,
                        referer=referer,
                    )
                    if res.stream_info is None:
                        if state.embed_pool is None:
                            log("Starte Browser-Pool für MKissa-Hoster …")
                            try:
                                state.embed_pool = VOEBrowserPool(
                                    log_cb=log,
                                    setup_voe=False,
                                )
                            except Exception as exc:
                                log(
                                    f"Browser-Pool konnte nicht starten: {exc}",
                                    "warn",
                                )
                                state.embed_pool = None
                        if state.embed_pool is not None:
                            res.stream_info = extract_stream_url(
                                play_url,
                                session=session,
                                log_cb=log,
                                pool=state.embed_pool,
                                referer=referer,
                                browser_wait_seconds=12,
                            )
                except Exception as exc:
                    log(f"  MKissa-Hoster fehlgeschlagen: {exc}", "warn")
                    res.stream_info = None
                if res.stream_info is None:
                    res.stream_info = (play_url, "web")
            res.referer = referer
            res.origin = "https://mkissa.to"
        else:
            # Generischer Hoster (Streamtape/Vidoza/Vidmoly/Filemoon/…):
            # yt-dlp probieren lassen. Referer = eigene Hoster-Domain
            # (bei s.to-Auflösung), sonst filmpalast wie gehabt.
            res.stream_info = (play_url, "web")
            if was_sto:
                parsed = urlparse(play_url)
                res.referer = f"{parsed.scheme}://{parsed.netloc}/" if parsed.netloc else "https://filmpalast.to/"
            else:
                res.referer = "https://filmpalast.to/"
            res.origin = ""

        if res.stream_info:
            stream_url, _stream_type = res.stream_info
            log(f"  Prüfe Hoster: {hoster.name}")
            ok, probe_msg = probe_stream_url(stream_url, referer=res.referer, origin=res.origin)
            state.hoster_intel.record_probe(
                play_url, ok, probe_msg, hoster_name=hoster.name,
            )
            if not ok:
                log(f"  {hoster.name} nicht nutzbar: {probe_msg}", "warn")
                if "unsupported url" in probe_msg.lower():
                    unsupported_domains.add(play_url)
                res.stream_info = None
                continue
            break

    return res


def find_movie_source_fallbacks(
    movie: FilmpalastMovie,
    selected_slug: str,
    excluded_urls: set,
) -> List[FilmpalastMovie]:
    """Sucht denselben Film erst dann bei anderen Katalogquellen, wenn alle
    Hoster des ausgewählten Treffers zur Laufzeit gescheitert sind."""
    title = strip_source_suffix(movie.title)
    wanted = _norm_title(title)
    wanted_year = str(movie.year or "")
    if not wanted:
        return []
    log(f"  Suche alternative Filmquellen für «{title}» …", "warn")
    alternatives: List[FilmpalastMovie] = []
    seen_urls = set(excluded_urls)
    try:
        candidates = search_movie_candidates(title)
    except Exception as exc:
        log(f"  Alternative Filmquellen nicht durchsuchbar: {exc}", "warn")
        return []

    for candidate in candidates:
        if not candidate.is_movie or candidate.slug == selected_slug:
            continue
        if _norm_title(candidate.title) != wanted:
            continue
        candidate_year = str(candidate.year or "")
        if wanted_year and candidate_year and candidate_year != wanted_year:
            continue
        if candidate.url in seen_urls:
            continue
        try:
            loaded = state.fp_movies.get(candidate.slug) or load_movie_for_slug(candidate.slug)
        except Exception as exc:
            log(f"  Filmquelle {candidate.title} nicht ladbar: {exc}", "warn")
            continue
        if not loaded or not loaded.hosters or _norm_title(loaded.title) != wanted:
            continue
        loaded_year = str(loaded.year or candidate_year or "")
        if wanted_year and loaded_year and loaded_year != wanted_year:
            continue
        if loaded.url in seen_urls:
            continue
        state.fp_movies[candidate.slug] = loaded
        seen_urls.add(loaded.url)
        alternatives.append(loaded)
        if len(alternatives) >= 6:
            break

    if alternatives:
        log(f"  {len(alternatives)} alternative Filmquelle(n) vorbereitet.")
    else:
        log("  Keine weitere Filmquelle mit exakt passendem Titel/Jahr gefunden.", "warn")
    return alternatives


def _enqueue_hoster_attempt(
    movie: FilmpalastMovie,
    movie_slug: str,
    out_path: Path,
    result: _HosterResult,
    unsupported_domains: set,
    failed_hoster_urls: set,
    attempt_errors: List[str],
    source_movies: List[FilmpalastMovie],
    source_index: int,
    source_fallbacks_loaded: List[bool],
    refreshed_hoster_urls: set,
    cancelled: Optional[Callable[[], bool]] = None,
    gate_seen: Optional[List[bool]] = None,
    gate_retry: Optional[Callable[[], bool]] = None,
    slow_candidates: Optional[List[tuple]] = None,
    last_resort: bool = False,
):
    """Startet einen Downloadversuch und schaltet bei Laufzeitfehlern auf den
    nächsten Hoster um. Ein logischer Job wird erst nach Erfolg oder nach dem
    letzten Anbieter als abgeschlossen gemeldet."""
    if (cancelled and cancelled()) or not _queue_slug_claimed(movie_slug):
        return False
    gate_seen = gate_seen or [bool(result.gated)]
    gate_seen[0] = gate_seen[0] or bool(result.gated)
    if slow_candidates is None:
        slow_candidates = []
    stream_url, stream_type = result.stream_info
    hoster_used = result.hoster_used
    label = f"{movie.title}  ({hoster_used})"
    log(f"  Stream bereit ({hoster_used}): {stream_url[:60]}…")

    def _attempt_done(ok: bool, msg: str):
        if result.hoster_url_used:
            state.hoster_intel.record_download(
                result.hoster_url_used,
                ok,
                hoster_name=hoster_used,
                speed_bps=getattr(job, "average_speed_bps", 0),
                failure_kind=getattr(job, "failure_kind", ""),
            )
        if ok:
            on_job_done(True, msg, label, out_path, slug=movie_slug)
            return
        if msg == "Abgebrochen":
            on_job_done(False, msg, label, out_path, slug=movie_slug)
            return
        if (cancelled and cancelled()) or not _queue_slug_claimed(movie_slug):
            on_job_done(False, "Abgebrochen", label, out_path, slug=movie_slug)
            return

        is_slow = getattr(job, "failure_kind", "") == "slow"
        if last_resort:
            final_msg = "; ".join(attempt_errors + [f"Letzte langsame Reserve: {msg}"])
            on_job_done(False, final_msg, label, out_path, slug=movie_slug)
            return
        if is_slow:
            source_key = result.source_hoster_url or result.hoster_url_used
            if not any(
                (candidate_result.source_hoster_url or candidate_result.hoster_url_used) == source_key
                for _candidate_movie, candidate_result, _speed in slow_candidates
            ):
                slow_candidates.append((
                    movie,
                    result,
                    float(getattr(job, "average_speed_bps", 0) or 0),
                ))

        # Signierte CDN-Links können zwischen Probe und Download ablaufen. Den
        # gleichen Hoster genau einmal frisch extrahieren, bevor er ausscheidet.
        source_url = result.source_hoster_url
        if not is_slow and source_url and source_url not in refreshed_hoster_urls:
            refreshed_hoster_urls.add(source_url)
            log(f"  {hoster_used}: Link wird einmal frisch aufgelöst …", "warn")
            with state.hoster_extract_lock:
                refreshed = _extract_from_movie(
                    movie,
                    unsupported_domains,
                    excluded_hoster_urls=failed_hoster_urls,
                )
            gate_seen[0] = gate_seen[0] or bool(refreshed.gated)
            if refreshed.stream_info:
                if _enqueue_hoster_attempt(
                    movie, movie_slug, out_path, refreshed, unsupported_domains,
                    failed_hoster_urls, attempt_errors, source_movies, source_index,
                    source_fallbacks_loaded, refreshed_hoster_urls, cancelled,
                    gate_seen, gate_retry, slow_candidates, last_resort,
                ):
                    return
                on_job_done(False, "Abgebrochen", label, out_path, slug=movie_slug)
                return

        attempt_errors.append(f"{hoster_used}: {msg}")
        if result.source_hoster_url:
            failed_hoster_urls.add(result.source_hoster_url)
        log(f"  {hoster_used}-Download fehlgeschlagen – versuche nächsten Anbieter", "warn")
        on_job_progress(-1, f"{hoster_used} ausgefallen · wechsle Anbieter …", label)

        with state.hoster_extract_lock:
            next_result = _extract_from_movie(
                movie,
                unsupported_domains,
                excluded_hoster_urls=failed_hoster_urls,
            )
        gate_seen[0] = gate_seen[0] or bool(next_result.gated)
        if next_result.stream_info:
            if _enqueue_hoster_attempt(
                movie, movie_slug, out_path, next_result, unsupported_domains,
                failed_hoster_urls, attempt_errors, source_movies, source_index,
                source_fallbacks_loaded, refreshed_hoster_urls, cancelled,
                gate_seen, gate_retry, slow_candidates, last_resort,
            ):
                return
            on_job_done(False, "Abgebrochen", label, out_path, slug=movie_slug)
            return

        # Alle Hoster dieses Katalogtreffers sind verbraucht. Nun denselben Inhalt
        # bei weiteren Katalogquellen testen – für Filme UND Episoden.
        ep_info = parse_episode_slug(movie_slug)
        if not source_fallbacks_loaded[0]:
            source_fallbacks_loaded[0] = True
            on_job_progress(-1, "Hoster erschöpft · suche alternative Quellen …", label)
            if ep_info:
                series_title = strip_episode_suffix(source_movies[0].title) or source_movies[0].title
                alternatives = find_episode_fallbacks(
                    series_title,
                    ep_info[1],
                    ep_info[2],
                    aliases=_episode_fallback_aliases(movie_slug, series_title),
                    source_slug=movie_slug,
                )
                seen = {m.url for m in source_movies}
                source_movies.extend(m for m in alternatives if m.url not in seen)
            else:
                source_movies.extend(find_movie_source_fallbacks(
                    source_movies[0], movie_slug, {m.url for m in source_movies},
                ))
        for next_index in range(source_index + 1, len(source_movies)):
            next_movie = source_movies[next_index]
            log(f"  Wechsle Filmquelle: {strip_source_suffix(next_movie.title)}", "warn")
            with state.hoster_extract_lock:
                source_result = _extract_from_movie(
                    next_movie,
                    unsupported_domains,
                    excluded_hoster_urls=failed_hoster_urls,
                )
            gate_seen[0] = gate_seen[0] or bool(source_result.gated)
            if not source_result.stream_info:
                continue
            if _enqueue_hoster_attempt(
                next_movie, movie_slug, out_path, source_result, unsupported_domains,
                failed_hoster_urls, attempt_errors, source_movies, next_index,
                source_fallbacks_loaded, refreshed_hoster_urls, cancelled,
                gate_seen, gate_retry, slow_candidates, last_resort,
            ):
                return
            on_job_done(False, "Abgebrochen", label, out_path, slug=movie_slug)
            return

        if ep_info and gate_seen[0] and gate_retry and gate_retry():
            log("  serienstream-Captcha aktiv – Episode nach Cooldown erneut versuchen.", "warn")
            on_job_progress(-1, "Captcha-Cooldown · Wiederholung vorgemerkt …", label)
            return

        if slow_candidates:
            reserve_movie, reserve_result, _reserve_speed = max(
                slow_candidates,
                key=lambda candidate: candidate[2],
            )
            reserve_label = reserve_result.hoster_used or "langsame Quelle"
            log(
                f"  Alle schnelleren Quellen erschoepft – {reserve_label} "
                "wird als langsame Reserve ohne Speed-Limit fortgesetzt.",
                "warn",
            )
            on_job_progress(
                -1,
                f"Keine schnellere Quelle · nutze {reserve_label} als Reserve …",
                label,
            )
            if _enqueue_hoster_attempt(
                reserve_movie,
                movie_slug,
                out_path,
                reserve_result,
                unsupported_domains,
                failed_hoster_urls,
                attempt_errors,
                source_movies,
                source_index,
                source_fallbacks_loaded,
                refreshed_hoster_urls,
                cancelled,
                gate_seen,
                gate_retry,
                [],
                True,
            ):
                return
            on_job_done(False, "Abgebrochen", label, out_path, slug=movie_slug)
            return

        reason = "serienstream-Captcha aktiv" if gate_seen[0] else "alle Anbieter und Filmquellen ausgeschöpft"
        final_msg = "; ".join(attempt_errors + [reason])
        on_job_done(False, final_msg, label, out_path, slug=movie_slug)

    job = DownloadJob(
        stream_url=stream_url,
        stream_type=stream_type,
        out_path=out_path,
        queue_slug=movie_slug,
        provider=result.provider or _movie_provider(movie, movie_slug),
        content_language=(
            result.content_language
            or _movie_content_language(movie, fallback=movie_slug)
        ),
        referer=result.referer,
        origin=result.origin,
        on_progress=lambda pct, msg: on_job_progress(pct, msg, label),
        on_done=_attempt_done,
        allow_slow=last_resort,
    )
    with state.queue_lifecycle_lock:
        with state.queue_claim_lock:
            if (cancelled and cancelled()) or movie_slug not in state.picked:
                return False
            add_front = getattr(state.dl_queue, "add_front", None)
            # Langsame Reserven ohne Speed-Limit koennen stundenlang kriechen.
            # Sie kommen ans Queue-Ende, damit schnelle Downloads nicht hinter
            # ihnen verhungern; normale Folgeversuche behalten ihren Slot vorn.
            if add_front and not last_resort:
                add_front(job)
            else:
                state.dl_queue.add(job)
    return True


def _existing_valid_movie_path(out_root: Path, movie: FilmpalastMovie) -> Optional[Path]:
    """Findet eine bereits vollständig geladene Filmdatei dieses Titels."""
    titles = [strip_source_suffix(movie.title)]
    if movie.title not in titles:
        titles.append(movie.title)
    checked: set = set()
    video_suffixes = {".mp4", ".mkv", ".webm", ".avi", ".mov", ".m4v"}
    for title in titles:
        expected = out_root / build_movie_filename(title, movie.year)
        try:
            candidates = expected.parent.glob(expected.stem + ".*")
            for candidate in candidates:
                if candidate in checked or candidate.suffix.casefold() not in video_suffixes:
                    continue
                checked.add(candidate)
                valid, detail = validate_media_file(candidate)
                if valid:
                    log(f"  Bereits vollständig vorhanden: {candidate.name} ({detail})")
                    return candidate
                log(f"  Vorhandene Datei ist ungültig und wird ersetzt: {candidate.name} ({detail})", "warn")
        except OSError as exc:
            log(f"  Vorhandene Filmdatei konnte nicht geprüft werden: {exc}", "warn")
    return None


def _existing_valid_episode_path(series_title: str, season: int, episode: int) -> Optional[Path]:
    expected = series_episode_out_path(series_title, season, episode)
    if not expected.parent.exists():
        return None
    video_suffixes = {".mp4", ".mkv", ".webm", ".avi", ".mov", ".m4v"}
    for candidate in expected.parent.glob(expected.stem + ".*"):
        if candidate.suffix.casefold() not in video_suffixes:
            continue
        valid, detail = _valid_media_cached(candidate)
        if valid:
            return candidate
        log(f"  Vorhandene Episode ist ungültig und wird ersetzt: {candidate.name} ({detail})", "warn")
    return None


def _episode_jellyfin_identity(
    base_slug: str,
    series_title: str,
    jf_client: JellyfinClient,
    jf_series: Optional[List[dict]],
) -> tuple[tuple[str, ...], set[str], str]:
    """Ermittelt eine eindeutige Serienidentität; Mehrdeutigkeit blockiert."""
    with state.watchlist_lock:
        stored = watchlist_lookup(base_slug)
        entry = dict(stored) if stored else {}
    tmdb_id = str(entry.get("tmdb_id") or "")
    aliases = list(dict.fromkeys(filter(None, (
        series_title,
        entry.get("title", ""),
        *(entry.get("aliases") or []),
    ))))
    tmdb = get_tmdb_series(series_title, tmdb_id)
    if tmdb:
        tmdb_id = str(tmdb_id or tmdb.get("tmdb_id") or "")
        aliases = list(dict.fromkeys(filter(None, (
            *aliases,
            tmdb.get("title", ""),
            tmdb.get("original_title", ""),
        ))))
    series_ids = jf_client.series_ids_for(
        series_title, tmdb_id=tmdb_id, aliases=aliases, items=jf_series,
    )
    if series_ids is None:
        raise RuntimeError("Jellyfin-Zuordnung mehrdeutig")
    return tuple(aliases), series_ids, tmdb_id


def _is_jellyfin_safety_block(reason: str) -> bool:
    return str(reason or "").startswith("Jellyfin")


def _content_already_available(movie: FilmpalastMovie, slug: str) -> tuple[bool, str]:
    """Serverseitiger Duplikatschutz für manuelle und automatische Queue-Adds."""
    episode_info = parse_episode_slug(slug)
    jf_client = get_jellyfin_client()
    if episode_info:
        series_title = strip_episode_suffix(movie.title) or movie.title
        if _existing_valid_episode_path(series_title, episode_info[1], episode_info[2]):
            return True, "lokal vorhanden"
        if jf_client.configured:
            items = get_jellyfin_episodes()
            jf_series = get_jellyfin_series()
            with state.jellyfin_cache_lock:
                config_generation = state.jellyfin_config_generation
                data_generation = state.jellyfin_episode_data_generation
                episodes_available = state.jellyfin_episodes_available
                series_available = state.jellyfin_series_available
            if items is None or not episodes_available:
                return True, "Jellyfin nicht erreichbar"
            if jf_series is None or not series_available:
                return True, "Jellyfin-Serienindex nicht verfügbar"
            try:
                aliases, series_ids, _tmdb_id = _episode_jellyfin_identity(
                    episode_info[0], series_title, jf_client, jf_series,
                )
            except RuntimeError as exc:
                return True, str(exc)
            with state.jellyfin_cache_lock:
                if (
                    config_generation != state.jellyfin_config_generation
                    or data_generation != state.jellyfin_episode_data_generation
                ):
                    return True, "Jellyfin-Daten werden gerade aktualisiert"
            if jf_client.has_episode(
                series_title, episode_info[1], episode_info[2], items=items,
                aliases=aliases, series_ids=series_ids,
            ):
                return True, "in Jellyfin vorhanden"
        return False, ""

    if _existing_valid_movie_path(Path(state.save_path), movie) is not None:
        return True, "lokal vorhanden"
    if jf_client.configured:
        items = get_jellyfin_library()
        with state.jellyfin_cache_lock:
            config_generation = state.jellyfin_config_generation
            data_generation = state.jellyfin_movie_data_generation
            library_available = state.jellyfin_library_available
        if items is None or not library_available:
            return True, "Jellyfin nicht erreichbar"
        title = strip_source_suffix(movie.title)
        tmdb = get_tmdb_client().movie_summary(title, movie.year)
        with state.jellyfin_cache_lock:
            if (
                config_generation != state.jellyfin_config_generation
                or data_generation != state.jellyfin_movie_data_generation
            ):
                return True, "Jellyfin-Daten werden gerade aktualisiert"
        if jf_client.match(
            title, movie.year, items=items, tmdb_id=(tmdb or {}).get("tmdb_id", ""),
        ):
            return True, "in Jellyfin vorhanden"
    return False, ""


def run_download_queue(
    jobs: List[tuple],
    out_root: Path,
    wave: int = 1,
    movie_fallbacks: Optional[Dict[str, List[FilmpalastMovie]]] = None,
    start_queue: bool = True,
    cancelled: Optional[Callable[[], bool]] = None,
):
    """jobs: Liste von (movie, slug)-Paaren. Der slug ist der Queue-Schlüssel
    (z.B. 'serienstream:the-last-of-us-s01e01') – daraus wird die Serie/Staffel/
    Episode erkannt. Wichtig: NICHT aus movie.url ableiten, denn bei s.to/moflix
    ist das letzte URL-Segment 'episode-1'/'1' und würde die Serie fälschlich als
    Film in den Wurzelordner legen.

    `wave` zählt die automatischen Wiederholungswellen für Episoden, die am
    serienstream-Captcha-Gate hingen (siehe Ende der Funktion)."""
    out_root.mkdir(parents=True, exist_ok=True)
    unsupported_domains: set = set()
    gated_jobs: List[tuple] = []   # (movie, slug) die am Captcha-Gate hingen
    queued_slugs: set = set()

    for movie, movie_slug in jobs:
        if (cancelled and cancelled()) or not _queue_slug_claimed(movie_slug):
            continue
        log(f"─── {movie.title} ───")

        # Bereits vorhandene Episode NICHT erneut auflösen/laden. Spart /r?t=-
        # Requests (wichtig fürs Gate) und macht das erneute Anstoßen nach einem
        # Captcha-Cooldown praktikabel: nur die noch fehlenden Folgen werden
        # verarbeitet statt der ganzen Staffel.
        ep_info = parse_episode_slug(movie_slug)
        if ep_info:
            series_title = strip_episode_suffix(movie.title) or movie.title
            existing_file = _existing_valid_episode_path(series_title, ep_info[1], ep_info[2])
            if existing_file is not None:
                if not (cancelled and cancelled()) and _queue_slug_claimed(movie_slug):
                    on_job_done(True, "bereits vorhanden", movie.title, existing_file, slug=movie_slug)
                continue

            # Konnte bereits die Episodenseite während der Vorbereitung nicht
            # geladen werden, bleibt der logische Job trotzdem erhalten. Vor
            # jedem Versuch die gewählte Quelle erneut laden; danach folgen die
            # Katalog-Fallbacks und bei Serienstream gegebenenfalls Cooldowns.
            primary_unavailable = False
            if not movie.hosters:
                try:
                    refreshed_movie = load_movie_for_slug(movie_slug)
                except Exception as exc:
                    log(f"  Episodenseite noch nicht ladbar: {exc}", "warn")
                    refreshed_movie = None
                if refreshed_movie and refreshed_movie.hosters:
                    movie = refreshed_movie
                    state.fp_movies[movie_slug] = refreshed_movie
                elif movie_slug.startswith(SERIENSTREAM_PREFIX):
                    primary_unavailable = True
        else:
            primary_unavailable = False
            existing_movie = _existing_valid_movie_path(out_root, movie)
            if existing_movie is not None:
                if not (cancelled and cancelled()) and _queue_slug_claimed(movie_slug):
                    on_job_done(True, "bereits vorhanden", movie.title, existing_movie, slug=movie_slug)
                continue

        # Originalen Serientitel VOR einem etwaigen Fallback festhalten, damit die
        # Episode – egal ob von s.to oder vom Fallback-Anbieter – immer im selben
        # Serien-/Staffel-Ordner landet (der Fallback-Movie hätte sonst einen leicht
        # abweichenden Titel und damit einen anderen Ordner).
        orig_series_title = strip_episode_suffix(movie.title) or movie.title
        source_movies = [movie]
        seen_source_urls = {movie.url}
        known_fallbacks = (movie_fallbacks or {}).get(movie_slug, [])
        for fallback_movie in known_fallbacks:
            if fallback_movie.url in seen_source_urls:
                continue
            source_movies.append(fallback_movie)
            seen_source_urls.add(fallback_movie.url)
        source_fallbacks_loaded = [movie_fallbacks is not None and movie_slug in movie_fallbacks]
        # Watchlist-Einträge behalten ihren ursprünglichen Katalog-Slug. Wurde
        # später eine andere Primärquelle konfiguriert, laden wir deren Treffer
        # vorab und sortieren die tatsächlich nutzbaren Quellen neu.
        if (
            ep_info
            and provider_for_value(movie_slug) != provider_priority("series")[0]
            and not source_fallbacks_loaded[0]
        ):
            source_fallbacks_loaded[0] = True
            alternatives = find_episode_fallbacks(
                orig_series_title,
                ep_info[1],
                ep_info[2],
                aliases=_episode_fallback_aliases(movie_slug, orig_series_title),
                source_slug=movie_slug,
            )
            for candidate in alternatives:
                if candidate.url not in seen_source_urls:
                    source_movies.append(candidate)
                    seen_source_urls.add(candidate.url)
        if ep_info:
            source_movies = _ordered_episode_sources(source_movies)
            movie = source_movies[0]
        source_index = 0

        with state.hoster_extract_lock:
            result = _extract_from_movie(movie, unsupported_domains)
        if primary_unavailable:
            # Eine temporaer nicht lesbare s.to-Episodenseite wird wie das
            # Redirect-Gate behandelt und nicht sofort terminal gezaehlt.
            result.gated = True
        gate_seen = [bool(result.gated)]

        # Scheitert bereits die Extraktion/Probe, denselben Inhalt sofort bei
        # allen Katalog-Fallbacks versuchen. Das gilt nicht nur bei Captcha.
        if not result.stream_info:
            if not source_fallbacks_loaded[0]:
                source_fallbacks_loaded[0] = True
                if ep_info:
                    alternatives = find_episode_fallbacks(
                        orig_series_title,
                        ep_info[1],
                        ep_info[2],
                        aliases=_episode_fallback_aliases(movie_slug, orig_series_title),
                        source_slug=movie_slug,
                    )
                    source_movies.extend(
                        candidate for candidate in alternatives
                        if candidate.url not in {m.url for m in source_movies}
                    )
                else:
                    source_movies.extend(find_movie_source_fallbacks(
                        source_movies[0], movie_slug, {m.url for m in source_movies},
                    ))
            for next_index in range(1, len(source_movies)):
                next_movie = source_movies[next_index]
                log(f"  Wechsle Quelle: {strip_source_suffix(next_movie.title)}", "warn")
                with state.hoster_extract_lock:
                    source_result = _extract_from_movie(next_movie, unsupported_domains)
                gate_seen[0] = gate_seen[0] or bool(source_result.gated)
                if not source_result.stream_info:
                    continue
                movie = next_movie
                result = source_result
                source_index = next_index
                break

        if not result.stream_info:
            if gate_seen[0]:
                # s.to-Gate aktiv UND kein Fallback nutzbar – für die spätere
                # Welle zurückstellen (NICHT als erledigt zählen).
                gated_jobs.append((source_movies[0], movie_slug))
                log("  Zurückgestellt – serienstream Captcha-Gate aktiv (Fallback erfolglos)", "warn")
            else:
                if not (cancelled and cancelled()) and _queue_slug_claimed(movie_slug):
                    on_job_done(False, "kein Hoster extrahierbar", movie.title, Path(""), slug=movie_slug)
            continue

        # Episode vs. Film aus dem Queue-Slug erkennen (NICHT aus movie.url –
        # s.to/moflix haben dort 'episode-1'/'1' als letztes Segment).
        episode_info = parse_episode_slug(movie_slug)
        if episode_info:
            _base_slug, season, episode = episode_info
            out_path = series_episode_out_path(orig_series_title, season, episode)
        else:
            primary_movie = source_movies[0]
            out_path = out_root / build_movie_filename(strip_source_suffix(primary_movie.title), primary_movie.year)

        enqueued = _enqueue_hoster_attempt(
            movie=movie,
            movie_slug=movie_slug,
            out_path=out_path,
            result=result,
            unsupported_domains=unsupported_domains,
            failed_hoster_urls=set(),
            attempt_errors=[],
            source_movies=source_movies,
            source_index=source_index,
            source_fallbacks_loaded=source_fallbacks_loaded,
            refreshed_hoster_urls=set(),
            cancelled=cancelled,
            gate_seen=gate_seen,
            gate_retry=lambda primary=source_movies[0], slug=movie_slug: _defer_gated_episode(
                primary,
                slug,
                out_root,
                wave,
                movie_fallbacks,
            ),
        )
        if enqueued:
            queued_slugs.add(movie_slug)

    # Am Captcha-Gate haengengebliebene Episoden zentral sammeln. Das gilt auch
    # fuer einen einzelnen Vorbereitungsjob ohne Erfolg in derselben Welle.
    if gated_jobs:
        deferred = 0
        for gated_movie, gated_slug in gated_jobs:
            if (cancelled and cancelled()) or not _queue_slug_claimed(gated_slug):
                continue
            if _defer_gated_episode(
                gated_movie,
                gated_slug,
                out_root,
                wave,
                movie_fallbacks,
            ):
                deferred += 1
                queued_slugs.add(gated_slug)
                continue
            on_job_done(
                False,
                "serienstream-Captcha blieb trotz aller Wiederholungen aktiv",
                gated_movie.title,
                Path(""),
                slug=gated_slug,
            )
        if deferred:
            log(
                f"⏳ {deferred} Episode(n) durch serienstream-Captcha verzoegert "
                f"– automatische Wiederholung nach Cooldown (max. {SERIES_MAX_WAVES} Wellen)."
            )

    # Erst nach der Gate-Entscheidung starten. Bei einer leeren Retry-Welle
    # sieht on_queue_done dadurch entweder den gesetzten Pending-Marker oder
    # den bereits terminal gezaehlten letzten Versuch.
    if start_queue:
        log("─── Starte Queue (max. 2 parallel) ───")
        state.dl_queue.start()

    # Telegram benötigt die konkreten Slugs, um bei Mehrfachanfragen sofort zu
    # erkennen, welche Episoden tatsächlich gestartet/zurückgestellt wurden.
    return queued_slugs


# ---------------------------------------------------------------------------
# Telegram-Filmwünsche
# ---------------------------------------------------------------------------
TELEGRAM_JELLYFIN_WAIT_SECONDS = 30 * 60
TELEGRAM_SERIES_CHOICE_TTL_SECONDS = 10 * 60
TELEGRAM_SERIES_LOADING_TTL_SECONDS = 30 * 60
TELEGRAM_SERIES_PAGE_SIZE = 6
TELEGRAM_SERIES_MAX_PENDING = 20


def _telegram_send(chat_id: str, text: str):
    if _telegram_bot is not None:
        _telegram_bot.send(chat_id, text)


def _rank_telegram_series_results(
    query: str, results: List[FilmpalastSeriesResult],
) -> List[FilmpalastSeriesResult]:
    wanted = _norm_title(query)
    unique: Dict[str, FilmpalastSeriesResult] = {}
    for result in results:
        key = result.base_slug or result.sample_slug
        if key and key not in unique:
            unique[key] = result
    ranked = sorted(
        unique.values(),
        key=lambda result: (
            _norm_title(result.title) != wanted,
            wanted not in _norm_title(result.title),
            abs(len(_norm_title(result.title)) - len(wanted)),
            not _norm_title(result.title).startswith(wanted),
            strip_source_suffix(result.title).casefold(),
        ),
    )
    # Identische Titel verschiedener Anbieter sind keine Auswahlvarianten. Der
    # erste Treffer folgt der Nutzerpriorität; weitere Quellen bleiben Fallbacks.
    deduped: List[FilmpalastSeriesResult] = []
    seen_titles: set[str] = set()
    for result in ranked:
        title_key = _norm_title(result.title)
        if title_key in seen_titles:
            continue
        seen_titles.add(title_key)
        deduped.append(result)
    return deduped


def _prune_telegram_series_choices_locked(
    now: float, reserve_slot: bool = False,
) -> None:
    expired = [
        token for token, entry in state.telegram_series_choices.items()
        if float(entry.get("expires_at", 0)) <= now
    ]
    for token in expired:
        state.telegram_series_choices.pop(token, None)
    limit = TELEGRAM_SERIES_MAX_PENDING - (1 if reserve_slot else 0)
    while len(state.telegram_series_choices) > limit:
        oldest = min(
            state.telegram_series_choices,
            key=lambda token: float(
                state.telegram_series_choices[token].get("created_at", 0),
            ),
        )
        state.telegram_series_choices.pop(oldest, None)


def _telegram_series_choice_markup(token: str, index: int) -> dict:
    return {"inline_keyboard": [[{
        "text": "Diese Serie auswählen",
        "callback_data": f"sr:{token}:{index}",
    }]]}


def _telegram_series_next_markup(token: str, next_index: int) -> dict:
    return {"inline_keyboard": [[{
        "text": "Weitere Treffer anzeigen",
        "callback_data": f"srn:{token}:{next_index}",
    }]]}


def _telegram_movie_choice_markup(token: str, index: int) -> dict:
    return {"inline_keyboard": [[{
        "text": "Diesen Film auswählen",
        "callback_data": f"mr:{token}:{index}",
    }]]}


def _telegram_movie_next_markup(token: str, next_index: int) -> dict:
    return {"inline_keyboard": [[{
        "text": "Weitere Treffer anzeigen",
        "callback_data": f"mrn:{token}:{next_index}",
    }]]}


def _send_telegram_series_choice_page_locked(token: str, entry: dict) -> bool:
    bot = _telegram_bot
    if bot is None:
        return False
    chat_id = entry["chat_id"]
    candidates = entry["candidates"]
    start = int(entry.get("next_index", 0))
    end = min(start + TELEGRAM_SERIES_PAGE_SIZE, len(candidates))
    sent_message_ids = []
    sent_candidate_count = 0

    for index in range(start, end):
        with state.telegram_choices_lock:
            if state.telegram_series_choices.get(token) is not entry:
                break
        candidate = candidates[index]
        title = strip_source_suffix(candidate.title).strip() or candidate.title
        caption = f"{index + 1}. {title}"
        if candidate.year:
            caption += f" ({candidate.year})"
        caption = caption[:1024]
        markup = _telegram_series_choice_markup(token, index)
        message_id = None
        cover_data = _fetch_cover_data(candidate.cover_url) if candidate.cover_url else None
        if cover_data:
            content, content_type = cover_data
            message_id = bot.send_photo(
                chat_id, content, caption, markup, content_type,
            )
        if message_id is None and candidate.cover_url:
            message_id = bot.send_photo(
                chat_id, candidate.cover_url, caption, markup,
            )
        if message_id is None:
            message_id = bot.send_message(
                chat_id, f"🖼️ {caption}\n(Cover nicht verfügbar)", markup,
            )
        if message_id is not None:
            sent_message_ids.append(message_id)
            sent_candidate_count += 1

    with state.telegram_choices_lock:
        current = state.telegram_series_choices.get(token)
    if current is not entry:
        for message_id in sent_message_ids:
            bot.clear_inline_keyboard(chat_id, message_id)
        return False

    if sent_candidate_count and end < len(candidates):
        remaining = len(candidates) - end
        message_id = bot.send_message(
            chat_id,
            f"Noch {remaining} Treffer.",
            _telegram_series_next_markup(token, end),
        )
        if message_id is not None:
            sent_message_ids.append(message_id)

    with state.telegram_choices_lock:
        if state.telegram_series_choices.get(token) is not entry:
            stale = True
        else:
            stale = False
            entry["message_ids"].extend(sent_message_ids)
            entry["next_index"] = end if sent_candidate_count else start
            entry["ready"] = True
            entry["expires_at"] = (
                time.monotonic() + TELEGRAM_SERIES_CHOICE_TTL_SECONDS
            )
    if stale:
        for message_id in sent_message_ids:
            bot.clear_inline_keyboard(chat_id, message_id)
        return False
    return bool(sent_candidate_count)


def _publish_telegram_series_choices_locked(
    chat_id: str,
    request: dict,
    results: List[FilmpalastSeriesResult],
) -> None:
    candidates = list(results)
    if not candidates or _telegram_bot is None:
        _telegram_send(chat_id, "❌ Telegram-Auswahl konnte nicht erstellt werden.")
        return

    now = time.monotonic()
    token = secrets.token_urlsafe(9)
    entry = {
        "kind": "series",
        "chat_id": chat_id,
        "request": dict(request),
        "candidates": candidates,
        "created_at": now,
        "expires_at": now + TELEGRAM_SERIES_LOADING_TTL_SECONDS,
        "message_ids": [],
        "next_index": 0,
        "ready": False,
    }
    old_message_ids = []
    with state.telegram_choices_lock:
        _prune_telegram_series_choices_locked(now)
        for old_token, old_entry in list(state.telegram_series_choices.items()):
            if old_entry.get("chat_id") == chat_id:
                old_message_ids.extend(old_entry.get("message_ids", []))
                state.telegram_series_choices.pop(old_token, None)
        _prune_telegram_series_choices_locked(now, reserve_slot=True)
        state.telegram_series_choices[token] = entry
    if old_message_ids:
        threading.Thread(
            target=_clear_telegram_choice_keyboards,
            args=(chat_id, old_message_ids),
            daemon=True,
        ).start()

    _telegram_send(
        chat_id,
        f"🔎 {len(results)} Serien gefunden. Bitte die richtige auswählen:",
    )
    if not _send_telegram_series_choice_page_locked(token, entry):
        with state.telegram_choices_lock:
            if state.telegram_series_choices.get(token) is entry:
                state.telegram_series_choices.pop(token, None)
        _telegram_send(chat_id, "❌ Treffer konnten nicht an Telegram gesendet werden.")


def _publish_telegram_series_choices(
    chat_id: str,
    request: dict,
    results: List[FilmpalastSeriesResult],
) -> None:
    with state.telegram_choices_publish_lock:
        _publish_telegram_series_choices_locked(chat_id, request, results)


def _consume_telegram_series_choice(
    chat_id: str, token: str, index: int,
) -> tuple[str, Optional[dict], Optional[FilmpalastSeriesResult]]:
    now = time.monotonic()
    with state.telegram_choices_lock:
        _prune_telegram_series_choices_locked(now)
        entry = state.telegram_series_choices.get(token)
        if not entry:
            return "expired", None, None
        if entry.get("chat_id") != chat_id:
            return "forbidden", None, None
        if entry.get("kind", "series") != "series":
            return "invalid", None, None
        if not entry.get("ready"):
            return "loading", None, None
        candidates = entry.get("candidates") or []
        if index < 0 or index >= len(candidates):
            return "invalid", None, None
        state.telegram_series_choices.pop(token, None)
        return "ok", entry, candidates[index]


def _prepare_telegram_series_next_page(
    chat_id: str, token: str, next_index: int,
) -> tuple[str, Optional[dict]]:
    now = time.monotonic()
    with state.telegram_choices_lock:
        _prune_telegram_series_choices_locked(now)
        entry = state.telegram_series_choices.get(token)
        if not entry:
            return "expired", None
        if entry.get("chat_id") != chat_id:
            return "forbidden", None
        if entry.get("kind", "series") != "series":
            return "invalid", None
        if not entry.get("ready"):
            return "loading", None
        candidates = entry.get("candidates") or []
        if next_index != entry.get("next_index") or next_index >= len(candidates):
            return "invalid", None
        entry["ready"] = False
        entry["expires_at"] = now + TELEGRAM_SERIES_LOADING_TTL_SECONDS
        return "ok", entry


def _build_telegram_movie_options(
    query: str, results: List[FilmpalastSearchResult],
) -> List[dict]:
    """Lädt Film-Treffer und bündelt identische Titel/Jahre als Fallbacks."""
    grouped: Dict[tuple, dict] = {}
    seen_urls: set[str] = set()
    for candidate in _telegram_best_result(query, results):
        if not candidate.is_movie:
            continue
        try:
            loaded = load_movie_for_slug(candidate.slug)
        except Exception as exc:
            log(f"Telegram-Filmtreffer nicht ladbar ({candidate.slug}): {exc}", "warn")
            continue
        if not loaded or not loaded.hosters or loaded.url in seen_urls:
            continue
        seen_urls.add(loaded.url)
        title = strip_source_suffix(loaded.title).strip() or strip_source_suffix(candidate.title).strip()
        year = str(loaded.year or candidate.year or "")
        key = (_norm_title(title), year)
        option = grouped.get(key)
        if option is None:
            grouped[key] = {
                "result": candidate,
                "movie": loaded,
                "fallback_movies": [],
                "title": title,
                "year": year,
                "cover_url": loaded.cover_url,
            }
        else:
            option["fallback_movies"].append(loaded)
            if not option.get("cover_url") and loaded.cover_url:
                option["cover_url"] = loaded.cover_url
    return list(grouped.values())


def _filter_existing_telegram_movie_options(
    options: List[dict],
) -> tuple[Optional[List[dict]], List[dict], str]:
    """Entfernt vorhandene Filme, bevor Telegram Download-Buttons anzeigt."""
    jf_items = get_jellyfin_library(force=True)
    with state.jellyfin_cache_lock:
        library_available = state.jellyfin_library_available
    if jf_items is None or not library_available:
        return None, [], "Jellyfin ist nicht erreichbar"

    downloadable = []
    existing = []
    for option in options:
        movie = option["movie"]
        result = option["result"]
        already_available, reason = _content_already_available(movie, result.slug)
        if already_available:
            if _is_jellyfin_safety_block(reason):
                return None, existing, reason
            existing.append(option)
        else:
            downloadable.append(option)
    return downloadable, existing, ""


def _send_telegram_movie_choice_page_locked(token: str, entry: dict) -> bool:
    bot = _telegram_bot
    if bot is None:
        return False
    chat_id = entry["chat_id"]
    candidates = entry["candidates"]
    start = int(entry.get("next_index", 0))
    end = min(start + TELEGRAM_SERIES_PAGE_SIZE, len(candidates))
    sent_message_ids = []
    sent_candidate_count = 0

    for index in range(start, end):
        with state.telegram_choices_lock:
            if state.telegram_series_choices.get(token) is not entry:
                break
        option = candidates[index]
        caption = f"{index + 1}. {option['title']}"
        if option.get("year"):
            caption += f" ({option['year']})"
        source_count = 1 + len(option.get("fallback_movies", []))
        if source_count > 1:
            caption += f" · {source_count} Quellen"
        markup = _telegram_movie_choice_markup(token, index)
        message_id = None
        cover_url = str(option.get("cover_url") or "")
        cover_data = _fetch_cover_data(cover_url) if cover_url else None
        if cover_data:
            content, content_type = cover_data
            message_id = bot.send_photo(chat_id, content, caption[:1024], markup, content_type)
        if message_id is None and cover_url:
            message_id = bot.send_photo(chat_id, cover_url, caption[:1024], markup)
        if message_id is None:
            message_id = bot.send_message(
                chat_id, f"🖼️ {caption}\n(Cover nicht verfügbar)", markup,
            )
        if message_id is not None:
            sent_message_ids.append(message_id)
            sent_candidate_count += 1

    with state.telegram_choices_lock:
        current = state.telegram_series_choices.get(token)
    if current is not entry:
        for message_id in sent_message_ids:
            bot.clear_inline_keyboard(chat_id, message_id)
        return False

    if sent_candidate_count and end < len(candidates):
        remaining = len(candidates) - end
        message_id = bot.send_message(
            chat_id,
            f"Noch {remaining} Treffer.",
            _telegram_movie_next_markup(token, end),
        )
        if message_id is not None:
            sent_message_ids.append(message_id)

    with state.telegram_choices_lock:
        if state.telegram_series_choices.get(token) is not entry:
            stale = True
        else:
            stale = False
            entry["message_ids"].extend(sent_message_ids)
            entry["next_index"] = end if sent_candidate_count else start
            entry["ready"] = True
            entry["expires_at"] = time.monotonic() + TELEGRAM_SERIES_CHOICE_TTL_SECONDS
    if stale:
        for message_id in sent_message_ids:
            bot.clear_inline_keyboard(chat_id, message_id)
        return False
    return bool(sent_candidate_count)


def _publish_telegram_movie_choices(
    chat_id: str, query: str, options: List[dict],
) -> None:
    with state.telegram_choices_publish_lock:
        if not options or _telegram_bot is None:
            _telegram_send(chat_id, "❌ Telegram-Auswahl konnte nicht erstellt werden.")
            return
        now = time.monotonic()
        token = secrets.token_urlsafe(9)
        entry = {
            "kind": "movie",
            "chat_id": chat_id,
            "query": query,
            "candidates": list(options),
            "created_at": now,
            "expires_at": now + TELEGRAM_SERIES_LOADING_TTL_SECONDS,
            "message_ids": [],
            "next_index": 0,
            "ready": False,
        }
        old_message_ids = []
        with state.telegram_choices_lock:
            _prune_telegram_series_choices_locked(now)
            for old_token, old_entry in list(state.telegram_series_choices.items()):
                if old_entry.get("chat_id") == chat_id:
                    old_message_ids.extend(old_entry.get("message_ids", []))
                    state.telegram_series_choices.pop(old_token, None)
            _prune_telegram_series_choices_locked(now, reserve_slot=True)
            state.telegram_series_choices[token] = entry
        if old_message_ids:
            threading.Thread(
                target=_clear_telegram_choice_keyboards,
                args=(chat_id, old_message_ids),
                daemon=True,
            ).start()
        _telegram_send(
            chat_id,
            f"🔎 {len(options)} Filme gefunden. Bitte den richtigen auswählen:",
        )
        if not _send_telegram_movie_choice_page_locked(token, entry):
            with state.telegram_choices_lock:
                if state.telegram_series_choices.get(token) is entry:
                    state.telegram_series_choices.pop(token, None)
            _telegram_send(chat_id, "❌ Treffer konnten nicht an Telegram gesendet werden.")


def _consume_telegram_movie_choice(
    chat_id: str, token: str, index: int,
) -> tuple[str, Optional[dict], Optional[dict]]:
    now = time.monotonic()
    with state.telegram_choices_lock:
        _prune_telegram_series_choices_locked(now)
        entry = state.telegram_series_choices.get(token)
        if not entry:
            return "expired", None, None
        if entry.get("chat_id") != chat_id:
            return "forbidden", None, None
        if entry.get("kind") != "movie":
            return "invalid", None, None
        if not entry.get("ready"):
            return "loading", None, None
        candidates = entry.get("candidates") or []
        if index < 0 or index >= len(candidates):
            return "invalid", None, None
        state.telegram_series_choices.pop(token, None)
        return "ok", entry, candidates[index]


def _prepare_telegram_movie_next_page(
    chat_id: str, token: str, next_index: int,
) -> tuple[str, Optional[dict]]:
    now = time.monotonic()
    with state.telegram_choices_lock:
        _prune_telegram_series_choices_locked(now)
        entry = state.telegram_series_choices.get(token)
        if not entry:
            return "expired", None
        if entry.get("chat_id") != chat_id:
            return "forbidden", None
        if entry.get("kind") != "movie":
            return "invalid", None
        if not entry.get("ready"):
            return "loading", None
        candidates = entry.get("candidates") or []
        if next_index != entry.get("next_index") or next_index >= len(candidates):
            return "invalid", None
        entry["ready"] = False
        entry["expires_at"] = now + TELEGRAM_SERIES_LOADING_TTL_SECONDS
        return "ok", entry


def _telegram_finish_job(job: dict, ok: bool, message: str, out_path: Path):
    chat_id = job["chat_id"]
    title = job["title"]
    year = job.get("year", "")
    if not ok:
        _telegram_send(chat_id, f"❌ Download von „{title}“ fehlgeschlagen: {message}")
        return

    jf_client = get_jellyfin_client()
    with state.jellyfin_cache_lock:
        jellyfin_generation = state.jellyfin_config_generation
    if not jf_client.configured:
        _telegram_send(chat_id, f"✅ „{title}“ wurde geladen: {out_path}\nJellyfin ist nicht konfiguriert.")
        return

    log(f"Telegram: Jellyfin-Scan für «{title}» gestartet.")
    jf_client.refresh_library()
    deadline = time.monotonic() + TELEGRAM_JELLYFIN_WAIT_SECONDS
    while time.monotonic() < deadline:
        items = get_jellyfin_library(force=True)
        with state.jellyfin_cache_lock:
            data_generation = state.jellyfin_movie_data_generation
            library_available = state.jellyfin_library_available
            current_generation = state.jellyfin_config_generation
        if current_generation != jellyfin_generation:
            jellyfin_generation = current_generation
            jf_client = get_jellyfin_client()
            if not jf_client.configured:
                _telegram_send(
                    chat_id,
                    f"✅ „{title}“ wurde geladen: {out_path}\nJellyfin ist nicht konfiguriert.",
                )
                return
            jf_client.refresh_library()
        if items is None or not library_available:
            time.sleep(15)
            continue
        if jf_client.match(
            title, year, items=items, tmdb_id=job.get("tmdb_id", ""),
        ):
            with state.jellyfin_cache_lock:
                stale = (
                    jellyfin_generation != state.jellyfin_config_generation
                    or data_generation != state.jellyfin_movie_data_generation
                )
            if stale:
                continue
            _telegram_send(chat_id, f"✅ „{title}“ ist jetzt in Jellyfin verfügbar.")
            return
        time.sleep(15)

    _telegram_send(
        chat_id,
        f"⚠️ „{title}“ wurde nach {out_path} geladen, ist aber nach 30 Minuten noch nicht in Jellyfin erschienen.",
    )


def _telegram_series_job_result(job: dict, slug: str, ok: bool, message: str, out_path: Path):
    """Sammelt Einzelergebnisse einer Telegram-Serienanfrage."""
    finished_group = None
    with state.telegram_jobs_lock:
        group = state.telegram_series_requests.get(job.get("request_id", ""))
        if not group:
            return
        group["pending_slugs"].discard(slug)
        label = f"S{job['season']:02d}E{job['episode']:02d}"
        if ok:
            group["completed"].append({
                "season": job["season"], "episode": job["episode"],
                "label": label, "path": str(out_path),
            })
        else:
            group["failed"].append(f"{label}: {message}")
        if not group["pending_slugs"]:
            finished_group = state.telegram_series_requests.pop(job["request_id"], None)
    if finished_group:
        threading.Thread(
            target=_telegram_finish_series_request,
            args=(finished_group,),
            daemon=True,
        ).start()


def _telegram_terminal_without_job(slug: str, ok: bool, message: str, out_path: Path):
    """Beendet Telegram-Tracking, wenn kein DownloadJob erzeugt wurde."""
    with state.queue_claim_lock:
        state.picked.discard(slug)
    _persist_queue_state()
    with state.telegram_jobs_lock:
        job = state.telegram_jobs.pop(slug, None)
    if not job:
        return
    if job.get("kind") == "series":
        _telegram_series_job_result(job, slug, ok, message, out_path)
    elif ok:
        threading.Thread(
            target=_telegram_finish_job,
            args=(job, True, message, out_path),
            daemon=True,
        ).start()
    else:
        _telegram_send(job["chat_id"], f"❌ Download von „{job['title']}“ fehlgeschlagen: {message}")


def _telegram_finish_series_request(group: dict):
    chat_id = group["chat_id"]
    title = group["title"]
    completed = group["completed"]
    failed = group["failed"]
    if not completed:
        detail = f"\n{failed[0]}" if failed else ""
        _telegram_send(chat_id, f"❌ Für „{title}“ konnte keine Episode geladen werden.{detail}")
        return

    jf_client = get_jellyfin_client()
    with state.jellyfin_cache_lock:
        jellyfin_generation = state.jellyfin_config_generation
    if not jf_client.configured:
        suffix = f" · {len(failed)} fehlgeschlagen" if failed else ""
        _telegram_send(chat_id, f"✅ {len(completed)} Episode(n) von „{title}“ geladen{suffix}.")
        return

    log(f"Telegram: Jellyfin-Scan für Serie «{title}» gestartet.")
    jf_client.refresh_library()
    deadline = time.monotonic() + TELEGRAM_JELLYFIN_WAIT_SECONDS
    while time.monotonic() < deadline:
        items = get_jellyfin_episodes(force=True)
        series_items = get_jellyfin_series(force=True)
        with state.jellyfin_cache_lock:
            data_generation = state.jellyfin_episode_data_generation
            current_generation = state.jellyfin_config_generation
            episodes_available = state.jellyfin_episodes_available
            series_available = state.jellyfin_series_available
        if current_generation != jellyfin_generation:
            jellyfin_generation = current_generation
            jf_client = get_jellyfin_client()
            if not jf_client.configured:
                suffix = f" · {len(failed)} fehlgeschlagen" if failed else ""
                _telegram_send(
                    chat_id,
                    f"✅ {len(completed)} Episode(n) von „{title}“ geladen{suffix}. "
                    "Jellyfin ist nicht konfiguriert.",
                )
                return
            jf_client.refresh_library()
        if (
            items is None or series_items is None
            or not episodes_available
            or not series_available
        ):
            time.sleep(15)
            continue
        series_ids = jf_client.series_ids_for(
            title,
            tmdb_id=group.get("tmdb_id", ""),
            aliases=group.get("aliases", ()),
            items=series_items,
        )
        if series_ids is None:
            time.sleep(15)
            continue
        if all(
            jf_client.has_episode(
                title, item["season"], item["episode"], items=items,
                aliases=group.get("aliases", ()), series_ids=series_ids,
            )
            for item in completed
        ):
            with state.jellyfin_cache_lock:
                stale = (
                    jellyfin_generation != state.jellyfin_config_generation
                    or data_generation != state.jellyfin_episode_data_generation
                )
            if stale:
                continue
            suffix = f" · {len(failed)} fehlgeschlagen" if failed else ""
            _telegram_send(
                chat_id,
                f"✅ „{title}“: {len(completed)} Episode(n) sind jetzt in Jellyfin verfügbar{suffix}.",
            )
            return
        time.sleep(15)

    suffix = f" {len(failed)} Download(s) sind fehlgeschlagen." if failed else ""
    _telegram_send(
        chat_id,
        f"⚠️ {len(completed)} Episode(n) von „{title}“ wurden geladen, sind aber nach 30 Minuten noch nicht vollständig in Jellyfin erschienen.{suffix}",
    )


# ---------------------------------------------------------------------------
# Seerr-Anfragen (Moonfin/Fire TV -> Seerr -> Royal Downloader)
# ---------------------------------------------------------------------------
SEERR_MEDIA_AVAILABLE = 5
SEERR_SCAN_RETRY_SECONDS = 5 * 60


def configure_moonfin_seerr(seerr_url: str, enabled: bool) -> dict:
    """Konfiguriert Plugin 1.9.1 und aktuelle Versionen ohne andere Werte zu löschen."""
    jf_url = str(state.jellyfin_cfg.get("url") or "").strip().rstrip("/")
    api_key = str(state.jellyfin_cfg.get("api_key") or "").strip()
    user_id = str(state.jellyfin_cfg.get("user_id") or "").strip()
    if not jf_url or not api_key:
        return {"configured": False, "detail": "Jellyfin ist nicht konfiguriert."}
    session = requests.Session()
    headers = {"X-Emby-Token": api_key, "Accept": "application/json"}
    try:
        response = session.get(f"{jf_url}/Plugins", headers=headers, timeout=10)
        response.raise_for_status()
        plugins = response.json()
        plugin = next(
            (item for item in plugins if str(item.get("Name") or "").casefold() == "moonfin"),
            None,
        )
        if not plugin or not plugin.get("Id"):
            return {"configured": False, "detail": "Moonfin-Plugin ist nicht installiert."}
        plugin_id = plugin["Id"]
        config_url = f"{jf_url}/Plugins/{plugin_id}/Configuration"
        response = session.get(config_url, headers=headers, timeout=10)
        response.raise_for_status()
        plugin_config = response.json()
        if "SeerrEnabled" in plugin_config or "SeerrUrl" in plugin_config:
            plugin_config.update({
                "SeerrEnabled": bool(enabled),
                "SeerrUrl": seerr_url,
                "SeerrDisplayName": "Seerr",
            })
        else:
            plugin_config.update({
                "JellyseerrEnabled": bool(enabled),
                "JellyseerrUrl": seerr_url,
                "JellyseerrDisplayName": "Seerr",
            })
        # Benutzerprofil zuerst speichern. Das anschließende Admin-Config-POST
        # kann das Plugin kurz neu laden; umgekehrt wäre das Profil-POST racy.
        if user_id:
            settings_url = f"{jf_url}/Moonfin/Settings/{user_id}"
            response = session.get(settings_url, headers=headers, timeout=10)
            current = response.json() if response.status_code == 200 else {}
            settings = dict(current) if isinstance(current, dict) else {}
            settings["schemaVersion"] = 2
            settings["syncEnabled"] = True
            for profile_name in ("global", "tv"):
                profile = settings.get(profile_name)
                profile = dict(profile) if isinstance(profile, dict) else {}
                profile["jellyseerrEnabled"] = bool(enabled)
                settings[profile_name] = profile
            response = session.post(
                settings_url,
                headers={**headers, "Content-Type": "application/json"},
                json={
                    "settings": settings,
                    "clientId": "royal-downloader",
                    "mergeMode": "merge",
                },
                timeout=10,
            )
            response.raise_for_status()
        response = session.post(
            config_url, headers={**headers, "Content-Type": "application/json"},
            json=plugin_config, timeout=10,
        )
        response.raise_for_status()
        return {"configured": True, "detail": "Moonfin wurde konfiguriert."}
    except (requests.RequestException, ValueError, TypeError) as exc:
        return {"configured": False, "detail": f"Moonfin-Konfiguration fehlgeschlagen: {exc}"}


def _seerr_client() -> SeerrClient:
    return SeerrClient(
        state.seerr_cfg.get("url", ""),
        state.seerr_cfg.get("api_key", ""),
    )


def _save_seerr_requests_locked() -> bool:
    snapshot = {key: dict(value) for key, value in state.seerr_requests.items()}
    return appconfig.save_seerr_requests(snapshot)


def _seerr_update_record(request_id, **updates) -> dict:
    key = str(request_id)
    with state.seerr_requests_lock:
        record = state.seerr_requests.setdefault(key, {"request_id": int(request_id)})
        record.update(updates)
        record["updated_at"] = time.time()
        _save_seerr_requests_locked()
        return dict(record)


def _seerr_mark_failure(request_id, message: str, status: str = "failed") -> None:
    key = str(request_id)
    with state.seerr_requests_lock:
        record = state.seerr_requests.setdefault(key, {"request_id": int(request_id)})
        attempts = int(record.get("attempts", 0) or 0) + 1
        retry_delay = min(6 * 60 * 60, 5 * 60 * (2 ** min(attempts - 1, 6)))
        if status == "needs_review":
            retry_delay = max(retry_delay, 24 * 60 * 60)
        record.update({
            "status": status,
            "message": str(message)[:400],
            "attempts": attempts,
            "next_retry": time.time() + retry_delay,
            "pending_slugs": [],
            "updated_at": time.time(),
        })
        _save_seerr_requests_locked()
    log(f"Seerr #{request_id}: {message}", "warn")


def _seerr_job_result(job: dict, slug: str, ok: bool, message: str, out_path: Path) -> None:
    request_id = str(job.get("request_id", ""))
    if not request_id:
        return
    with state.seerr_requests_lock:
        record = state.seerr_requests.get(request_id)
        if not record:
            return
        pending = [value for value in record.get("pending_slugs", []) if value != slug]
        completed = list(record.get("completed_slugs", []))
        failures = list(record.get("failures", []))
        if ok:
            if slug not in completed:
                completed.append(slug)
        else:
            failures.append({"slug": slug, "message": str(message)[:240]})
        record.update({
            "pending_slugs": pending,
            "completed_slugs": completed,
            "failures": failures[-50:],
            "updated_at": time.time(),
        })
        if not pending:
            if failures:
                record["status"] = "partial" if completed else "failed"
                attempts = int(record.get("attempts", 0) or 0) + 1
                record["attempts"] = attempts
                record["next_retry"] = time.time() + min(
                    6 * 60 * 60, 5 * 60 * (2 ** min(attempts - 1, 6)),
                )
                record["message"] = (
                    f"{len(completed)} erfolgreich, {len(failures)} fehlgeschlagen"
                    if completed else str(message)[:400]
                )
            else:
                record["status"] = "completed"
                record["message"] = "Download abgeschlossen; Seerr wartet auf den Jellyfin-Scan."
                record["next_retry"] = 0
        _save_seerr_requests_locked()
    if not record.get("pending_slugs"):
        log(
            f"Seerr #{request_id}: {record.get('status')} "
            f"({len(record.get('completed_slugs', []))} Download(s))"
        )


def _seerr_terminal_without_job(slug: str, ok: bool, message: str, out_path: Path) -> None:
    with state.queue_claim_lock:
        state.picked.discard(slug)
    _persist_queue_state()
    with state.seerr_jobs_lock:
        jobs = state.seerr_jobs.pop(slug, [])
    for job in jobs:
        _seerr_job_result(job, slug, ok, message, out_path)


def _seerr_register_request_jobs(request_id, items: dict, title: str, **record_values) -> None:
    pending_slugs = list(items)
    _seerr_update_record(
        request_id,
        status="queued",
        title=title,
        pending_slugs=pending_slugs,
        slugs=sorted(set(record_values.pop("slugs", [])) | set(pending_slugs)),
        items=items,
        failures=[],
        message=f"{len(pending_slugs)} Download(s) eingeplant.",
        **record_values,
    )
    with state.seerr_jobs_lock:
        for slug, item in items.items():
            job = {
                "request_id": str(request_id),
                "title": title,
                **item,
            }
            jobs = state.seerr_jobs.setdefault(slug, [])
            jobs[:] = [
                existing for existing in jobs
                if str(existing.get("request_id", "")) != str(request_id)
            ]
            jobs.append(job)


def _seerr_movie_title_key(value: str) -> str:
    """Normalisiert Quelltitel inklusive optional angehängtem Erscheinungsjahr."""
    title = strip_source_suffix(str(value or "").strip())
    title = re.sub(r"\s*[\(\[]?(?:19|20)\d{2}[\)\]]?\s*$", "", title).strip()
    return _norm_title(title)


def _seerr_movie_aliases(title: str, original_title: str) -> List[tuple[str, str]]:
    """Liefert nur Aliase, die von den überwiegend lateinischen Katalogen suchbar sind."""
    aliases: List[tuple[str, str]] = []
    seen: set[str] = set()
    for raw_value in (title, original_title):
        value = " ".join(str(raw_value or "").split()).strip()
        key = _seerr_movie_title_key(value)
        # CJK-/sonstige Originaltitel wurden bisher zu einem leeren Schlüssel
        # und ließen dadurch beliebige Treffer wie exakte Matches aussehen.
        if not value or not key or key in seen:
            continue
        seen.add(key)
        aliases.append((value, key))
    return aliases


def _seerr_http_status(exc: Exception) -> int:
    response = getattr(exc, "response", None)
    for value in (getattr(response, "status_code", None), getattr(exc, "code", None)):
        try:
            return int(value)
        except (TypeError, ValueError, OverflowError):
            continue
    return 0


def _seerr_explicitly_non_german(movie: FilmpalastMovie) -> bool:
    """True, wenn jeder Hoster explizit oder über seinen Anbieter nichtdeutsch ist."""
    hosters = list(movie.hosters or [])
    return bool(hosters) and all(
        (
            language := _movie_content_language(
                movie,
                str(getattr(hoster, "language", "") or ""),
            )
        )
        and language != "de"
        for hoster in hosters
    )


def _seerr_find_movie_sources(metadata: dict, tmdb_id: int) -> List[tuple]:
    """Findet wenige, exakt passende und TMDB-bestätigte deutsche Filmquellen."""
    title = str(metadata.get("title") or "").strip()
    original_title = str(metadata.get("original_title") or "").strip()
    year = str(metadata.get("year") or "").strip()
    aliases = _seerr_movie_aliases(title, original_title)
    if not aliases:
        raise RuntimeError(f"Kein durchsuchbarer Titel für „{title or tmdb_id}“ vorhanden")

    movie_options: List[tuple] = []
    attempted_slugs: set[str] = set()
    seen_urls: set[str] = set()
    rate_limited = False
    non_german_found = False
    tmdb_client = get_tmdb_client()
    max_detail_requests = 8

    for query, query_key in aliases:
        candidates = []
        for candidate in search_movie_candidates(query):
            if not candidate.is_movie or candidate.slug in attempted_slugs:
                continue
            if _seerr_movie_title_key(candidate.title) != query_key:
                continue
            candidate_year = str(candidate.year or "").strip()
            if year and candidate_year and candidate_year != year:
                continue
            candidates.append(candidate)
        candidates.sort(key=lambda candidate: (
            bool(year) and str(candidate.year or "").strip() != year,
            not bool(str(candidate.year or "").strip()),
            strip_source_suffix(candidate.title).casefold(),
        ))

        for candidate in candidates:
            if len(attempted_slugs) >= max_detail_requests:
                break
            # Vor dem Netzaufruf markieren, damit derselbe Slug über einen
            # zweiten Alias nicht erneut geladen wird.
            attempted_slugs.add(candidate.slug)
            try:
                loaded = load_movie_for_slug(candidate.slug)
            except Exception as exc:
                status = _seerr_http_status(exc)
                rate_limited = rate_limited or status == 429
                suffix = f" (HTTP {status})" if status else f": {exc}"
                log(f"Seerr-Filmquelle übersprungen: {candidate.slug}{suffix}", "warn")
                continue
            if not loaded or not loaded.hosters:
                continue

            loaded_title = strip_source_suffix(loaded.title)
            loaded_key = _seerr_movie_title_key(loaded_title)
            if loaded_key not in {key for _value, key in aliases}:
                continue
            loaded_year = str(loaded.year or candidate.year or "").strip()
            if year and loaded_year and loaded_year != year:
                continue
            try:
                summary = tmdb_client.movie_summary(loaded_title, loaded_year or year)
            except Exception as exc:
                status = _seerr_http_status(exc)
                rate_limited = rate_limited or status == 429
                log(f"TMDB-Prüfung für „{loaded_title}“ übersprungen: {exc}", "warn")
                continue
            if not summary or int(summary.get("tmdb_id") or 0) != int(tmdb_id):
                continue
            if _seerr_explicitly_non_german(loaded):
                non_german_found = True
                log(f"Seerr-Filmquelle ohne deutsche Tonspur übersprungen: {loaded_title}", "warn")
                continue
            if loaded.url in seen_urls:
                continue
            seen_urls.add(loaded.url)
            movie_options.append((candidate, loaded))

        # Der lokalisierte Titel hatte bestätigte Quellen. Den Originaltitel
        # nicht zusätzlich über alle vier Anbieter schicken.
        if movie_options:
            break
        if len(attempted_slugs) >= max_detail_requests:
            break

    if movie_options:
        movie_options.sort(key=lambda value: not any(
            bool(getattr(hoster, "is_de", False))
            for hoster in (value[1].hosters or [])
        ))
        return movie_options
    if rate_limited:
        raise RuntimeError("Filmquellen vorübergehend begrenzt (HTTP 429); neuer Versuch folgt")
    if non_german_found:
        raise RuntimeError(f"„{title}“ gefunden, aber derzeit ohne deutsche Tonspur")
    raise RuntimeError(f"Keine eindeutige Downloadquelle für „{title}“ gefunden")


def _seerr_process_movie(request: SeerrRequest, metadata: dict) -> None:
    request_id = request.request_id
    jf_client = get_jellyfin_client()
    jf_items = get_jellyfin_library(force=True)
    with state.jellyfin_cache_lock:
        library_available = state.jellyfin_library_available
    if not jf_client.configured or jf_items is None or not library_available:
        raise RuntimeError("Jellyfin ist für den sicheren Duplikat-Check nicht erreichbar")

    title = str(metadata.get("title") or "").strip()
    year = str(metadata.get("year") or "")
    if jf_client.match(title, year, items=jf_items, tmdb_id=request.tmdb_id):
        _seerr_update_record(
            request_id, status="available", title=title,
            message="Bereits in Jellyfin vorhanden.", next_retry=0,
        )
        return

    movie_options = _seerr_find_movie_sources(metadata, request.tmdb_id)

    chosen, movie = movie_options[0]
    fallbacks = [value for _candidate, value in movie_options[1:]]
    already_available, reason = _content_already_available(movie, chosen.slug)
    if already_available:
        if _is_jellyfin_safety_block(reason):
            raise RuntimeError(reason)
        _seerr_update_record(
            request_id, status="completed", title=title,
            message=f"Bereits {reason}.", next_retry=0,
        )
        return

    with state.queue_lifecycle_lock:
        active = any(chosen.slug in _job_queue_slugs(job) for job in state.dl_queue.active_jobs())
        with state.queue_claim_lock:
            with state.download_state_lock:
                already_queued = (
                    chosen.slug in state.picked
                    or chosen.slug in state.counted_queue_slugs
                    or active
                )
            if not already_queued:
                state.picked.add(chosen.slug)
    state.fp_movies[chosen.slug] = movie
    item = {
        "kind": "movie", "year": year,
        "tmdb_id": request.tmdb_id,
    }
    _seerr_register_request_jobs(
        request_id, {chosen.slug: item}, title,
        media_type="movie", tmdb_id=request.tmdb_id,
        seasons=[], is_4k=request.is_4k,
    )
    _persist_queue_state()
    if already_queued:
        log(f"Seerr #{request_id}: „{title}“ an laufenden Download angehängt.")
        return
    accepted = _enqueue_automatic_downloads(
        [chosen.slug], movie_fallbacks={chosen.slug: fallbacks},
    )
    if chosen.slug not in accepted:
        _seerr_terminal_without_job(
            chosen.slug, False, "Downloadstart fehlgeschlagen", Path(""),
        )


def _seerr_find_series(metadata: dict) -> Optional[FilmpalastSeries]:
    titles = list(dict.fromkeys(filter(None, (
        str(metadata.get("title") or "").strip(),
        str(metadata.get("original_title") or "").strip(),
    ))))
    wanted = {_norm_title(value) for value in titles if _norm_title(value)}
    matches: Dict[str, FilmpalastSeriesResult] = {}
    for query in titles:
        for candidate in search_series_candidates(query):
            if _norm_title(candidate.title) in wanted:
                matches.setdefault(candidate.sample_slug, candidate)
    if not matches:
        return None
    candidates = list(matches.values())
    year = str(metadata.get("year") or "")
    if year:
        same_year = [candidate for candidate in candidates if str(candidate.year or "") == year]
        if same_year:
            candidates = same_year
        else:
            unknown_year = [candidate for candidate in candidates if not candidate.year]
            if unknown_year:
                candidates = unknown_year
            else:
                raise RuntimeError(
                    "Serientreffer hat ein abweichendes Erscheinungsjahr und muss geprüft werden"
                )
    wanted_tmdb_id = str(metadata.get("tmdb_id") or "").strip()
    tmdb = get_tmdb_client()
    verified = [
        candidate for candidate in candidates
        if tmdb.series_matches_id(
            strip_source_suffix(candidate.title), wanted_tmdb_id, year,
        )
    ]
    if not verified:
        raise RuntimeError(
            "Serientreffer ist ohne bestätigte TMDB-ID mehrdeutig und muss geprüft werden"
        )
    # Mehrere bestätigte Treffer derselben TMDB-Serie sind Anbieter-Fallbacks;
    # search_series_candidates liefert sie bereits in Nutzerpriorität.
    return get_series_for_value(verified[0].sample_slug)


def _seerr_process_series(request: SeerrRequest, metadata: dict) -> None:
    request_id = request.request_id
    jf_client = get_jellyfin_client()
    if not jf_client.configured:
        raise RuntimeError("Jellyfin ist nicht konfiguriert")
    series = _seerr_find_series(metadata)
    if series is None or not series.all_episodes:
        raise RuntimeError(
            f"Keine eindeutige Downloadquelle für „{metadata.get('title') or request.tmdb_id}“ gefunden"
        )

    requested_seasons = set(request.seasons)
    selected = [
        episode for episode in series.all_episodes
        if not requested_seasons or episode.season in requested_seasons
    ]
    if not selected:
        raise RuntimeError("Die angeforderten Staffeln sind beim Anbieter nicht vorhanden")

    downloaded = compute_downloaded_episodes(series)
    jf_episodes = get_jellyfin_episodes(force=True)
    jf_series = get_jellyfin_series(force=True)
    with state.jellyfin_cache_lock:
        jf_available = state.jellyfin_episodes_available and state.jellyfin_series_available
    if jf_episodes is None or jf_series is None or not jf_available:
        raise RuntimeError("Jellyfin ist für den sicheren Duplikat-Check nicht erreichbar")
    aliases = tuple(dict.fromkeys(filter(None, (
        series.title,
        metadata.get("title", ""),
        metadata.get("original_title", ""),
    ))))
    series_ids = jf_client.series_ids_for(
        series.title, tmdb_id=request.tmdb_id, aliases=aliases, items=jf_series,
    )
    if series_ids is None:
        raise RuntimeError("Jellyfin-Zuordnung der Serie ist mehrdeutig")
    missing = [
        episode for episode in selected
        if episode.slug not in downloaded
        and not jf_client.has_episode(
            series.title, episode.season, episode.episode,
            items=jf_episodes, aliases=aliases, series_ids=series_ids,
        )
    ]
    if not missing:
        _seerr_update_record(
            request_id, status="available", title=series.title,
            message="Alle angeforderten Episoden sind bereits vorhanden.", next_retry=0,
        )
        return

    movies: Dict[str, FilmpalastMovie] = {}
    episode_items: Dict[str, dict] = {}
    for episode in missing:
        try:
            movie = load_movie_for_slug(episode.slug)
        except Exception as exc:
            movie = None
            log(f"Seerr-Serie: {episode.label} nicht direkt ladbar: {exc}", "warn")
        if not movie or not movie.hosters:
            movie = _episode_placeholder(episode.slug, series.title)
        movies[episode.slug] = movie
        episode_items[episode.slug] = {
            "kind": "series", "season": episode.season,
            "episode": episode.episode, "tmdb_id": request.tmdb_id,
        }

    candidate_slugs = set(movies)
    with state.queue_lifecycle_lock:
        active_slugs = {
            slug for job in state.dl_queue.active_jobs() for slug in _job_queue_slugs(job)
        }
        with state.queue_claim_lock:
            with state.download_state_lock:
                existing = candidate_slugs & (
                    set(state.picked) | set(state.counted_queue_slugs) | active_slugs
                )
                new_slugs = candidate_slugs - existing
            state.picked.update(new_slugs)
    tracked_slugs = existing | new_slugs
    for slug in tracked_slugs:
        state.fp_movies[slug] = movies[slug]
    items = {slug: episode_items[slug] for slug in tracked_slugs}
    _seerr_register_request_jobs(
        request_id, items, series.title,
        media_type="tv", tmdb_id=request.tmdb_id,
        seasons=list(request.seasons), is_4k=request.is_4k,
    )
    _persist_queue_state()
    if new_slugs:
        accepted = _enqueue_automatic_downloads(sorted(new_slugs))
        for slug in new_slugs - set(accepted):
            _seerr_terminal_without_job(
                slug, False, "Downloadstart fehlgeschlagen", Path(""),
            )
    if existing:
        log(f"Seerr #{request_id}: {len(existing)} Episode(n) an laufende Downloads angehängt.")


def _seerr_retry_completed_scan(request: SeerrRequest, previous: dict) -> None:
    """Stößt den Jellyfin-Scan erneut an, bis Seerr das Medium als verfügbar meldet."""
    now = time.time()
    last_retry = float(previous.get("last_scan_retry", 0) or 0)
    if now - last_retry < SEERR_SCAN_RETRY_SECONDS:
        return
    with state.seerr_scan_retry_lock:
        if now - state.seerr_last_scan_retry < SEERR_SCAN_RETRY_SECONDS:
            return
        state.seerr_last_scan_retry = now
    jellyfin = get_jellyfin_client()
    started = bool(jellyfin.configured and jellyfin.refresh_library())
    message = (
        "Download abgeschlossen; Jellyfin-Scan erneut gestartet."
        if started
        else "Download abgeschlossen; Jellyfin-Scan konnte nicht gestartet werden."
    )
    _seerr_update_record(
        request.request_id,
        status="completed",
        last_scan_retry=now,
        message=message,
    )
    if not started:
        log(f"Seerr #{request.request_id}: {message}", "warn")


def _seerr_record_matches_request(record: dict, request: SeerrRequest) -> bool:
    required = {"media_type", "tmdb_id", "seasons", "is_4k"}
    if not required.issubset(record):
        return False
    try:
        stored_seasons = tuple(sorted(int(value) for value in record.get("seasons", [])))
    except (TypeError, ValueError, OverflowError):
        return False
    stored_4k = record.get("is_4k")
    if isinstance(stored_4k, str):
        stored_4k = stored_4k.strip().casefold() in {"1", "true", "yes", "on"}
    return (
        str(record.get("media_type") or "").casefold() == request.media_type
        and str(record.get("tmdb_id") or "") == str(request.tmdb_id)
        and stored_seasons == tuple(request.seasons)
        and bool(stored_4k) == request.is_4k
    )


def _seerr_reset_reused_request(request_id: str) -> None:
    """Entkoppelt lokalen Altzustand, wenn Seerr eine Request-ID neu verwendet."""
    with state.seerr_jobs_lock:
        for slug, jobs in list(state.seerr_jobs.items()):
            remaining = [
                job for job in jobs
                if str(job.get("request_id", "")) != request_id
            ]
            if remaining:
                state.seerr_jobs[slug] = remaining
            else:
                state.seerr_jobs.pop(slug, None)
    with state.seerr_requests_lock:
        state.seerr_requests.pop(request_id, None)
        _save_seerr_requests_locked()
    log(f"Seerr #{request_id}: geänderte Anfrage erkannt; Altzustand verworfen.")


def _seerr_process_request(request: SeerrRequest) -> None:
    request_id = str(request.request_id)
    with state.seerr_requests_lock:
        previous = dict(state.seerr_requests.get(request_id, {}))
    if previous and not _seerr_record_matches_request(previous, request):
        _seerr_reset_reused_request(request_id)
        previous = {}
    status = previous.get("status", "")
    if request.media_status == SEERR_MEDIA_AVAILABLE:
        if status != "available":
            _seerr_update_record(
                request.request_id, status="available", media_type=request.media_type,
                tmdb_id=request.tmdb_id, seasons=list(request.seasons),
                is_4k=request.is_4k, message="In Jellyfin verfügbar.", next_retry=0,
            )
        return
    if request.is_4k:
        if previous.get("seerr_declined"):
            return
        now = time.time()
        if status == "unsupported" and now < float(previous.get("next_retry", 0) or 0):
            return
        try:
            client = _seerr_client()
            declined = client.decline_request(request.request_id)
            decline_error = getattr(client, "last_error", "")
        except Exception as exc:
            declined = False
            decline_error = str(exc)
        message = (
            "4K-Anfrage in Seerr abgelehnt: Die Downloadquelle garantiert keine 4K-Qualität."
            if declined
            else (
                "4K wird nicht geladen; Seerr-Ablehnung wird erneut versucht"
                + (f": {decline_error}" if decline_error else ".")
            )
        )
        _seerr_update_record(
            request.request_id,
            status="unsupported",
            media_type=request.media_type,
            tmdb_id=request.tmdb_id,
            seasons=list(request.seasons),
            is_4k=True,
            seerr_declined=declined,
            message=message,
            next_retry=0 if declined else now + SEERR_SCAN_RETRY_SECONDS,
        )
        if not declined:
            log(f"Seerr #{request.request_id}: {message}", "warn")
        return
    if status == "completed":
        _seerr_retry_completed_scan(request, previous)
        return
    if status in ("available", "unsupported"):
        return
    if status == "queued":
        pending = set(previous.get("pending_slugs", []))
        with state.queue_claim_lock:
            active = pending & set(state.picked)
        if active:
            return
    if status in ("failed", "partial", "needs_review"):
        if time.time() < float(previous.get("next_retry", 0) or 0):
            return

    _seerr_update_record(
        request.request_id,
        status="resolving",
        media_type=request.media_type,
        tmdb_id=request.tmdb_id,
        seasons=list(request.seasons),
        is_4k=request.is_4k,
        message="Quelle und Jellyfin-Bestand werden geprüft.",
        pending_slugs=[],
    )
    try:
        tmdb = get_tmdb_client()
        if not tmdb.configured:
            raise RuntimeError("TMDB ist nicht konfiguriert")
        if request.media_type == "movie":
            metadata = tmdb.movie_by_id(request.tmdb_id)
            if not metadata:
                raise RuntimeError(f"TMDB-Film {request.tmdb_id} wurde nicht gefunden")
            _seerr_process_movie(request, metadata)
        else:
            metadata = tmdb.series_by_id(request.tmdb_id)
            if not metadata:
                raise RuntimeError(f"TMDB-Serie {request.tmdb_id} wurde nicht gefunden")
            _seerr_process_series(request, metadata)
    except Exception as exc:
        detail = str(exc).casefold()
        kind = (
            "needs_review"
            if "mehrdeutig" in detail or "abweichendes erscheinungsjahr" in detail
            else "failed"
        )
        _seerr_mark_failure(request.request_id, str(exc), kind)


def _hydrate_seerr_jobs() -> None:
    """Verknüpft persistierte Seerr-Wünsche wieder mit der Queue."""
    stale = []
    with state.seerr_requests_lock:
        records = [(key, dict(value)) for key, value in state.seerr_requests.items()]
    with state.queue_claim_lock:
        picked = set(state.picked)
    with state.seerr_jobs_lock:
        for request_id, record in records:
            if record.get("status") != "queued":
                continue
            pending = set(record.get("pending_slugs", []))
            active = pending & picked
            item_map = record.get("items") if isinstance(record.get("items"), dict) else {}
            for slug in active:
                item = item_map.get(slug) if isinstance(item_map.get(slug), dict) else {}
                job = {
                    "request_id": request_id,
                    "title": record.get("title", ""),
                    **item,
                }
                jobs = state.seerr_jobs.setdefault(slug, [])
                if not any(
                    str(existing.get("request_id", "")) == str(request_id)
                    for existing in jobs
                ):
                    jobs.append(job)
            if pending and not active:
                stale.append(request_id)
    for request_id in stale:
        _seerr_mark_failure(request_id, "Offene Queue-Zuordnung nach Neustart verloren")


def seerr_poll_once() -> dict:
    if not state.seerr_poll_lock.acquire(blocking=False):
        return {"ok": False, "detail": "Seerr-Abgleich läuft bereits."}
    try:
        state.seerr_last_poll = time.time()
        cfg = dict(state.seerr_cfg)
        client = _seerr_client()
        if not cfg.get("enabled"):
            return {"ok": False, "detail": "Seerr-Integration ist deaktiviert."}
        if not client.configured:
            state.seerr_last_error = "Seerr-URL oder API-Schlüssel fehlt."
            return {"ok": False, "detail": state.seerr_last_error}
        if not client.test_connection():
            state.seerr_last_error = (
                getattr(client, "last_error", "")
                or "Seerr ist nicht erreichbar oder der API-Schlüssel ist ungültig."
            )
            return {"ok": False, "detail": state.seerr_last_error}
        requests = client.approved_requests()
        if getattr(client, "last_error", ""):
            state.seerr_last_error = client.last_error
            return {"ok": False, "detail": state.seerr_last_error}
        state.seerr_last_success = time.time()
        state.seerr_last_error = ""
        for request in requests:
            _seerr_process_request(request)
        if requests:
            log(f"Seerr-Abgleich: {len(requests)} genehmigte Anfrage(n) geprüft.")
        return {"ok": True, "requests": len(requests)}
    except Exception as exc:
        state.seerr_last_error = str(exc)[:300]
        log(f"Seerr-Abgleich fehlgeschlagen: {exc}", "warn")
        return {"ok": False, "detail": state.seerr_last_error}
    finally:
        state.seerr_poll_lock.release()


def seerr_poll_loop() -> None:
    _hydrate_seerr_jobs()
    while not _seerr_stop_event.is_set():
        if state.seerr_cfg.get("enabled"):
            seerr_poll_once()
        interval = max(15, int(state.seerr_cfg.get("poll_interval_seconds", 60) or 60))
        _seerr_wake_event.wait(interval)
        _seerr_wake_event.clear()


def _parse_telegram_series_request(text: str) -> Optional[dict]:
    if re.match(r"^/film(?:\s|$)", text.strip(), flags=re.IGNORECASE):
        return None
    value = re.sub(r"^/serie\s+", "", text.strip(), flags=re.IGNORECASE)
    match = re.match(
        r"^(?P<title>.+?)\s+(?:(?P<all>alles)|staffel\s*0*(?P<season>\d+)"
        r"(?:\s*(?:ep|e|episode|folge)\s*0*(?P<episode>\d+))?)\s*$",
        value,
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    title = match.group("title").strip().strip('"„“')
    if not title:
        return None
    if match.group("all"):
        return {"title": title, "mode": "all", "season": None, "episode": None}
    season = int(match.group("season"))
    episode = int(match.group("episode")) if match.group("episode") else None
    return {
        "title": title,
        "mode": "episode" if episode is not None else "season",
        "season": season,
        "episode": episode,
    }


def _telegram_series_scope_label(request: dict) -> str:
    if request["mode"] == "all":
        return "alle fehlenden Episoden"
    if request["mode"] == "season":
        return f"Staffel {request['season']}"
    return f"Staffel {request['season']} Episode {request['episode']}"


def _telegram_best_result(query: str, results: List[FilmpalastSearchResult]) -> List[FilmpalastSearchResult]:
    wanted = _norm_title(query)
    return sorted(
        results,
        key=lambda result: (
            _norm_title(result.title) != wanted,
            wanted not in _norm_title(result.title),
            strip_source_suffix(result.title).casefold(),
        ),
    )


def _format_storage_size(value: int) -> str:
    size = float(value)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB", "PiB"):
        if size < 1024 or unit == "PiB":
            return f"{size:.0f} {unit}" if unit in ("B", "KiB", "MiB") else f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} PiB"


def _telegram_storage_text() -> str:
    lines = ["💾 NAS-Speicher"]
    seen_volumes = {}
    for label, raw_path in (("Filme", state.save_path), ("Serien", state.series_path)):
        path = Path(raw_path)
        try:
            usage = shutil.disk_usage(path)
            device = os.stat(path).st_dev
            if device in seen_volumes:
                lines.append(f"{label}: gemeinsames Volume mit {seen_volumes[device]} ({path})")
                continue
            seen_volumes[device] = label
            percent = (usage.used / usage.total * 100) if usage.total else 0
            lines.append(
                f"{label} ({path})\n"
                f"  {_format_storage_size(usage.free)} frei von {_format_storage_size(usage.total)} · {percent:.1f}% belegt"
            )
        except OSError as exc:
            lines.append(f"{label} ({path}): nicht erreichbar ({exc})")
    return "\n".join(lines)


def _telegram_paths_text() -> str:
    lines = ["📁 Speicherpfade"]
    for label, raw_path in (("Filme", state.save_path), ("Serien", state.series_path)):
        path = Path(raw_path)
        status = "erreichbar" if path.is_dir() else "nicht erreichbar"
        lines.append(f"{label}: {path} · {status}")
    return "\n".join(lines)


def _telegram_watchlist_text() -> str:
    if not state.watchlist:
        return "📺 Keine Serien abonniert."
    lines = [f"📺 Abonnierte Serien: {len(state.watchlist)}"]
    for entry in state.watchlist[:25]:
        new_count = len(state.watchlist_new_slugs.get(entry["base_slug"], set()))
        suffix = f" · {new_count} neu" if new_count else ""
        lines.append(f"• {entry['title']}{suffix}")
    if len(state.watchlist) > 25:
        lines.append(f"… und {len(state.watchlist) - 25} weitere")
    return "\n".join(lines)


def _telegram_help_text() -> str:
    return (
        "Royal Downloader\n"
        "Filmtitel – Film prüfen und herunterladen\n"
        "/film Filmtitel – Film ausdrücklich auswählen\n"
        "Serientitel ALLES\n"
        "Serientitel Staffel 2\n"
        "Serientitel Staffel 2 EP 5\n"
        "Mehrere Film- und Serientreffer werden mit Cover zur Auswahl angezeigt.\n"
        "/status – laufende Downloads\n"
        "/speicher – freier NAS-Speicher\n"
        "/pfade – Film- und Serienpfad\n"
        "/abos – abonnierte Serien\n"
        "/jellyfin – Bibliotheksstatus\n"
        "/hilfe – diese Übersicht"
    )


def _run_telegram_series_request(
    chat_id: str,
    request: dict,
    series_value: str,
    wait_for_lock: bool = False,
):
    if not state.telegram_request_lock.acquire(blocking=wait_for_lock):
        _telegram_send(chat_id, "Ein anderer Telegram-Wunsch wird gerade verarbeitet. Versuche es gleich erneut.")
        return
    try:
        jf_client = get_jellyfin_client()
        if not jf_client.configured:
            _telegram_send(chat_id, "Jellyfin-URL oder API-Schlüssel fehlt in den Einstellungen.")
            return

        scope_label = _telegram_series_scope_label(request)
        _telegram_send(chat_id, f"🔎 Lade Serie „{request['title']}“ · {scope_label} …")
        series = get_series_for_value(series_value)
        if series is None or not series.all_episodes:
            _telegram_send(chat_id, f"❌ Serie „{request['title']}“ nicht gefunden.")
            return

        selected = list(series.all_episodes)
        if request["mode"] in ("season", "episode"):
            selected = [ep for ep in selected if ep.season == request["season"]]
        if request["mode"] == "episode":
            selected = [ep for ep in selected if ep.episode == request["episode"]]
        if not selected:
            _telegram_send(chat_id, f"❌ „{series.title}“ enthält {scope_label} nicht.")
            return

        downloaded = compute_downloaded_episodes(series)
        jf_episodes = get_jellyfin_episodes(force=True)
        jf_series = get_jellyfin_series(force=True)
        with state.jellyfin_cache_lock:
            jf_available = (
                state.jellyfin_episodes_available and state.jellyfin_series_available
            )
        if jf_episodes is None or jf_series is None or not jf_available:
            _telegram_send(chat_id, "Jellyfin ist nicht erreichbar. Download wurde zum Duplikatschutz nicht gestartet.")
            return
        try:
            aliases, series_ids, tmdb_id = _episode_jellyfin_identity(
                series.base_slug, series.title, jf_client, jf_series,
            )
        except RuntimeError as exc:
            _telegram_send(chat_id, f"{exc}. Download wurde zum Duplikatschutz nicht gestartet.")
            return
        missing = [
            ep for ep in selected
            if ep.slug not in downloaded
            and not jf_client.has_episode(
                series.title, ep.season, ep.episode, items=jf_episodes,
                aliases=aliases, series_ids=series_ids,
            )
        ]
        if not missing:
            _telegram_send(chat_id, f"✅ „{series.title}“ · {scope_label} ist bereits vollständig vorhanden.")
            return

        _telegram_send(chat_id, f"⬇️ {len(missing)} fehlende Episode(n) werden vorbereitet …")
        jobs: List[tuple] = []
        initial_failures: List[str] = []
        episode_by_slug = {ep.slug: ep for ep in missing}
        for ep in missing:
            try:
                movie = load_movie_for_slug(ep.slug)
            except Exception as exc:
                movie = None
                log(f"Telegram-Serie: {ep.label} nicht ladbar: {exc}", "warn")
            if not movie or not movie.hosters:
                movie = _episode_placeholder(ep.slug, series.title)
                log(
                    f"Telegram-Serie: {ep.label} wird trotz blockierter "
                    "Episodenseite fuer Fallback/Retry eingeplant.",
                    "warn",
                )
            already_available, reason = _content_already_available(movie, ep.slug)
            if already_available:
                initial_failures.append(f"{ep.label}: {reason}")
                continue
            state.fp_movies[ep.slug] = movie
            jobs.append((movie, ep.slug))

        if not jobs:
            _telegram_send(
                chat_id,
                f"❌ Für „{series.title}“ konnte keine der {len(missing)} fehlenden Episoden gestartet werden.",
            )
            return

        request_id = f"{chat_id}:{time.time_ns()}"
        candidate_slugs = {slug for _movie, slug in jobs}
        with state.queue_lifecycle_lock:
            active_slugs = {
                slug for job in state.dl_queue.active_jobs() for slug in _job_queue_slugs(job)
            }
            with state.queue_claim_lock:
                with state.download_state_lock:
                    pending_slugs = {
                        slug for slug in candidate_slugs
                        if slug not in state.picked
                        and slug not in state.counted_queue_slugs
                        and slug not in active_slugs
                    }
                state.picked.update(pending_slugs)
        jobs = [(movie, slug) for movie, slug in jobs if slug in pending_slugs]
        if not jobs:
            _telegram_send(chat_id, "Alle fehlenden Episoden sind bereits eingeplant.")
            return
        group = {
            "chat_id": chat_id,
            "title": series.title,
            "scope_label": scope_label,
            "pending_slugs": set(pending_slugs),
            "completed": [],
            "failed": list(initial_failures),
            "aliases": list(aliases),
            "tmdb_id": tmdb_id,
        }
        with state.telegram_jobs_lock:
            state.telegram_series_requests[request_id] = group
            for _movie, slug in jobs:
                ep = episode_by_slug[slug]
                state.telegram_jobs[slug] = {
                    "kind": "series",
                    "request_id": request_id,
                    "chat_id": chat_id,
                    "title": series.title,
                    "season": ep.season,
                    "episode": ep.episode,
                }

        _persist_queue_state()
        _telegram_send(chat_id, f"▶️ „{series.title}“ · {scope_label}: {len(jobs)} Download(s) starten.")

        try:
            accepted = _enqueue_automatic_downloads(list(pending_slugs))
        except Exception:
            for slug in pending_slugs:
                _telegram_terminal_without_job(slug, False, "Downloadstart fehlgeschlagen", Path(""))
            raise
        for slug in pending_slugs - set(accepted):
            _telegram_terminal_without_job(slug, False, "kein Stream startbar", Path(""))
    except Exception as exc:
        log(f"Telegram-Serienwunsch fehlgeschlagen: {exc}", "warn")
        _telegram_send(chat_id, f"❌ Serienwunsch fehlgeschlagen: {exc}")
    finally:
        state.telegram_request_lock.release()


def _handle_telegram_series_request(chat_id: str, request: dict):
    title = str(request.get("title") or "").strip()
    if (
        title.startswith((
            SERIENSTREAM_PREFIX, MOFLIX_PREFIX, EINSCHALTEN_PREFIX,
            KINOX_PREFIX, KINOGER_PREFIX, MEGAKINO_PREFIX, XCINE_PREFIX,
        ))
        or title.startswith("http://")
        or title.startswith("https://")
    ):
        _run_telegram_series_request(chat_id, request, title)
        return

    if not state.telegram_request_lock.acquire(blocking=False):
        _telegram_send(chat_id, "Ein anderer Telegram-Wunsch wird gerade verarbeitet. Versuche es gleich erneut.")
        return
    try:
        if not get_jellyfin_client().configured:
            _telegram_send(chat_id, "Jellyfin-URL oder API-Schlüssel fehlt in den Einstellungen.")
            return
        scope_label = _telegram_series_scope_label(request)
        _telegram_send(chat_id, f"🔎 Suche Serie „{title}“ · {scope_label} …")
        results = _rank_telegram_series_results(title, search_series_candidates(title))
        if not results:
            _telegram_send(chat_id, f"❌ Serie „{title}“ nicht gefunden.")
            return
        if len(results) > 1:
            _publish_telegram_series_choices(chat_id, request, results)
            return
        selected_value = results[0].sample_slug
    except Exception as exc:
        log(f"Telegram-Seriensuche fehlgeschlagen: {exc}", "warn")
        _telegram_send(chat_id, f"❌ Seriensuche fehlgeschlagen: {exc}")
        return
    finally:
        state.telegram_request_lock.release()

    _run_telegram_series_request(chat_id, request, selected_value)


def _run_telegram_movie_request(
    chat_id: str,
    query: str,
    option: dict,
    wait_for_lock: bool = False,
):
    if not state.telegram_request_lock.acquire(blocking=wait_for_lock):
        _telegram_send(chat_id, "Ein anderer Telegram-Wunsch wird gerade verarbeitet. Versuche es gleich erneut.")
        return
    try:
        jf_client = get_jellyfin_client()
        if not jf_client.configured:
            _telegram_send(chat_id, "Jellyfin-URL oder API-Schlüssel fehlt in den Einstellungen.")
            return

        movie = option["movie"]
        chosen_result = option["result"]
        fallback_movies = list(option.get("fallback_movies", []))
        title = str(option.get("title") or strip_source_suffix(movie.title)).strip()
        year = str(option.get("year") or movie.year or chosen_result.year or "")
        _telegram_send(chat_id, f"🔎 Prüfe „{title}“{f' ({year})' if year else ''} …")

        jf_items = get_jellyfin_library(force=True)
        if jf_items is None or not state.jellyfin_library_available:
            _telegram_send(chat_id, "Jellyfin ist nicht erreichbar. Download wurde zum Duplikatschutz nicht gestartet.")
            return
        tmdb = get_tmdb_client().movie_summary(title, year)
        if jf_client.match(
            title, year, items=jf_items, tmdb_id=(tmdb or {}).get("tmdb_id", ""),
        ):
            _telegram_send(chat_id, f"✅ „{title}“ ist bereits in Jellyfin vorhanden.")
            return
        already_available, reason = _content_already_available(movie, chosen_result.slug)
        if already_available:
            _telegram_send(chat_id, f"Download nicht gestartet: „{title}“ ist {reason}.")
            return

        with state.queue_lifecycle_lock:
            physically_active = any(
                chosen_result.slug in _job_queue_slugs(job)
                for job in state.dl_queue.active_jobs()
            )
            with state.queue_claim_lock:
                with state.download_state_lock:
                    already_queued = (
                        chosen_result.slug in state.picked
                        or chosen_result.slug in state.counted_queue_slugs
                        or physically_active
                    )
                if not already_queued:
                    state.picked.add(chosen_result.slug)
        if already_queued:
            _telegram_send(chat_id, f"„{title}“ ist bereits eingeplant.")
            return

        state.fp_movies[chosen_result.slug] = movie
        _persist_queue_state()
        with state.telegram_jobs_lock:
            state.telegram_jobs[chosen_result.slug] = {
                "chat_id": chat_id,
                "query": query,
                "title": title,
                "year": year,
                "tmdb_id": (tmdb or {}).get("tmdb_id", ""),
            }

        source_count = 1 + len(fallback_movies)
        source_note = f" · {source_count} Filmquellen" if source_count > 1 else ""
        _telegram_send(
            chat_id,
            f"⬇️ Gefunden: „{title}“{f' ({year})' if year else ''}{source_note}. Download startet.",
        )
        try:
            accepted = _enqueue_automatic_downloads(
                [chosen_result.slug],
                movie_fallbacks={chosen_result.slug: fallback_movies},
            )
        except Exception:
            _telegram_terminal_without_job(
                chosen_result.slug, False, "Downloadstart fehlgeschlagen", Path(""),
            )
            raise
        if chosen_result.slug not in accepted:
            _telegram_terminal_without_job(
                chosen_result.slug, False, "Downloadstart fehlgeschlagen", Path(""),
            )
    except Exception as exc:
        log(f"Telegram-Filmwunsch fehlgeschlagen: {exc}", "warn")
        _telegram_send(chat_id, f"❌ Filmwunsch fehlgeschlagen: {exc}")
    finally:
        state.telegram_request_lock.release()


def _handle_telegram_movie_request(chat_id: str, query: str):
    if not state.telegram_request_lock.acquire(blocking=False):
        _telegram_send(chat_id, "Ein anderer Telegram-Filmwunsch wird gerade verarbeitet. Versuche es gleich erneut.")
        return
    selected = None
    try:
        if not get_jellyfin_client().configured:
            _telegram_send(chat_id, "Jellyfin-URL oder API-Schlüssel fehlt in den Einstellungen.")
            return
        _telegram_send(chat_id, f"🔎 Suche Film „{query}“ …")
        results = search_movie_candidates(query)
        if not results:
            _telegram_send(chat_id, f"❌ Kein Film zu „{query}“ gefunden.")
            return
        options = _build_telegram_movie_options(query, results)
        if not options:
            _telegram_send(
                chat_id,
                f"❌ „{query}“ wurde gefunden, aber kein funktionierender Hoster ist verfügbar.",
            )
            return
        requires_selection = len(options) > 1
        options, existing_options, check_error = _filter_existing_telegram_movie_options(options)
        if options is None:
            _telegram_send(
                chat_id,
                f"{check_error}. Download wurde zum Duplikatschutz nicht angeboten.",
            )
            return
        if not options:
            _telegram_send(chat_id, f"✅ „{query}“ ist bereits vorhanden.")
            return
        if existing_options:
            count = len(existing_options)
            message = (
                "✅ 1 bereits vorhandener Treffer wird nicht zum Download angeboten."
                if count == 1
                else f"✅ {count} bereits vorhandene Treffer werden nicht zum Download angeboten."
            )
            _telegram_send(chat_id, message)
        if requires_selection or len(options) > 1:
            _publish_telegram_movie_choices(chat_id, query, options)
            return
        selected = options[0]
    except Exception as exc:
        log(f"Telegram-Filmsuche fehlgeschlagen: {exc}", "warn")
        _telegram_send(chat_id, f"❌ Filmsuche fehlgeschlagen: {exc}")
        return
    finally:
        state.telegram_request_lock.release()

    _run_telegram_movie_request(chat_id, query, selected)


def _clear_telegram_choice_keyboards(chat_id: str, message_ids: List[int]) -> None:
    bot = _telegram_bot
    if bot is None:
        return
    for message_id in message_ids:
        bot.clear_inline_keyboard(chat_id, message_id)


def handle_telegram_callback(
    chat_id: str, callback_query_id: str, data: str, sender_name: str = "",
):
    bot = _telegram_bot
    if bot is None:
        return
    allowed_chat = str(state.telegram_cfg.get("chat_id", "")).strip()
    if not allowed_chat or chat_id != allowed_chat:
        bot.answer_callback(callback_query_id, "Nicht erlaubt.")
        log(f"Telegram-Callback von nicht erlaubter Chat-ID {chat_id} verworfen.", "warn")
        return

    movie_next_match = re.fullmatch(r"mrn:([A-Za-z0-9_-]{8,32}):(\d{1,4})", data or "")
    if movie_next_match:
        token, raw_index = movie_next_match.groups()
        status, entry = _prepare_telegram_movie_next_page(chat_id, token, int(raw_index))
        if status == "loading":
            bot.answer_callback(callback_query_id, "Treffer werden noch geladen.")
            return
        if status == "forbidden":
            bot.answer_callback(callback_query_id, "Diese Auswahl gehört zu einem anderen Chat.")
            return
        if status != "ok" or entry is None:
            bot.answer_callback(callback_query_id, "Seite abgelaufen oder bereits geladen.")
            return
        bot.answer_callback(callback_query_id, "Weitere Treffer werden geladen.")
        with state.telegram_choices_publish_lock:
            if not _send_telegram_movie_choice_page_locked(token, entry):
                _telegram_send(chat_id, "❌ Weitere Treffer konnten nicht gesendet werden.")
        return

    movie_match = re.fullmatch(r"mr:([A-Za-z0-9_-]{8,32}):(\d{1,4})", data or "")
    if movie_match:
        token, raw_index = movie_match.groups()
        status, entry, option = _consume_telegram_movie_choice(
            chat_id, token, int(raw_index),
        )
        if status == "loading":
            bot.answer_callback(callback_query_id, "Treffer werden noch geladen.")
            return
        if status == "forbidden":
            bot.answer_callback(callback_query_id, "Diese Auswahl gehört zu einem anderen Chat.")
            return
        if status != "ok" or entry is None or option is None:
            bot.answer_callback(callback_query_id, "Auswahl abgelaufen oder bereits verwendet.")
            return
        title = str(option.get("title") or "Film")
        bot.answer_callback(callback_query_id, f"Ausgewählt: {title}")
        threading.Thread(
            target=_clear_telegram_choice_keyboards,
            args=(chat_id, list(entry.get("message_ids", []))),
            daemon=True,
        ).start()
        _telegram_send(chat_id, f"✅ Ausgewählt: „{title}“.")
        _run_telegram_movie_request(
            chat_id, entry["query"], option, wait_for_lock=True,
        )
        return

    next_match = re.fullmatch(r"srn:([A-Za-z0-9_-]{8,32}):(\d{1,4})", data or "")
    if next_match:
        token, raw_index = next_match.groups()
        status, entry = _prepare_telegram_series_next_page(
            chat_id, token, int(raw_index),
        )
        if status == "loading":
            bot.answer_callback(callback_query_id, "Treffer werden noch geladen.")
            return
        if status == "forbidden":
            bot.answer_callback(callback_query_id, "Diese Auswahl gehört zu einem anderen Chat.")
            return
        if status != "ok" or entry is None:
            bot.answer_callback(callback_query_id, "Seite abgelaufen oder bereits geladen.")
            return
        bot.answer_callback(callback_query_id, "Weitere Treffer werden geladen.")
        with state.telegram_choices_publish_lock:
            if not _send_telegram_series_choice_page_locked(token, entry):
                _telegram_send(chat_id, "❌ Weitere Treffer konnten nicht gesendet werden.")
        return

    match = re.fullmatch(r"sr:([A-Za-z0-9_-]{8,32}):(\d{1,4})", data or "")
    if not match:
        bot.answer_callback(callback_query_id, "Unbekannte Auswahl.")
        return
    token, raw_index = match.groups()
    status, entry, candidate = _consume_telegram_series_choice(
        chat_id, token, int(raw_index),
    )
    if status == "loading":
        bot.answer_callback(callback_query_id, "Treffer werden noch geladen.")
        return
    if status == "forbidden":
        bot.answer_callback(callback_query_id, "Diese Auswahl gehört zu einem anderen Chat.")
        return
    if status != "ok" or entry is None or candidate is None:
        bot.answer_callback(callback_query_id, "Auswahl abgelaufen oder bereits verwendet.")
        return

    title = strip_source_suffix(candidate.title).strip() or candidate.title
    bot.answer_callback(callback_query_id, f"Ausgewählt: {title}")
    threading.Thread(
        target=_clear_telegram_choice_keyboards,
        args=(chat_id, list(entry.get("message_ids", []))),
        daemon=True,
    ).start()
    _telegram_send(chat_id, f"✅ Ausgewählt: „{title}“.")
    _run_telegram_series_request(
        chat_id,
        entry["request"],
        candidate.sample_slug,
        wait_for_lock=True,
    )


def handle_telegram_message(chat_id: str, text: str, sender_name: str = ""):
    cfg = state.telegram_cfg
    allowed_chat = str(cfg.get("chat_id", "")).strip()

    # Sicherer Einrichtungsmodus: Ohne Whitelist werden keine Downloads erlaubt,
    # der Bot verrät dem Absender lediglich dessen Chat-ID.
    if not allowed_chat:
        _telegram_send(
            chat_id,
            f"Deine Chat-ID ist {chat_id}. Trage sie in Royal Downloader → Einstellungen → Telegram ein.",
        )
        return
    if chat_id != allowed_chat:
        log(f"Telegram-Zugriff von nicht erlaubter Chat-ID {chat_id} verworfen.", "warn")
        return

    command = text.split(maxsplit=1)[0].split("@", 1)[0].casefold()
    if command in ("/start", "/help", "/hilfe"):
        _telegram_send(chat_id, _telegram_help_text())
        return
    if command == "/status":
        active = state.dl_queue.active_count()
        pending = state.dl_queue.pending_count()
        with state.telegram_jobs_lock:
            titles = sorted({job["title"] for job in state.telegram_jobs.values()})
        detail = f"\nTelegram: {', '.join(titles)}" if titles else ""
        _telegram_send(chat_id, f"⬇️ Downloader: {active} aktiv, {pending} wartend.{detail}")
        return
    if command in ("/speicher", "/storage", "/disk"):
        _telegram_send(chat_id, _telegram_storage_text())
        return
    if command == "/pfade":
        _telegram_send(chat_id, _telegram_paths_text())
        return
    if command in ("/abos", "/serien"):
        _telegram_send(chat_id, _telegram_watchlist_text())
        return
    if command == "/jellyfin":
        jf_client = get_jellyfin_client()
        if not jf_client.configured:
            _telegram_send(chat_id, "Jellyfin ist nicht konfiguriert.")
            return
        movies = jf_client.list_movies()
        episodes = jf_client.list_episodes()
        if movies is None or episodes is None:
            _telegram_send(chat_id, "⚠️ Jellyfin ist derzeit nicht erreichbar.")
            return
        _telegram_send(
            chat_id,
            f"🎞️ Jellyfin\n{len(movies)} Filme · {len(episodes)} Episoden\n{jf_client.base_url}",
        )
        return

    series_request = _parse_telegram_series_request(text)
    if series_request:
        _handle_telegram_series_request(chat_id, series_request)
        return
    if command == "/serie":
        _telegram_send(
            chat_id,
            "Format: /serie The Rookie ALLES · /serie The Rookie Staffel 8 · /serie The Rookie Staffel 8 EP 3",
        )
        return

    query = re.sub(r"^/film\s+", "", text, flags=re.IGNORECASE).strip()
    if not query or query.startswith("/"):
        _telegram_send(chat_id, "Sende einen Filmtitel oder nutze /status.")
        return

    _handle_telegram_movie_request(chat_id, query)


# ---------------------------------------------------------------------------
# Automatische Bibliotheks-Prüfung (Benachrichtigungs-Glocke)
# ---------------------------------------------------------------------------
def is_within_download_window() -> bool:
    """True, wenn die aktuelle Uhrzeit im konfigurierten Download-Zeitfenster
    liegt. Ist kein Fenster gesetzt (start/end None), gilt: jederzeit. start>end
    bedeutet über Mitternacht (z.B. 1–7 Uhr = nachts)."""
    start = state.automation.get("dl_window_start")
    end = state.automation.get("dl_window_end")
    if start is None or end is None:
        return True
    now_h = time.localtime().tm_hour   # nutzt die Container-Zeitzone (TZ)
    if start == end:
        return True
    if start < end:
        return start <= now_h < end
    return now_h >= start or now_h < end   # Fenster über Mitternacht


def _auto_download_new_episodes():
    """Lädt alle als neu erkannten Episoden abonnierter Serien automatisch
    herunter (nutzt dieselbe Pipeline wie der manuelle Download inkl.
    konfigurierter Anbieter-Fallbacks). Neue Jobs werden auch
    während eines laufenden Downloads an dieselbe 2-Slot-Queue angehängt."""
    # Trigger nicht verwerfen: Ein direkt danach abgeschlossener Abo-/JF-Check
    # kann zusätzliche Slugs geliefert haben, die der erste Snapshot nicht sah.
    state.auto_download_lock.acquire()
    claimed: List[str] = []
    try:
        if not state.automation.get("auto_download"):
            return
        if not is_within_download_window():
            log("Auto-Download: außerhalb des Zeitfensters – warte.")
            broadcast({"type": "watchlist_update", **watchlist_payload()})
            return
        with state.watchlist_lock:
            pending = sorted(
                {
                    slug
                    for entry in state.watchlist
                    if not entry.get("last_error")
                    for slug in state.watchlist_new_slugs.get(entry.get("base_slug", ""), set())
                },
                key=episode_sort_key,
            )
        if not pending:
            return

        prepared_slugs: List[str] = []
        for slug in pending:
            if not _watchlist_retry_allowed(slug):
                continue
            with state.watchlist_lock:
                if not any(
                    not entry.get("last_error")
                    and slug in state.watchlist_new_slugs.get(entry.get("base_slug", ""), set())
                    for entry in state.watchlist
                ):
                    continue
            with state.queue_lifecycle_lock:
                physically_active = any(
                    slug in _job_queue_slugs(job) for job in state.dl_queue.active_jobs()
                )
                with state.queue_claim_lock:
                    with state.download_state_lock:
                        already_owned = (
                            slug in state.picked or slug in state.counted_queue_slugs
                        )
                    if physically_active or already_owned:
                        continue
                    state.picked.add(slug)
                    claimed.append(slug)
            try:
                movie = load_movie_for_slug(slug)
            except Exception as exc:
                log(f"Auto-Download: «{slug}» nicht ladbar: {exc}", "warn")
                movie = None
            if not movie or not movie.hosters:
                movie = _episode_placeholder(slug)
                log(
                    f"Auto-Download: «{slug}» wird trotz blockierter "
                    "Episodenseite fuer Fallback/Retry eingeplant.",
                    "warn",
                )

            already_available, reason = _content_already_available(movie, slug)
            if already_available:
                with state.queue_claim_lock:
                    state.picked.discard(slug)
                claimed.remove(slug)
                with state.watchlist_lock:
                    for entry in state.watchlist:
                        base_slug = entry.get("base_slug", "")
                        pending_for_entry = state.watchlist_new_slugs.get(base_slug, set())
                        if slug not in pending_for_entry:
                            continue
                        if _is_jellyfin_safety_block(reason):
                            entry["last_error"] = f"{reason} – Auto-Download pausiert"
                            continue
                        pending_for_entry.discard(slug)
                        failures = entry.get("failed_downloads")
                        if isinstance(failures, dict):
                            failures.pop(slug, None)
                        if not pending_for_entry:
                            state.watchlist_new_slugs.pop(base_slug, None)
                log(f"Auto-Download übersprungen: «{slug}» ist {reason}.")
                continue
            state.fp_movies[slug] = movie
            prepared_slugs.append(slug)
            with state.watchlist_lock:
                for entry in state.watchlist:
                    failures = entry.get("failed_downloads")
                    if isinstance(failures, dict):
                        failures.pop(slug, None)

        if not prepared_slugs:
            with state.watchlist_lock:
                appconfig.save_watchlist(state.watchlist)
            broadcast({"type": "watchlist_update", **watchlist_payload()})
            return

        with state.watchlist_lock:
            still_pending = {
                slug
                for entry in state.watchlist
                if not entry.get("last_error")
                for slug in state.watchlist_new_slugs.get(entry.get("base_slug", ""), set())
            }
        withdrawn = set(prepared_slugs) - still_pending
        if withdrawn:
            with state.queue_claim_lock:
                state.picked.difference_update(withdrawn)
            prepared_slugs = [slug for slug in prepared_slugs if slug in still_pending]
        if not prepared_slugs:
            return

        _persist_queue_state()
        accepted = _enqueue_automatic_downloads(prepared_slugs)
        if len(accepted) != len(prepared_slugs):
            with state.queue_claim_lock:
                state.picked.difference_update(set(prepared_slugs) - accepted)
            _persist_queue_state()
        with state.watchlist_lock:
            appconfig.save_watchlist(state.watchlist)
        log(f"⬇ Auto-Download: {len(accepted)} neue Episode(n) eingereiht …")
        broadcast({"type": "watchlist_update", **watchlist_payload()})
    except Exception as exc:
        with state.download_state_lock:
            counted = set(state.counted_queue_slugs)
        with state.queue_claim_lock:
            state.picked.difference_update(slug for slug in claimed if slug not in counted)
        _persist_queue_state()
        log(f"Auto-Download konnte nicht eingeplant werden: {exc}", "err")
    finally:
        state.auto_download_lock.release()


WATCHLIST_JELLYFIN_RETRY_SECONDS = 15
WATCHLIST_QUICK_RETRY_ERRORS = (
    "Jellyfin nicht erreichbar",
    "Jellyfin-Serienindex nicht verfügbar",
    "Jellyfin-Benutzerstatus nicht verfügbar",
    "Jellyfin-Konfiguration wird geprüft",
    "Prüfung läuft",
)


def _watchlist_auto_check_once() -> tuple[int, int]:
    with state.watchlist_lock:
        entries = list(state.watchlist)
        before = {slug: set(eps) for slug, eps in state.watchlist_new_slugs.items()}
    if not entries:
        return 0, 0

    checked = check_watchlist_entries(entries, refresh_jellyfin=True)
    with state.watchlist_lock:
        found_new = any(
            state.watchlist_new_slugs.get(slug, set()) - before.get(slug, set())
            for slug in state.watchlist_new_slugs
        )
    broadcast({"type": "watchlist_update", **watchlist_payload()})
    if found_new:
        log("Neue Episode(n) in der Bibliothek verfügbar.")
    _auto_download_new_episodes()
    return checked, len(entries)


def _watchlist_auto_check_delay(checked: int, total: int, interval_min: int) -> int:
    if checked < total:
        with state.watchlist_lock:
            retry_jellyfin = any(
                any(
                    str(entry.get("last_error") or "").startswith(prefix)
                    for prefix in WATCHLIST_QUICK_RETRY_ERRORS
                )
                for entry in state.watchlist
            )
        if retry_jellyfin:
            return WATCHLIST_JELLYFIN_RETRY_SECONDS
    return max(5, int(interval_min)) * 60


def watchlist_auto_check_loop():
    """Prüft abonnierte Serien periodisch im Hintergrund auf neue Episoden,
    pusht das Ergebnis per WebSocket (Glocke) und lädt – falls Auto-Download
    aktiv ist und wir im Zeitfenster sind – die neuen Folgen direkt herunter.
    Das Intervall ist über die Automatik-Einstellungen konfigurierbar."""
    while True:
        interval_min = state.automation.get("check_interval_min", 30)
        checked = total = 0
        try:
            checked, total = _watchlist_auto_check_once()
        except Exception as exc:
            log(f"Automatische Bibliotheks-Prüfung fehlgeschlagen: {exc}", "warn")
        time.sleep(_watchlist_auto_check_delay(checked, total, interval_min))


# ---------------------------------------------------------------------------
# FastAPI-App
# ---------------------------------------------------------------------------
def start_background_services():
    """Startet Server-Hintergrunddienste genau einmal nach dem Setup."""
    global _background_services_started, _recommender_thread, _seerr_thread
    global _updater_thread, _ytdlp_updater_thread
    with _background_services_lock:
        if _background_services_started:
            return
        _background_services_started = True
    threading.Thread(target=warm_home_movie_cache, daemon=True).start()
    threading.Thread(target=watchlist_auto_check_loop, daemon=True).start()
    threading.Thread(target=restore_persisted_queue, daemon=True).start()
    _recommender_stop_event.clear()
    _recommender_wake_event.clear()
    _recommender_thread = threading.Thread(
        target=jellyfin_recommender_loop,
        name="jellyfin-recommender",
        daemon=True,
    )
    _recommender_thread.start()
    _seerr_stop_event.clear()
    _seerr_wake_event.clear()
    _seerr_thread = threading.Thread(
        target=seerr_poll_loop,
        name="seerr-request-bridge",
        daemon=True,
    )
    _seerr_thread.start()
    _updater_stop_event.clear()
    _updater_wake_event.clear()
    _updater_thread = threading.Thread(
        target=automatic_update_loop,
        name="automatic-updater",
        daemon=True,
    )
    _updater_thread.start()
    _ytdlp_updater_stop_event.clear()
    _ytdlp_updater_thread = threading.Thread(
        target=ytdlp_runtime_update_loop,
        name="ytdlp-runtime-updater",
        daemon=True,
    )
    _ytdlp_updater_thread.start()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _main_loop, _telegram_bot
    import asyncio
    _main_loop = asyncio.get_event_loop()
    bind_host = os.environ.get("HOST", "127.0.0.1")
    if bind_host not in ("127.0.0.1", "localhost", "::1") and not AUTH_ENABLED:
        logger.warning(
            "SICHERHEIT: Webserver ist im Netzwerk ohne Anmeldung erreichbar. "
            "APP_USERNAME und APP_PASSWORD setzen."
        )
    removed_staging = await asyncio.to_thread(
        cleanup_stale_staging, [state.save_path, state.series_path], 24 * 60 * 60,
    )
    if removed_staging:
        logger.info("%s altes Staging-Artefakt(e) entfernt.", removed_staging)
    if appconfig.is_initialized():
        start_background_services()
    _telegram_bot = TelegramBot(
        lambda: state.telegram_cfg,
        handle_telegram_message,
        log,
        callback_cb=handle_telegram_callback,
    )
    _telegram_bot.start()
    yield
    _seerr_stop_event.set()
    _seerr_wake_event.set()
    _updater_stop_event.set()
    _updater_wake_event.set()
    _ytdlp_updater_stop_event.set()
    stop_jellyfin_recommender()
    if _telegram_bot is not None:
        _telegram_bot.stop()
    if state.voe_pool is not None:
        try:
            state.voe_pool.close()
        except Exception:
            pass
    if state.embed_pool is not None:
        try:
            state.embed_pool.close()
        except Exception:
            pass
    try:
        if appconfig.is_initialized():
            appconfig.save(state.save_path)
    except Exception:
        pass


app = FastAPI(lifespan=lifespan)


@app.middleware("http")
async def require_basic_auth(request: Request, call_next):
    if request.url.path == "/api/health" or _authorized_header(request.headers.get("authorization", "")):
        return await call_next(request)
    return JSONResponse(
        status_code=401,
        content={"detail": "Anmeldung erforderlich."},
        headers={"WWW-Authenticate": 'Basic realm="Royal Downloader"'},
    )


@app.get("/api/health")
async def api_health():
    return {
        "status": "ok",
        "initialized": appconfig.is_initialized(),
        "queue_active": state.dl_queue.active_count(),
        "queue_pending": state.dl_queue.pending_count(),
    }


@app.exception_handler(Exception)
async def handle_exc(request, exc):
    log(f"Serverfehler: {exc}", "err")
    return JSONResponse(status_code=500, content={"error": str(exc)})


# ── Genres ──────────────────────────────────────────────────────────────────
@app.get("/api/genres")
async def api_genres():
    def _work():
        loaders = {
            "filmfrei24": lambda: FilmFrei24Scraper(progress_cb=log).list_genres(),
            "filmpalast": lambda: get_fp_scraper().list_genres(),
            "moflix": lambda: MoflixScraper(progress_cb=log).list_genres(),
            "einschalten": lambda: EinschaltenScraper(progress_cb=log).list_genres(),
            "kinox": lambda: KinoxScraper(progress_cb=log).list_genres(),
            "kinoger": lambda: KinogerScraper(progress_cb=log).list_genres(),
            "megakino": lambda: MegaKinoScraper(progress_cb=log).list_genres(),
            "xcine": lambda: XcineScraper(progress_cb=log).list_genres(),
            "sflix": lambda: SflixScraper(progress_cb=log).list_genres(),
            "ridomovies": lambda: RidomoviesScraper(progress_cb=log).list_genres(),
        }
        cleaned = {provider: set() for provider in appconfig.MOVIE_PROVIDER_DEFAULTS}
        for provider in provider_priority("movies"):
            try:
                values = loaders[provider]()
            except Exception as exc:
                log(f"{PROVIDER_LABELS[provider]} Genres übersprungen: {exc}", "warn")
                continue
            cleaned[provider] = {
                clean_genre(genre)
                for genre in values
                if clean_genre(genre)
            }
        return cleaned

    provider_genres = await run_in_threadpool(_work)
    ff_c = provider_genres["filmfrei24"]
    fp_c = provider_genres["filmpalast"]
    mx_c = provider_genres["moflix"]
    es_c = provider_genres["einschalten"]
    kx_c = provider_genres["kinox"]
    kg_c = provider_genres["kinoger"]
    mk_c = provider_genres["megakino"]
    xc_c = provider_genres["xcine"]
    sf_c = provider_genres["sflix"]
    rm_c = provider_genres["ridomovies"]
    state.filmfrei24_provider_genres = ff_c
    state.fp_provider_genres = fp_c
    state.moflix_provider_genres = mx_c
    state.einschalten_provider_genres = es_c
    state.kinox_provider_genres = kx_c
    state.kinoger_provider_genres = kg_c
    state.megakino_provider_genres = mk_c
    state.xcine_provider_genres = xc_c
    state.sflix_provider_genres = sf_c
    state.ridomovies_provider_genres = rm_c
    genres = sorted(
        {
            canonical_movie_genre(genre)
            for genre in (
                ff_c | fp_c | mx_c | es_c | kx_c | kg_c | mk_c | xc_c | sf_c
                | rm_c
            )
        },
        key=str.casefold,
    )
    return {"genres": genres}


# ── Filme: Suche / Listen / Genre ───────────────────────────────────────────
@app.get("/api/movies")
async def api_movies(mode: str = "search", query: str = "", genre: str = "", page: int = 1):
    if page < 1 or page > MOVIE_MAX_GLOBAL_PAGE:
        raise HTTPException(400, f"Seite muss zwischen 1 und {MOVIE_MAX_GLOBAL_PAGE} liegen.")

    def _work():
        if mode == "search":
            q = query.strip()
            if not q:
                return {
                    "results": [], "category": None, "page": 1,
                    "has_more": False, "sources": [],
                }
            results = search_movie_candidates(q)
            return {
                "results": results, "category": None, "page": 1,
                "has_more": False, "sources": [],
            }

        category = "genre" if mode == "genre" else mode if mode in {"new", "top"} else "new"
        try:
            catalog = movie_catalog_page(category, page, genre if category == "genre" else "")
        except MovieCatalogColdLoadLimit as exc:
            raise HTTPException(409, str(exc)) from exc
        return {**catalog, "category": category}

    data = await run_in_threadpool(_work)
    result_dicts = [asdict(r) for r in data["results"]]
    jf_items = await run_in_threadpool(get_jellyfin_library)
    with state.jellyfin_cache_lock:
        jf_available = state.jellyfin_library_available
    if jf_items is not None and jf_available:
        jf_client = get_jellyfin_client()
        for rd in result_dicts:
            rd["in_jellyfin"] = jf_client.match(strip_source_suffix(rd["title"]), rd.get("year", ""), items=jf_items)
    return {
        "results": result_dicts,
        "category": data["category"],
        "page": data["page"],
        "has_more": data["has_more"],
        # Rückwärtskompatibel für ältere Web-Builds. Semantisch ist dies jetzt
        # korrekt: Eine weitere globale Seite ist tatsächlich vorhanden.
        "last_page_full": data["has_more"],
        "sources": data["sources"],
    }


@app.get("/api/movie/{slug:path}")
async def api_movie(slug: str):
    def _work():
        movie = state.fp_movies.get(slug)
        if movie is None:
            movie = load_movie_for_slug(slug)
        if movie is not None:
            state.fp_movies[slug] = movie
            return movie_to_dict(movie)
        return None

    payload = await run_in_threadpool(_work)
    if payload is None:
        raise HTTPException(404, "Film nicht gefunden oder kein Hoster.")
    return payload


class PreloadBody(BaseModel):
    slugs: List[str]


@app.post("/api/movies/preload")
async def api_movies_preload(body: PreloadBody):
    def _work():
        payloads = {}
        for slug in body.slugs:
            movie = state.fp_movies.get(slug)
            if movie is None:
                movie = load_movie_for_slug(slug)
            if movie is not None:
                state.fp_movies[slug] = movie
                payloads[slug] = movie_to_dict(movie)
        return payloads

    payloads = await run_in_threadpool(_work)
    return {"movies": payloads}


class MovieMetadataItem(BaseModel):
    slug: str
    title: str
    year: str = ""
    tmdb_id: Optional[int] = None


class MovieMetadataBody(BaseModel):
    items: List[MovieMetadataItem]


@app.post("/api/tmdb/movie")
async def api_tmdb_movie(item: MovieMetadataItem):
    """Vollständige TMDB-Details eines Films – ohne Anbieter-/Hoster-Aufruf."""
    if not get_tmdb_client().configured:
        return {"movie": None}
    title = strip_source_suffix(item.title)
    movie = await run_in_threadpool(get_tmdb_client().movie, title, item.year)
    return {"movie": movie}


@app.post("/api/jellyfin/matches")
async def api_jellyfin_matches(body: MovieMetadataBody):
    """Aktualisiert nur die JF-Badges, ohne Anbieter oder Streams neu zu laden."""
    def _work():
        items = get_jellyfin_library()
        with state.jellyfin_cache_lock:
            library_available = state.jellyfin_library_available
        if items is None or not library_available:
            return {}
        client = get_jellyfin_client()
        return {
            item.slug: client.match(
                strip_source_suffix(item.title), item.year,
                items=items, tmdb_id=item.tmdb_id,
            )
            for item in body.items[:100]
        }
    return {"matches": await run_in_threadpool(_work)}


@app.post("/api/tmdb/movies")
async def api_tmdb_movies(body: MovieMetadataBody):
    """Lädt schnelle TMDB-Listenmetadaten parallel, ohne Hoster-Seiten."""
    if not get_tmdb_client().configured or not body.items:
        return {"movies": {}}

    def _work():
        unique = {}
        for item in body.items[:100]:
            title = strip_source_suffix(item.title)
            key = (_norm_title(title), str(item.year or ""))
            group = unique.setdefault(key, {"title": title, "year": item.year, "slugs": []})
            group["slugs"].append(item.slug)

        result = {}
        groups = list(unique.values())
        with ThreadPoolExecutor(max_workers=min(TMDB_MOVIE_BATCH_MAX_WORKERS, len(groups))) as pool:
            futures = [(group, pool.submit(get_tmdb_client().movie_summary, group["title"], group["year"])) for group in groups]
            for group, future in futures:
                try:
                    metadata = future.result()
                except Exception as exc:
                    log(f"TMDB-Vorladen fehlgeschlagen ({group['title']}): {exc}", "warn")
                    metadata = None
                if metadata:
                    for slug in group["slugs"]:
                        result[slug] = metadata
        return result

    return {"movies": await run_in_threadpool(_work)}


# ── Serien ───────────────────────────────────────────────────────────────────
@app.get("/api/series")
async def api_series(mode: str = "search", query: str = "", letter: str = "", page: int = 1):
    if page < 1 or page > SERIES_MAX_GLOBAL_PAGE:
        raise HTTPException(400, f"Seite muss zwischen 1 und {SERIES_MAX_GLOBAL_PAGE} liegen.")

    def _work():
        if mode == "search":
            q = query.strip()
            if not q:
                return {
                    "entries": [], "direct_series": None, "mode": "search",
                    "page": 1, "has_more": False, "sources": [],
                }
            if q.startswith("http"):
                series = get_series_for_value(q)
                if series is None:
                    return {
                        "entries": [], "direct_series": None, "mode": "search",
                        "page": 1, "has_more": False, "sources": [],
                    }
                stub = FilmpalastSeriesResult(
                    title=series.title, base_slug=series.base_slug,
                    sample_slug=series.all_episodes[0].slug if series.all_episodes else "",
                    sample_url=series.url,
                )
                state.series_cache[series.base_slug] = series
                provider = provider_for_value(stub.sample_slug or stub.base_slug or stub.sample_url)
                entry = _SeriesCatalogEntry(provider, stub, (provider,))
                return {
                    "entries": [entry],
                    "direct_series": series_to_dict(series, defer_checks=True),
                    "mode": "search", "page": 1, "has_more": False,
                    "sources": _series_catalog_sources(
                        [entry], provider_priority("series"),
                    ),
                }
            try:
                catalog = series_search_catalog(q)
            except Exception as exc:
                log(f"Serien-Suche fehlgeschlagen: {exc}", "warn")
                catalog = {
                    "entries": [], "page": 1, "has_more": False, "sources": [],
                }
            return {**catalog, "direct_series": None, "mode": "search"}

        browse_mode = mode if mode in {"discover", "new", "trending", "alpha"} else "discover"
        try:
            catalog = series_catalog_page(browse_mode, page, letter)
        except SeriesCatalogColdLoadLimit as exc:
            raise HTTPException(409, str(exc)) from exc
        return {**catalog, "direct_series": None, "mode": browse_mode}

    data = await run_in_threadpool(_work)
    return {
        "results": [_series_entry_to_dict(entry) for entry in data["entries"]],
        "direct_series": data["direct_series"],
        "mode": data["mode"],
        "page": data["page"],
        "has_more": data["has_more"],
        "last_page_full": data["has_more"],
        "sources": data["sources"],
    }


class SeriesLoadBody(BaseModel):
    sample_slug: str
    base_slug: str = ""
    refresh_jellyfin: bool = False
    defer_checks: bool = False


@app.post("/api/series/load")
async def api_series_load(body: SeriesLoadBody):
    def _work():
        series = state.series_cache.get(body.base_slug) if body.base_slug else None
        if series is None:
            series = get_series_for_value(body.sample_slug)
        if series is None:
            return None, None
        state.series_cache[series.base_slug] = series
        return series, series_to_dict(
            series,
            refresh_jellyfin=body.refresh_jellyfin,
            defer_checks=body.defer_checks,
        )

    series, payload = await run_in_threadpool(_work)
    if series is None:
        raise HTTPException(404, "Serie nicht gefunden.")
    return payload


# ── Anime ───────────────────────────────────────────────────────────────────
@app.get("/api/anime")
async def api_anime(
    mode: str = "latest",
    query: str = "",
    page: int = 1,
):
    if page < 1 or page > 50:
        raise HTTPException(400, "Seite muss zwischen 1 und 50 liegen.")
    if "mkissa" not in provider_priority("anime"):
        return {
            "results": [],
            "mode": mode,
            "page": 1,
            "has_more": False,
            "total": 0,
            "disabled": True,
            "disabled_reason": (
                "MKissa ist pausiert. Aktiviere englische Inhalte und die "
                "Anime-Quelle in den Einstellungen."
            ),
        }
    browse_mode = mode if mode in {"search", "latest", "popular", "trending"} else "latest"
    if browse_mode == "search" and not query.strip():
        return {
            "results": [],
            "mode": browse_mode,
            "page": 1,
            "has_more": False,
            "total": 0,
            "disabled": False,
        }

    def _work():
        with state.mkissa_lock:
            return get_mkissa_scraper().browse(
                mode=browse_mode,
                query=query,
                page=page,
                limit=50,
            )

    try:
        payload = await run_in_threadpool(_work)
    except Exception as exc:
        log(f"MKissa-Katalog fehlgeschlagen: {exc}", "warn")
        raise HTTPException(502, f"MKissa ist gerade nicht erreichbar: {exc}") from exc
    return {
        **payload,
        "mode": browse_mode,
        "disabled": False,
        "provider": "mkissa",
        "provider_label": PROVIDER_LABELS["mkissa"],
        "content_language": provider_content_language("mkissa"),
    }


@app.get("/api/anime/{anime_id}")
async def api_anime_detail(
    anime_id: str,
    translation: str = "",
    episode_page: int = 1,
):
    if "mkissa" not in provider_priority("anime"):
        raise HTTPException(
            409,
            "MKissa ist in den Quellen oder über die Inhaltssprache deaktiviert.",
        )
    requested_track = str(translation or "").strip().casefold()

    def _work():
        with state.mkissa_lock:
            anime = get_mkissa_scraper().get_anime(anime_id)
        available = anime.translations
        track = requested_track if requested_track in available else (
            "dub" if available.get("dub") else
            "sub" if available.get("sub") else
            next(iter(available), "")
        )
        if not track:
            raise LookupError("MKissa meldet keine verfügbaren Episoden.")
        episodes = anime_episode_page(
            anime,
            track,
            page=episode_page,
            page_size=100,
        )
        for episode in episodes["episodes"]:
            slug = episode["slug"]
            episode["queued"] = slug in state.picked
            episode["downloaded"] = bool(
                _existing_valid_episode_path(
                    anime.title,
                    1,
                    int(episode["number"]),
                )
            )
        return {
            **anime.public_dict(),
            "translation": track,
            "translation_labels": {
                "dub": "English Dub",
                "sub": "English Sub",
                "raw": "Japanese Raw",
            },
            **episodes,
        }

    try:
        return await run_in_threadpool(_work)
    except (LookupError, ValueError) as exc:
        raise HTTPException(404, str(exc)) from exc
    except Exception as exc:
        log(f"MKissa-Details fehlgeschlagen: {exc}", "warn")
        raise HTTPException(502, f"MKissa-Details sind nicht verfügbar: {exc}") from exc


# ── Warteschlange ────────────────────────────────────────────────────────────
class _QueuePreparationJob:
    """Löst neu hinzugefügte Inhalte innerhalb derselben 2-Slot-Queue auf.

    Dadurch starten neue Einträge automatisch, ohne neben der bestehenden
    Queue einen zweiten Scheduler oder mehr als zwei parallele Jobs zu öffnen.
    """

    def __init__(
        self, jobs: List[tuple], out_root: Path,
        movie_fallbacks: Optional[Dict[str, List[FilmpalastMovie]]] = None,
    ):
        self.jobs = jobs
        self.out_root = out_root
        self.movie_fallbacks = movie_fallbacks or {}
        self.queue_slugs = {slug for _movie, slug in jobs}
        self.queue_slug = next(iter(self.queue_slugs)) if len(self.queue_slugs) == 1 else ""
        self._cancelled = threading.Event()

    def start(self):
        thread = threading.Thread(target=self._run, daemon=True)
        thread.start()
        return thread

    def cancel(self):
        self._cancelled.set()

    def _run(self):
        queued_slugs: set[str] = set()
        try:
            with state.queue_prepare_lock:
                if self._cancelled.is_set():
                    return
                queued_slugs = run_download_queue(
                    self.jobs,
                    self.out_root,
                    start_queue=False,
                    cancelled=self._cancelled.is_set,
                    movie_fallbacks=self.movie_fallbacks,
                ) or set()
        except Exception as exc:
            log(f"Automatische Downloadvorbereitung fehlgeschlagen: {exc}", "err")
            for movie, slug in self.jobs:
                on_job_done(
                    False, f"Vorbereitung fehlgeschlagen: {exc}",
                    movie.title, Path(""), slug=slug,
                )
        finally:
            if not self._cancelled.is_set():
                for movie, slug in self.jobs:
                    if slug not in queued_slugs and _queue_slug_claimed(slug):
                        on_job_done(
                            False,
                            "Downloadvorbereitung ohne Abschluss beendet",
                            movie.title,
                            Path(""),
                            slug=slug,
                        )
            # Falls während einer laufenden Extraktion abgebrochen wurde, dürfen
            # danach erzeugte echte DownloadJobs nicht liegenbleiben/anlaufen.
            if self._cancelled.is_set():
                remove_pending = getattr(state.dl_queue, "remove_pending", None)
                if remove_pending:
                    remove_pending(
                        lambda job: bool(self.queue_slugs & set(getattr(job, "queue_slugs", [])))
                        or getattr(job, "queue_slug", "") in self.queue_slugs
                    )


def _enqueue_automatic_downloads(
    slugs: List[str],
    movie_fallbacks: Optional[Dict[str, List[FilmpalastMovie]]] = None,
) -> set[str]:
    if UPDATE_INSTALLER.is_active() or state.ytdlp_update_active:
        log("Downloadstart pausiert: Ein Systemupdate läuft.", "warn")
        return set()
    content_keys = {
        slug: queue_content_key(slug, state.fp_movies.get(slug))
        for slug in slugs if slug in state.fp_movies
    }
    with state.queue_lifecycle_lock:
        # Zweite Prüfung unter demselben Lock, den auch der Updater beim Start
        # hält. So kann zwischen Vorprüfung und Queue-Aufbau kein Update starten.
        if UPDATE_INSTALLER.is_active() or state.ytdlp_update_active:
            log("Downloadstart pausiert: Ein Systemupdate läuft.", "warn")
            return set()
        queue_idle = (
            state.dl_queue.active_count() == 0
            and state.dl_queue.pending_count() == 0
        )
        active_slugs = {
            active_slug
            for active_job in state.dl_queue.active_jobs()
            for active_slug in _job_queue_slugs(active_job)
        }
        with state.queue_claim_lock:
            state.queue_content_keys.update(content_keys)
            queue_idle = queue_idle and not state.gated_retry_pending
            with state.download_state_lock:
                if queue_idle:
                    state.total_jobs = 0
                    state.done_jobs = 0
                    state.done_slugs.clear()
                    state.counted_queue_slugs.clear()
                already_counted = set(state.counted_queue_slugs)

            # Ein bereits gezählter oder noch physisch aktiver Slug gehört zu
            # einem älteren/aktiven Queue-Eintrag. Dessen Claim darf beim
            # Bereinigen neu abgelehnter Cross-Provider-Duplikate nicht fallen.
            protected_slugs = already_counted | active_slugs
            retained_key_slugs = protected_slugs | set(content_keys)
            for stale_slug in set(state.queue_content_keys) - retained_key_slugs:
                state.queue_content_keys.pop(stale_slug, None)
            occupied_keys = {
                state.queue_content_keys.get(existing_slug, "")
                for existing_slug in protected_slugs
            }
            occupied_keys.discard("")

            # Claim nach allen langsamen Provider-Aufrufen erneut prüfen. Ein
            # zwischenzeitliches Entfernen oder ein paralleler Trigger darf
            # keinen ungetrackten beziehungsweise doppelten Job starten.
            jobs = []
            for slug in slugs:
                movie = state.fp_movies.get(slug)
                key = content_keys.get(slug, "")
                if (
                    slug not in state.picked
                    or slug in already_counted
                    or slug in active_slugs
                    or movie is None
                    or (not movie.hosters and parse_episode_slug(slug) is None)
                    or (key and key in occupied_keys)
                ):
                    continue
                jobs.append((movie, slug))
                if key:
                    occupied_keys.add(key)

            newly_counted = {slug for _movie, slug in jobs}
            rejected_claims = {
                slug for slug in set(slugs)
                if slug in state.picked
                and slug not in newly_counted
                and slug not in protected_slugs
            }
            state.picked.difference_update(rejected_claims)

            if jobs:
                with state.download_state_lock:
                    state.counted_queue_slugs.update(newly_counted)
                    state.total_jobs += len(newly_counted)
                    done_jobs = state.done_jobs
                    total_jobs = state.total_jobs

                if queue_idle:
                    if state.sto_scraper is not None:
                        state.sto_scraper.reset_gate()
                    state.fallback_series_cache.clear()

                # Ein Vorbereitungsjob pro Inhalt: Dadurch werden signierte Stream-URLs
                # erst kurz vor ihrem echten Queue-Slot extrahiert statt stapelweise.
                for job in jobs:
                    slug = job[1]
                    # Key-Praesenz bedeutet in run_download_queue bewusst:
                    # "alle Katalog-Fallbacks wurden bereits gesucht". Ohne
                    # explizite Map darf deshalb kein leerer Key entstehen.
                    fallbacks = {}
                    if movie_fallbacks is not None and slug in movie_fallbacks:
                        fallbacks[slug] = list(movie_fallbacks[slug])
                    state.dl_queue.add(_QueuePreparationJob(
                        [job], Path(state.save_path), movie_fallbacks=fallbacks,
                    ))
                state.dl_queue.start()

    if rejected_claims:
        _persist_queue_state()
    if not jobs:
        if rejected_claims:
            broadcast({"type": "queue_update", "queue": build_queue_payload()})
        return set()
    log(f"Automatisch eingeplant: {len(jobs)} Download(s) (max. 2 parallel)")
    broadcast({
        "type": "queue_started",
        "added": len(jobs),
        "done_jobs": done_jobs,
        "total_jobs": total_jobs,
        "queue": build_queue_payload(),
    })
    return {slug for _movie, slug in jobs}


def restore_persisted_queue():
    """Stellt nach einem Neustart noch offene Queue-Einträge sicher wieder her."""
    with state.queue_claim_lock:
        unresolved = set(state.picked)
    if not unresolved:
        return
    log(f"Stelle {len(unresolved)} gespeicherte Queue-Einträge wieder her …")
    while unresolved:
        prepared: List[str] = []
        for slug in list(unresolved):
            with state.queue_claim_lock:
                if slug not in state.picked:
                    unresolved.discard(slug)
                    continue
            try:
                try:
                    movie = load_movie_for_slug(slug)
                except Exception:
                    if not parse_episode_slug(slug):
                        raise
                    movie = None
                if movie is None or not movie.hosters:
                    if parse_episode_slug(slug):
                        movie = _episode_placeholder(slug)
                    else:
                        continue
                already, reason = _content_already_available(movie, slug)
                if already and _is_jellyfin_safety_block(reason):
                    continue
                if already:
                    _release_removed_queue_slugs({slug})
                    unresolved.discard(slug)
                    continue
                state.fp_movies[slug] = movie
                prepared.append(slug)
                unresolved.discard(slug)
            except Exception as exc:
                log(f"Queue-Wiederherstellung für «{slug}» wartet: {exc}", "warn")
        if prepared:
            _enqueue_automatic_downloads(prepared)
        if unresolved:
            time.sleep(60)


class QueueAddBody(BaseModel):
    slugs: List[str]


@app.post("/api/queue/add")
async def api_queue_add(body: QueueAddBody):
    def _work():
        added_slugs: List[str] = []
        skipped = 0
        skipped_details: Dict[str, str] = {}
        for slug in body.slugs:
            with state.queue_lifecycle_lock:
                physically_active = any(
                    slug in _job_queue_slugs(job) for job in state.dl_queue.active_jobs()
                )
                with state.queue_claim_lock:
                    if slug in state.picked:
                        skipped += 1
                        skipped_details[slug] = "bereits eingeplant"
                        continue
                    with state.download_state_lock:
                        if slug in state.counted_queue_slugs or physically_active:
                            skipped += 1
                            skipped_details[slug] = "Abbruch läuft noch"
                            continue
                    state.picked.add(slug)
            try:
                movie = state.fp_movies.get(slug)
                if movie is None:
                    try:
                        movie = load_movie_for_slug(slug)
                    except Exception:
                        if not parse_episode_slug(slug):
                            raise
                        movie = None
                if movie is None or not movie.hosters:
                    if parse_episode_slug(slug):
                        movie = _episode_placeholder(slug)
                    else:
                        raise RuntimeError("kein Hoster verfügbar")
                already_available, reason = _content_already_available(movie, slug)
                if already_available:
                    skipped += 1
                    skipped_details[slug] = reason
                    with state.queue_claim_lock:
                        state.picked.discard(slug)
                    continue
                state.fp_movies[slug] = movie
                added_slugs.append(slug)
            except Exception as exc:
                with state.queue_claim_lock:
                    state.picked.discard(slug)
                skipped += 1
                skipped_details[slug] = str(exc)[:180]
        return added_slugs, skipped, skipped_details

    added_slugs, skipped, skipped_details = await run_in_threadpool(_work)
    _persist_queue_state()
    accepted = _enqueue_automatic_downloads(added_slugs)
    duplicate_rejected = set(added_slugs) - accepted
    if len(accepted) < len(added_slugs):
        with state.queue_claim_lock:
            not_started = {
                slug for slug in added_slugs if slug in state.picked and slug not in accepted
            }
            state.picked.difference_update(not_started)
        _persist_queue_state()
        skipped += len(duplicate_rejected)
        for slug in duplicate_rejected:
            skipped_details.setdefault(slug, "gleicher Inhalt bereits eingeplant")
    with state.download_state_lock:
        done_jobs = state.done_jobs
        total_jobs = state.total_jobs
    return {
        "added": len(accepted),
        "skipped": skipped,
        "skipped_details": skipped_details,
        "auto_started": len(accepted),
        "done_jobs": done_jobs,
        "total_jobs": total_jobs,
        "queue": build_queue_payload(),
    }


class QueueRemoveBody(BaseModel):
    slug: str


def _job_queue_slugs(job) -> set[str]:
    slugs = set(getattr(job, "queue_slugs", set()) or set())
    slug = getattr(job, "queue_slug", "")
    if slug:
        slugs.add(slug)
    return slugs


def _drop_queue_claims(slugs: set[str]) -> None:
    if not slugs:
        return
    with state.queue_claim_lock:
        state.picked.difference_update(slugs)
        for slug in slugs:
            state.gated_retry_jobs.pop(slug, None)
        state.gated_retry_slugs.difference_update(slugs)
        state.gated_retry_pending = bool(state.gated_retry_slugs)
    _persist_queue_state()


def _release_removed_queue_slugs(slugs: set[str]) -> None:
    if not slugs:
        return
    with state.queue_lifecycle_lock:
        with state.queue_claim_lock:
            state.picked.difference_update(slugs)
            for slug in slugs:
                state.gated_retry_jobs.pop(slug, None)
            state.gated_retry_slugs.difference_update(slugs)
            state.gated_retry_pending = bool(state.gated_retry_slugs)
            with state.download_state_lock:
                counted = slugs & state.counted_queue_slugs
                state.counted_queue_slugs.difference_update(counted)
                state.total_jobs = max(state.done_jobs, state.total_jobs - len(counted))
            _persist_queue_state()


def _cancel_queue_slugs(slugs: set[str], reason: str) -> None:
    if not slugs:
        return
    with state.queue_lifecycle_lock:
        state.dl_queue.remove_pending(lambda job: bool(slugs & _job_queue_slugs(job)))
        state.dl_queue.cancel_active(lambda job: bool(slugs & _job_queue_slugs(job)))
        state.dl_queue.remove_pending(lambda job: bool(slugs & _job_queue_slugs(job)))
        _release_removed_queue_slugs(slugs)
    for slug in slugs:
        _telegram_terminal_without_job(slug, False, reason, Path(""))
        _seerr_terminal_without_job(slug, False, reason, Path(""))
    broadcast({"type": "queue_update", "queue": build_queue_payload()})


def _cancel_withdrawn_watchlist_slugs(slugs: set[str], reason: str) -> set[str]:
    """Bricht nur Slugs ab, die kein aktueller Abo-Stand mehr benötigt."""
    if not slugs:
        return set()
    # Der Auto-Scheduler darf zwischen Recheck und Abbruch keinen veralteten
    # Snapshot neu einreihen. Die Watchlist bleibt bis nach dem Queue-Abbruch
    # gesperrt, damit ein neuerer Check denselben Slug nicht wieder freigibt.
    with state.auto_download_lock:
        # Globale Reihenfolge: Queue-Lebenszyklus → Claim → Watchlist. Damit
        # bleibt die Entscheidung atomar, ohne mit watchlist_payload()
        # (Claim → Watchlist) eine Lock-Inversion zu erzeugen.
        with state.queue_lifecycle_lock:
            with state.queue_claim_lock:
                with state.watchlist_lock:
                    currently_required = {
                        slug
                        for pending in state.watchlist_new_slugs.values()
                        for slug in pending
                    }
                    cancellable = set(slugs) - currently_required
                    if cancellable:
                        _cancel_queue_slugs(cancellable, reason)
    return cancellable


@app.post("/api/queue/remove")
async def api_queue_remove(body: QueueRemoveBody):
    with state.queue_lifecycle_lock:
        removed = state.dl_queue.remove_pending(lambda job: body.slug in _job_queue_slugs(job))
        active = state.dl_queue.cancel_active(lambda job: body.slug in _job_queue_slugs(job))
        # Ein Vorbereitungsjob kann genau zwischen remove_pending() und
        # cancel_active() noch einen echten Download eingereiht haben.
        removed.extend(
            state.dl_queue.remove_pending(
                lambda job: body.slug in _job_queue_slugs(job)
            )
        )
        # Abbruch konsumiert das logische Abschlusstoken selbst. Ein alter
        # Hoster-Job kann bereits an einen Pending-Fallback übergeben haben und
        # würde dann keinen eigenen Terminalcallback mehr liefern.
        _release_removed_queue_slugs({body.slug})
        removed.extend(
            state.dl_queue.remove_pending(
                lambda job: body.slug in _job_queue_slugs(job)
            )
        )
    _telegram_terminal_without_job(body.slug, False, "Abgebrochen", Path(""))
    _seerr_terminal_without_job(body.slug, False, "Abgebrochen", Path(""))
    broadcast({"type": "queue_update", "queue": build_queue_payload()})
    return {
        "removed": len(removed),
        "cancelled": len(active),
        "queue": build_queue_payload(),
    }


@app.post("/api/queue/clear")
async def api_queue_clear():
    with state.queue_lifecycle_lock:
        removed = state.dl_queue.remove_pending(lambda _job: True)
        removed_slugs = {slug for job in removed for slug in _job_queue_slugs(job)}
        active_slugs = {
            slug for job in state.dl_queue.active_jobs() for slug in _job_queue_slugs(job)
        }
        with state.queue_claim_lock:
            removed_slugs.update(state.picked - active_slugs)
        _release_removed_queue_slugs(removed_slugs)
    for slug in removed_slugs:
        _telegram_terminal_without_job(slug, False, "Abgebrochen", Path(""))
        _seerr_terminal_without_job(slug, False, "Abgebrochen", Path(""))
    broadcast({"type": "queue_update", "queue": build_queue_payload()})
    return {"removed": len(removed_slugs), "queue": build_queue_payload()}


@app.get("/api/queue")
async def api_queue_get():
    return {"queue": build_queue_payload()}


# ── Downloads ────────────────────────────────────────────────────────────────
@app.post("/api/download/cancel")
async def api_download_cancel():
    with state.queue_lifecycle_lock:
        had_queue_activity = bool(
            state.dl_queue.active_count() or state.dl_queue.pending_count()
        )
        state.dl_queue.cancel_all()
        with state.queue_claim_lock:
            with state.download_state_lock:
                cancelled_slugs = set(state.picked) | set(state.counted_queue_slugs)
                refresh_partial_success = bool(had_queue_activity and state.done_slugs)
            state.picked.clear()
            state.gated_retry_jobs.clear()
            state.gated_retry_slugs.clear()
            state.gated_retry_pending = False
            with state.download_state_lock:
                state.counted_queue_slugs.clear()
                state.total_jobs = state.done_jobs
            _persist_queue_state()
        with state.hoster_extract_lock:
            if state.voe_pool is not None:
                try:
                    state.voe_pool.close()
                except Exception:
                    pass
                state.voe_pool = None
            if state.embed_pool is not None:
                try:
                    state.embed_pool.close()
                except Exception:
                    pass
                state.embed_pool = None
    for slug in cancelled_slugs:
        _telegram_terminal_without_job(slug, False, "Abgebrochen", Path(""))
        _seerr_terminal_without_job(slug, False, "Abgebrochen", Path(""))
    broadcast({"type": "queue_update", "queue": build_queue_payload()})
    log("Download abgebrochen.")
    if refresh_partial_success:
        threading.Thread(target=refresh_jellyfin_after_download, daemon=True).start()
    return {"cancelled": True, "queue": build_queue_payload()}


# ── Einstellungen ────────────────────────────────────────────────────────────


@app.get("/api/updater/status")
async def api_updater_status(force: bool = False):
    payload = await run_in_threadpool(UPDATE_CHECKER.check, force)
    payload["installer"] = UPDATE_INSTALLER.status()
    payload["config"] = _updater_config_payload()
    return payload


class UpdateInstallBody(BaseModel):
    target_sha: str


@app.post("/api/updater/install")
async def api_updater_install(body: UpdateInstallBody):
    update = await run_in_threadpool(UPDATE_CHECKER.check, True)
    target_sha = str(update.get("latest_sha") or "")
    if not target_sha or target_sha != body.target_sha.strip():
        raise HTTPException(409, "Der angebotene GitHub-Stand hat sich geändert; bitte erneut prüfen.")
    if update.get("update_available") is not True:
        raise HTTPException(409, "Für diesen Build ist kein installierbares Update verfügbar.")
    try:
        installer = _start_update_when_idle(target_sha)
    except (RuntimeError, ValueError) as exc:
        raise HTTPException(409, str(exc)) from exc
    return {"installer": installer}


@app.get("/api/updater/install/status")
async def api_updater_install_status():
    return {"installer": UPDATE_INSTALLER.status()}


class UpdaterConfigBody(BaseModel):
    update_mode: str = appconfig.UPDATE_MODE_MANUAL
    auto_update_interval_hours: int = 6


@app.get("/api/updater/config")
async def api_updater_config_get():
    return _updater_config_payload()


@app.post("/api/updater/config")
async def api_updater_config_set(body: UpdaterConfigBody):
    mode = str(body.update_mode or "").strip().lower()
    if mode not in appconfig.UPDATE_MODES:
        raise HTTPException(400, "Update-Modus muss 'manual' oder 'automatic' sein.")
    interval = max(1, min(168, int(body.auto_update_interval_hours or 6)))
    if not appconfig.save_updater(mode, interval):
        raise HTTPException(500, "Update-Einstellungen konnten nicht gespeichert werden.")
    with state.updater_config_lock:
        state.updater_cfg = appconfig.load_updater()
    if mode == appconfig.UPDATE_MODE_AUTOMATIC:
        _set_updater_runtime("scheduled", "Automatische Updateprüfung wird gestartet.")
    else:
        _set_updater_runtime("manual", "Updates werden nur manuell installiert.")
    _updater_wake_event.set()
    return {**_updater_config_payload(), "saved": True}


class SetupCompleteBody(BaseModel):
    save_path: str
    series_path: str = ""
    ui_language: str = "de"
    jellyfin_url: str = ""
    jellyfin_api_key: str = ""
    jellyfin_user_id: str = ""
    jellyfin_user_name: str = ""
    tmdb_api_key: str = ""
    telegram_enabled: bool = False
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    auto_download: bool = False
    check_interval_min: int = 30
    dl_window_start: Optional[int] = None
    dl_window_end: Optional[int] = None
    movie_provider_order: Optional[List[str]] = None
    series_provider_order: Optional[List[str]] = None
    anime_provider_order: Optional[List[str]] = None
    movie_providers: Optional[List[str]] = None
    series_providers: Optional[List[str]] = None
    anime_providers: Optional[List[str]] = None
    content_languages: Optional[List[str]] = None


def _prepare_media_directory(raw_path: str, label: str) -> dict:
    path = Path(raw_path).expanduser()
    try:
        path.mkdir(parents=True, exist_ok=True)
        if not path.is_dir():
            raise OSError("Pfad ist kein Ordner")
        with tempfile.NamedTemporaryFile(prefix=".royal-write-test-", dir=path, delete=True) as probe:
            probe.write(b"ok")
            probe.flush()
            os.fsync(probe.fileno())
        usage = shutil.disk_usage(path)
    except OSError as exc:
        raise HTTPException(400, f"{label} ist nicht beschreibbar: {exc}") from exc
    if usage.free < 512 * 1024 * 1024:
        raise HTTPException(400, f"{label} hat weniger als 512 MB freien Speicher.")
    return {"path": str(path), "free": usage.free}


@app.get("/api/setup/status")
async def api_setup_status():
    return {
        "required": not appconfig.is_initialized(),
        "config_path": str(appconfig.config_path()),
        "defaults": {
            "save_path": state.save_path,
            "series_path": state.series_path,
            "ui_language": state.ui_language,
            "ui_language_configured": appconfig.ui_language_configured(),
            "providers": _provider_priority_payload(),
            "jellyfin": {
                "url": state.jellyfin_cfg.get("url", ""),
                "api_key": "",
                "has_api_key": bool(state.jellyfin_cfg.get("api_key")),
                "user_id": state.jellyfin_cfg.get("user_id", ""),
                "user_name": state.jellyfin_cfg.get("user_name", ""),
                "cleanup_default": normalize_cleanup_mode(
                    state.jellyfin_cfg.get("cleanup_default")
                ),
            },
            "tmdb": {
                "api_key": "",
                "has_api_key": bool(state.tmdb_cfg.get("api_key")),
                "language": state.tmdb_cfg.get("language", "de-DE"),
            },
            "telegram": {
                "enabled": bool(state.telegram_cfg.get("enabled")),
                "bot_token": "",
                "has_bot_token": bool(state.telegram_cfg.get("bot_token")),
                "chat_id": state.telegram_cfg.get("chat_id", ""),
            },
            "automation": state.automation,
        },
    }


@app.post("/api/setup/complete")
async def api_setup_complete(body: SetupCompleteBody):
    movie_path = body.save_path.strip()
    series_path = body.series_path.strip() or movie_path
    jellyfin_url = body.jellyfin_url.strip()
    with state.jellyfin_cache_lock:
        previous_jellyfin = dict(state.jellyfin_cfg)
    same_jellyfin = (
        jellyfin_url.rstrip("/")
        and jellyfin_url.rstrip("/") == previous_jellyfin.get("url", "").rstrip("/")
    )
    jellyfin_api_key = body.jellyfin_api_key.strip() or (
        previous_jellyfin.get("api_key", "") if same_jellyfin else ""
    )
    jellyfin_user_id = body.jellyfin_user_id.strip()
    jellyfin_user_name = body.jellyfin_user_name.strip()
    movie_order = (
        [str(value).strip().casefold() for value in body.movie_provider_order]
        if body.movie_provider_order is not None
        else provider_order("movies")
    )
    series_order = (
        [str(value).strip().casefold() for value in body.series_provider_order]
        if body.series_provider_order is not None
        else provider_order("series")
    )
    anime_order = (
        [str(value).strip().casefold() for value in body.anime_provider_order]
        if body.anime_provider_order is not None
        else provider_order("anime")
    )
    movie_providers = (
        [str(value).strip().casefold() for value in body.movie_providers]
        if body.movie_providers is not None
        else list(state.provider_enabled.get("movies", appconfig.MOVIE_PROVIDER_DEFAULTS))
    )
    series_providers = (
        [str(value).strip().casefold() for value in body.series_providers]
        if body.series_providers is not None
        else list(state.provider_enabled.get("series", appconfig.SERIES_PROVIDER_DEFAULTS))
    )
    anime_providers = (
        [str(value).strip().casefold() for value in body.anime_providers]
        if body.anime_providers is not None
        else list(state.provider_enabled.get("anime", appconfig.ANIME_PROVIDER_DEFAULTS))
    )
    content_languages = (
        appconfig.normalize_content_languages(body.content_languages)
        if body.content_languages is not None
        else list(state.content_languages)
    )
    if not movie_path:
        raise HTTPException(400, "Ein Speicherordner für Filme fehlt.")
    if (
        len(movie_order) != len(set(movie_order))
        or set(movie_order) != set(appconfig.MOVIE_PROVIDER_DEFAULTS)
    ):
        raise HTTPException(400, "Die Reihenfolge der Filmquellen ist ungültig.")
    if (
        len(series_order) != len(set(series_order))
        or set(series_order) != set(appconfig.SERIES_PROVIDER_DEFAULTS)
    ):
        raise HTTPException(400, "Die Reihenfolge der Serienquellen ist ungültig.")
    if (
        len(anime_order) != len(set(anime_order))
        or set(anime_order) != set(appconfig.ANIME_PROVIDER_DEFAULTS)
    ):
        raise HTTPException(400, "Die Reihenfolge der Anime-Quellen ist ungültig.")
    if (
        not movie_providers
        or len(movie_providers) != len(set(movie_providers))
        or not set(movie_providers).issubset(appconfig.MOVIE_PROVIDER_DEFAULTS)
    ):
        raise HTTPException(400, "Mindestens eine gültige Filmquelle muss aktiv sein.")
    if (
        not series_providers
        or len(series_providers) != len(set(series_providers))
        or not set(series_providers).issubset(appconfig.SERIES_PROVIDER_DEFAULTS)
    ):
        raise HTTPException(400, "Mindestens eine gültige Serienquelle muss aktiv sein.")
    if (
        len(anime_providers) != len(set(anime_providers))
        or not set(anime_providers).issubset(appconfig.ANIME_PROVIDER_DEFAULTS)
    ):
        raise HTTPException(400, "Die Auswahl der Anime-Quellen ist ungültig.")
    if not content_languages:
        raise HTTPException(400, "Mindestens eine Inhaltssprache muss aktiv sein.")
    if any(
        provider_content_language(provider) not in content_languages
        for provider in movie_providers + series_providers + anime_providers
    ):
        raise HTTPException(400, "Aktive Quellen und Inhaltssprachen passen nicht zusammen.")
    if jellyfin_url and not jellyfin_api_key:
        raise HTTPException(400, "Für Jellyfin fehlt der API-Schlüssel.")
    if jellyfin_url:
        users = await run_in_threadpool(JellyfinClient(jellyfin_url, jellyfin_api_key).list_users)
        if users is None:
            raise HTTPException(502, "Jellyfin ist nicht erreichbar; Einstellungen wurden nicht gespeichert.")
        if jellyfin_user_id:
            selected = next((user for user in users if user["id"] == jellyfin_user_id), None)
            if selected is None:
                raise HTTPException(400, "Der gewählte Jellyfin-Benutzer ist nicht verfügbar.")
            jellyfin_user_name = selected["name"]
    if body.telegram_enabled and not (body.telegram_bot_token.strip() or state.telegram_cfg.get("bot_token", "")):
        raise HTTPException(400, "Für Telegram fehlt der Bot-Token.")
    for value, label in ((movie_path, "Filmordner"), (series_path, "Serienordner")):
        await run_in_threadpool(_prepare_media_directory, value, label)

    ok = await run_in_threadpool(
        appconfig.save_initial_setup,
        movie_path,
        series_path,
        jellyfin_url,
        jellyfin_api_key,
        jellyfin_user_id,
        jellyfin_user_name,
        body.tmdb_api_key or state.tmdb_cfg.get("api_key", ""),
        body.telegram_enabled,
        body.telegram_bot_token or state.telegram_cfg.get("bot_token", ""),
        body.telegram_chat_id,
        body.auto_download,
        body.check_interval_min,
        body.dl_window_start,
        body.dl_window_end,
        body.ui_language,
        movie_order,
        series_order,
        movie_providers,
        series_providers,
        content_languages,
        anime_order,
        anime_providers,
    )
    if not ok:
        raise HTTPException(500, f"Einstellungen konnten nicht unter {appconfig.config_path()} gespeichert werden.")

    state.save_path = appconfig.load()
    state.series_path = appconfig.load_series_path()
    with state.ui_language_lock:
        state.ui_language = appconfig.load_ui_language()
    with state.provider_priority_lock:
        state.provider_priorities = appconfig.load_provider_priorities()
        state.provider_enabled = appconfig.load_provider_enabled()
        state.content_languages = set(appconfig.load_content_languages())
    _set_runtime_jellyfin_config(appconfig.load_jellyfin())
    state.tmdb_cfg = appconfig.load_tmdb()
    state.tmdb_client = TMDBClient(**state.tmdb_cfg)
    state.telegram_cfg = appconfig.load_telegram()
    state.automation = appconfig.load_automation()
    start_background_services()
    return {
        "saved": True,
        "required": False,
        "config_path": str(appconfig.config_path()),
        "save_path": state.save_path,
        "series_path": state.series_path,
        "ui_language": state.ui_language,
    }


class UILanguageBody(BaseModel):
    language: str = "de"


class UITranslationBody(BaseModel):
    target_language: str
    texts: List[str]


def _ui_language_payload(saved: bool = False) -> dict:
    with state.ui_language_lock:
        language = state.ui_language
    return {
        "language": language,
        "configured": appconfig.ui_language_configured(),
        "languages": SUPPORTED_UI_LANGUAGES,
        "translator": {
            "browser_preferred": True,
            "fallback_engine": UI_TRANSLATOR.engine,
        },
        "saved": saved,
    }


@app.get("/api/ui/config")
async def api_ui_config_get():
    return _ui_language_payload()


@app.post("/api/ui/config")
async def api_ui_config_set(body: UILanguageBody):
    language = normalize_ui_language(body.language)
    if not appconfig.save_ui_language(language):
        raise HTTPException(500, "Die Sprache konnte nicht gespeichert werden.")
    with state.ui_language_lock:
        state.ui_language = language
    return _ui_language_payload(saved=True)


@app.post("/api/ui/translate")
async def api_ui_translate(body: UITranslationBody):
    target = normalize_ui_language(body.target_language)
    texts = [str(value or "") for value in body.texts]
    requested = str(body.target_language or "").strip().replace("_", "-").casefold()
    if target != requested.split("-", 1)[0]:
        raise HTTPException(400, "Nicht unterstützte Zielsprache.")
    if len(texts) > 120:
        raise HTTPException(400, "Pro Anfrage sind höchstens 120 Texte erlaubt.")
    if any(len(text) > 600 for text in texts) or sum(map(len, texts)) > 30_000:
        raise HTTPException(400, "Die Übersetzungsanfrage ist zu groß.")
    translated = await run_in_threadpool(
        UI_TRANSLATOR.translate_many,
        texts,
        target,
    )
    return {
        "source_language": "de",
        "target_language": target,
        "translations": translated,
        "engine": UI_TRANSLATOR.engine,
    }


class ConfigBody(BaseModel):
    save_path: str
    series_path: Optional[str] = None


@app.get("/api/config")
async def api_config_get():
    return {"save_path": state.save_path, "series_path": state.series_path}


@app.post("/api/config")
async def api_config_set(body: ConfigBody):
    movie_path = body.save_path.strip()
    series = (body.series_path or "").strip() or movie_path
    if not movie_path:
        raise HTTPException(400, "Ein Speicherordner für Filme fehlt.")
    await run_in_threadpool(_prepare_media_directory, movie_path, "Filmordner")
    await run_in_threadpool(_prepare_media_directory, series, "Serienordner")
    ok = appconfig.save(movie_path)
    # Serien-Pfad optional: leer/None -> gleicher Ordner wie Filme (Fallback).
    ok_series = appconfig.save_series_path(series)
    if not (ok and ok_series):
        raise HTTPException(500, "Speicherorte konnten nicht gespeichert werden.")
    state.save_path = movie_path
    state.series_path = appconfig.load_series_path()
    return {"save_path": state.save_path, "series_path": state.series_path, "saved": True}


class ProviderPriorityBody(BaseModel):
    movies: List[str]
    series: List[str]
    anime: Optional[List[str]] = None
    enabled_movies: Optional[List[str]] = None
    enabled_series: Optional[List[str]] = None
    enabled_anime: Optional[List[str]] = None
    content_languages: Optional[List[str]] = None


def _provider_priority_payload(saved: bool = False) -> dict:
    movie_order = provider_order("movies")
    series_order = provider_order("series")
    anime_order = provider_order("anime")
    with state.provider_priority_lock:
        enabled_movie_ids = set(state.provider_enabled.get(
            "movies", appconfig.MOVIE_PROVIDER_DEFAULTS,
        ))
        enabled_series_ids = set(state.provider_enabled.get(
            "series", appconfig.SERIES_PROVIDER_DEFAULTS,
        ))
        enabled_anime_ids = set(state.provider_enabled.get(
            "anime", appconfig.ANIME_PROVIDER_DEFAULTS,
        ))
        content_languages = set(state.content_languages)
    return {
        "movies": movie_order,
        "series": series_order,
        "anime": anime_order,
        "enabled_movies": [
            provider for provider in movie_order if provider in enabled_movie_ids
        ],
        "enabled_series": [
            provider for provider in series_order if provider in enabled_series_ids
        ],
        "enabled_anime": [
            provider for provider in anime_order if provider in enabled_anime_ids
        ],
        "labels": PROVIDER_LABELS,
        "catalog": provider_catalog_payload(),
        "content_languages": [
            language
            for language in appconfig.CONTENT_LANGUAGE_DEFAULTS
            if language in content_languages
        ],
        "languages": provider_language_payload(),
        "saved": saved,
    }


@app.get("/api/providers/config")
async def api_provider_priority_get():
    return _provider_priority_payload()


@app.post("/api/providers/config")
async def api_provider_priority_set(body: ProviderPriorityBody):
    movie_ids = [str(value).strip().casefold() for value in body.movies]
    series_ids = [str(value).strip().casefold() for value in body.series]
    anime_ids = (
        [str(value).strip().casefold() for value in body.anime]
        if body.anime is not None
        else provider_order("anime")
    )
    if len(movie_ids) != len(set(movie_ids)) or set(movie_ids) != set(appconfig.MOVIE_PROVIDER_DEFAULTS):
        raise HTTPException(400, "Die Film-Anbieterliste ist unvollständig oder ungültig.")
    if len(series_ids) != len(set(series_ids)) or set(series_ids) != set(appconfig.SERIES_PROVIDER_DEFAULTS):
        raise HTTPException(400, "Die Serien-Anbieterliste ist unvollständig oder ungültig.")
    if len(anime_ids) != len(set(anime_ids)) or set(anime_ids) != set(appconfig.ANIME_PROVIDER_DEFAULTS):
        raise HTTPException(400, "Die Anime-Anbieterliste ist unvollständig oder ungültig.")
    current_enabled = appconfig.load_provider_enabled()
    enabled_movies = [
        str(value).strip().casefold()
        for value in (
            body.enabled_movies
            if body.enabled_movies is not None
            else current_enabled["movies"]
        )
    ]
    enabled_series = [
        str(value).strip().casefold()
        for value in (
            body.enabled_series
            if body.enabled_series is not None
            else current_enabled["series"]
        )
    ]
    enabled_anime = [
        str(value).strip().casefold()
        for value in (
            body.enabled_anime
            if body.enabled_anime is not None
            else current_enabled["anime"]
        )
    ]
    content_languages = (
        appconfig.normalize_content_languages(body.content_languages)
        if body.content_languages is not None
        else appconfig.load_content_languages()
    )
    if (
        not enabled_movies
        or len(enabled_movies) != len(set(enabled_movies))
        or not set(enabled_movies).issubset(appconfig.MOVIE_PROVIDER_DEFAULTS)
    ):
        raise HTTPException(400, "Mindestens eine gültige Filmquelle muss aktiv sein.")
    if (
        not enabled_series
        or len(enabled_series) != len(set(enabled_series))
        or not set(enabled_series).issubset(appconfig.SERIES_PROVIDER_DEFAULTS)
    ):
        raise HTTPException(400, "Mindestens eine gültige Serienquelle muss aktiv sein.")
    if (
        len(enabled_anime) != len(set(enabled_anime))
        or not set(enabled_anime).issubset(appconfig.ANIME_PROVIDER_DEFAULTS)
    ):
        raise HTTPException(400, "Die Auswahl der Anime-Quellen ist ungültig.")
    if not content_languages:
        raise HTTPException(400, "Mindestens eine Inhaltssprache muss aktiv sein.")
    if any(
        provider_content_language(provider) not in content_languages
        for provider in enabled_movies + enabled_series + enabled_anime
    ):
        raise HTTPException(400, "Aktive Quellen und Inhaltssprachen passen nicht zusammen.")
    if not appconfig.save_provider_priorities(
        movie_ids,
        series_ids,
        enabled_movies,
        enabled_series,
        content_languages=content_languages,
        anime=anime_ids,
        enabled_anime=enabled_anime,
    ):
        raise HTTPException(500, "Anbieter-Prioritäten konnten nicht gespeichert werden.")
    with state.provider_priority_lock:
        state.provider_priorities = appconfig.load_provider_priorities()
        state.provider_enabled = appconfig.load_provider_enabled()
        state.content_languages = set(appconfig.load_content_languages())
    with state.movie_list_cache_lock:
        state.movie_list_cache.clear()
    with state.series_list_cache_lock:
        state.series_list_cache.clear()
    state.fallback_series_cache.clear()
    return _provider_priority_payload(saved=True)


class JellyfinConfigBody(BaseModel):
    url: str
    api_key: str
    user_id: str = ""
    user_name: str = ""
    cleanup_default: Optional[str] = None


@app.get("/api/jellyfin/config")
async def api_jellyfin_config_get():
    return {
        "url": state.jellyfin_cfg.get("url", ""),
        "api_key": "",
        "has_api_key": bool(state.jellyfin_cfg.get("api_key")),
        "user_id": state.jellyfin_cfg.get("user_id", ""),
        "user_name": state.jellyfin_cfg.get("user_name", ""),
        "cleanup_default": normalize_cleanup_mode(
            state.jellyfin_cfg.get("cleanup_default")
        ),
    }


@app.post("/api/jellyfin/config")
async def api_jellyfin_config_set(body: JellyfinConfigBody):
    url = body.url.strip()
    with state.jellyfin_cache_lock:
        previous = dict(state.jellyfin_cfg)
    same_server = bool(url) and url.rstrip("/") == previous.get("url", "").rstrip("/")
    api_key = body.api_key.strip() or (previous.get("api_key", "") if same_server else "")
    user_id = body.user_id.strip()
    user_name = body.user_name.strip()
    if body.cleanup_default is not None and body.cleanup_default not in CLEANUP_MODE_LABELS:
        raise HTTPException(400, "Unbekannte Standard-Löschregel.")
    cleanup_default = normalize_cleanup_mode(
        body.cleanup_default
        if body.cleanup_default is not None
        else previous.get("cleanup_default")
    )
    if url and not api_key:
        raise HTTPException(400, "Für Jellyfin fehlt der API-Schlüssel.")
    if url and api_key:
        users = await run_in_threadpool(JellyfinClient(url, api_key).list_users)
        if users is None:
            raise HTTPException(502, "Jellyfin ist nicht erreichbar; Einstellungen wurden nicht geändert.")
        if user_id:
            selected = next((user for user in users if user["id"] == user_id), None)
            if selected is None:
                raise HTTPException(400, "Der gewählte Jellyfin-Benutzer ist nicht verfügbar.")
            user_name = selected["name"]
    with state.jellyfin_config_update_lock:
        ok = appconfig.save_jellyfin(
            url, api_key, user_id, user_name, cleanup_default,
        )
        if not ok:
            raise HTTPException(500, "Jellyfin-Einstellungen konnten nicht gespeichert werden.")
        _set_runtime_jellyfin_config({
            "url": url,
            "api_key": api_key,
            "user_id": user_id,
            "user_name": user_name,
            "cleanup_default": cleanup_default,
        })
        _recommender_wake_event.set()

    def _recheck():
        with state.watchlist_lock:
            entries = list(state.watchlist)
        check_watchlist_entries(entries, refresh_jellyfin=True)
        broadcast({"type": "jellyfin_update", **watchlist_payload()})
        _auto_download_new_episodes()

    threading.Thread(target=_recheck, daemon=True).start()
    return {
        "url": url,
        "api_key": "",
        "has_api_key": bool(api_key),
        "user_id": user_id,
        "user_name": user_name,
        "cleanup_default": cleanup_default,
        "saved": True,
    }


class JellyfinUsersBody(BaseModel):
    url: str
    api_key: str


@app.post("/api/jellyfin/users")
async def api_jellyfin_users(body: JellyfinUsersBody):
    url = body.url.strip() or state.jellyfin_cfg.get("url", "")
    key = body.api_key.strip()
    if not key and url.rstrip("/") == state.jellyfin_cfg.get("url", "").rstrip("/"):
        key = state.jellyfin_cfg.get("api_key", "")
    client = JellyfinClient(url, key)
    if not client.configured:
        raise HTTPException(400, "Jellyfin-Adresse oder API-Schlüssel fehlt.")
    users = await run_in_threadpool(client.list_users)
    if users is None:
        raise HTTPException(502, "Jellyfin-Benutzer konnten nicht geladen werden.")
    return {"users": users}


class TMDBConfigBody(BaseModel):
    api_key: str = ""
    language: str = "de-DE"


@app.get("/api/tmdb/config")
async def api_tmdb_config_get():
    return {
        "api_key": "",
        "has_api_key": bool(state.tmdb_cfg.get("api_key")),
        "language": state.tmdb_cfg.get("language", "de-DE"),
        "configured": bool(state.tmdb_cfg.get("api_key")),
    }


@app.post("/api/tmdb/config")
async def api_tmdb_config_set(body: TMDBConfigBody):
    language = (body.language or "de-DE").strip()
    api_key = body.api_key.strip() or state.tmdb_cfg.get("api_key", "")
    ok = appconfig.save_tmdb(api_key, language)
    if not ok:
        raise HTTPException(500, "TMDB-Einstellungen konnten nicht gespeichert werden.")
    state.tmdb_cfg = appconfig.load_tmdb()
    state.tmdb_client = TMDBClient(**state.tmdb_cfg)
    valid = await run_in_threadpool(state.tmdb_client.validate) if api_key else False
    return {
        "api_key": "",
        "has_api_key": bool(api_key),
        "language": language,
        "configured": bool(api_key),
        "valid": valid,
        "saved": True,
    }


class AutomationConfigBody(BaseModel):
    auto_download: bool = False
    check_interval_min: int = 30
    dl_window_start: Optional[int] = None
    dl_window_end: Optional[int] = None


@app.get("/api/automation/config")
async def api_automation_config_get():
    return {**state.automation, "in_window": is_within_download_window()}


@app.post("/api/automation/config")
async def api_automation_config_set(body: AutomationConfigBody):
    ok = appconfig.save_automation(
        body.auto_download, body.check_interval_min,
        body.dl_window_start, body.dl_window_end,
    )
    if not ok:
        raise HTTPException(500, "Automatik-Einstellungen konnten nicht gespeichert werden.")
    state.automation = appconfig.load_automation()
    if state.automation.get("auto_download"):
        threading.Thread(target=_auto_download_new_episodes, daemon=True).start()
    return {**state.automation, "in_window": is_within_download_window(), "saved": True}


class TelegramConfigBody(BaseModel):
    enabled: bool = False
    bot_token: str = ""
    chat_id: str = ""


@app.get("/api/telegram/config")
async def api_telegram_config_get():
    return {
        "enabled": bool(state.telegram_cfg.get("enabled")),
        "bot_token": "",
        "has_bot_token": bool(state.telegram_cfg.get("bot_token")),
        "chat_id": state.telegram_cfg.get("chat_id", ""),
    }


@app.post("/api/telegram/config")
async def api_telegram_config_set(body: TelegramConfigBody):
    token = body.bot_token.strip() or state.telegram_cfg.get("bot_token", "")
    if body.enabled and not token:
        raise HTTPException(400, "Für Telegram fehlt der Bot-Token.")
    ok = appconfig.save_telegram(body.enabled, token, body.chat_id)
    if not ok:
        raise HTTPException(500, "Telegram-Einstellungen konnten nicht gespeichert werden.")
    state.telegram_cfg = appconfig.load_telegram()
    return {
        "enabled": bool(state.telegram_cfg.get("enabled")),
        "bot_token": "",
        "has_bot_token": bool(token),
        "chat_id": state.telegram_cfg.get("chat_id", ""),
        "saved": True,
    }


class SeerrConfigBody(BaseModel):
    enabled: bool = False
    url: str = ""
    api_key: str = ""
    poll_interval_seconds: int = 60


def _seerr_config_payload() -> dict:
    with state.seerr_requests_lock:
        records = list(state.seerr_requests.values())
    counts: Dict[str, int] = {}
    for record in records:
        status = str(record.get("status") or "unknown")
        counts[status] = counts.get(status, 0) + 1
    return {
        "enabled": bool(state.seerr_cfg.get("enabled")),
        "url": state.seerr_cfg.get("url", ""),
        "api_key": "",
        "has_api_key": bool(state.seerr_cfg.get("api_key")),
        "poll_interval_seconds": int(state.seerr_cfg.get("poll_interval_seconds", 60)),
        "connected": bool(state.seerr_last_success and not state.seerr_last_error),
        "last_poll": state.seerr_last_poll or None,
        "last_success": state.seerr_last_success or None,
        "last_error": state.seerr_last_error,
        "moonfin_configured": state.seerr_moonfin_configured,
        "moonfin_error": state.seerr_moonfin_error,
        "requests": counts,
    }


@app.get("/api/seerr/config")
async def api_seerr_config_get():
    return _seerr_config_payload()


@app.post("/api/seerr/config")
async def api_seerr_config_set(body: SeerrConfigBody):
    url = body.url.strip().rstrip("/")
    previous = dict(state.seerr_cfg)
    same_server = bool(url) and url.casefold() == str(previous.get("url") or "").rstrip("/").casefold()
    api_key = body.api_key.strip() or (previous.get("api_key", "") if same_server else "")
    interval = max(15, min(3600, int(body.poll_interval_seconds or 60)))
    if body.enabled and (not url or not api_key):
        raise HTTPException(400, "Für Seerr fehlen URL oder API-Schlüssel.")
    if body.enabled:
        valid = await run_in_threadpool(SeerrClient(url, api_key).test_connection)
        if not valid:
            raise HTTPException(
                502,
                "Seerr ist nicht erreichbar oder der API-Schlüssel ist ungültig; Einstellungen wurden nicht geändert.",
            )
    if not appconfig.save_seerr(body.enabled, url, api_key, interval):
        raise HTTPException(500, "Seerr-Einstellungen konnten nicht gespeichert werden.")
    state.seerr_cfg = appconfig.load_seerr()
    state.seerr_last_error = ""
    if url:
        moonfin = await run_in_threadpool(configure_moonfin_seerr, url, body.enabled)
        state.seerr_moonfin_configured = bool(moonfin.get("configured"))
        state.seerr_moonfin_error = "" if state.seerr_moonfin_configured else str(moonfin.get("detail") or "")
    _seerr_wake_event.set()
    payload = _seerr_config_payload()
    payload["saved"] = True
    return payload


@app.post("/api/seerr/sync")
async def api_seerr_sync():
    result = await run_in_threadpool(seerr_poll_once)
    if not result.get("ok"):
        raise HTTPException(502, result.get("detail") or "Seerr-Abgleich fehlgeschlagen.")
    return {**result, **_seerr_config_payload()}


@app.get("/api/seerr/requests")
async def api_seerr_requests():
    with state.seerr_requests_lock:
        records = [dict(record) for record in state.seerr_requests.values()]
    records.sort(key=lambda record: float(record.get("updated_at", 0) or 0), reverse=True)
    return {"requests": records[:100]}


@app.get("/api/browse-dir")
async def api_browse_dir(path: str = ""):
    def _work():
        p = Path(path) if path else Path(state.save_path)
        if not p.exists():
            p = Path.home()
        p = p.resolve()
        try:
            dirs = sorted(
                (d for d in p.iterdir() if d.is_dir() and not d.name.startswith(".")),
                key=lambda d: d.name.casefold(),
            )
        except OSError as exc:
            return {"path": str(p), "parent": None, "dirs": [], "error": str(exc)}
        parent = str(p.parent) if p.parent != p else None
        return {
            "path": str(p), "parent": parent,
            "dirs": [{"name": d.name, "path": str(d)} for d in dirs],
        }

    return await run_in_threadpool(_work)


@app.post("/api/session/clear-cookies")
async def api_clear_cookies():
    f = _cookie_file_for("filmpalast.to")
    cleared = False
    if f.exists():
        f.unlink()
        cleared = True
    if state.fp_scraper is not None:
        state.fp_scraper.session.clear_cookies()
    log("Cookies gelöscht." if cleared else "Keine Cookies vorhanden.")
    return {"cleared": cleared}


# ── Cover-Proxy ──────────────────────────────────────────────────────────────
def _safe_public_http_url(raw_url: str) -> bool:
    try:
        parsed = urlparse(raw_url)
        if parsed.scheme not in ("http", "https") or not parsed.hostname or parsed.username or parsed.password:
            return False
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        addresses = socket.getaddrinfo(parsed.hostname, port, type=socket.SOCK_STREAM)
        if not addresses:
            return False
        for _family, _socktype, _proto, _canonname, sockaddr in addresses:
            if not ipaddress.ip_address(sockaddr[0]).is_global:
                return False
        return True
    except (OSError, ValueError):
        return False


COVER_FAIL_RETRY_SECONDS = 180.0


def _fetch_cover_data(url: str) -> Optional[tuple]:
    if not _safe_public_http_url(url):
        return None
    with state.cover_cache_lock:
        if url in state.cover_cache:
            state.cover_cache.move_to_end(url)
            return state.cover_cache[url]
        failed_at = state.cover_fail_cache.get(url)
        if failed_at is not None:
            if time.time() - failed_at < COVER_FAIL_RETRY_SECONDS:
                return None
            del state.cover_fail_cache[url]
    try:
        def _download(manager, referer: str) -> tuple:
            resp = manager._curl.get(
                url,
                headers=manager._browser_headers(url, referer),
                timeout=20,
                stream=True,
                allow_redirects=True,
            )
            if resp.status_code != 200:
                raise RuntimeError(f"HTTP {resp.status_code}")
            content_type = (resp.headers.get("Content-Type") or "").split(";", 1)[0].strip().lower()
            if not content_type.startswith("image/"):
                raise RuntimeError(f"kein Bild ({content_type or 'unbekannter Content-Type'})")
            declared = int(resp.headers.get("Content-Length", 0) or 0)
            if declared > 10 * 1024 * 1024:
                raise RuntimeError("Bild ist größer als 10 MB")
            content = bytearray()
            for chunk in resp.iter_content(chunk_size=128 * 1024):
                content.extend(chunk)
                if len(content) > 10 * 1024 * 1024:
                    raise RuntimeError("Bild ist größer als 10 MB")
            return bytes(content), content_type

        parsed_url = urlparse(url)
        hostname = (parsed_url.hostname or "").casefold()
        referer = f"{parsed_url.scheme}://{parsed_url.netloc}/"
        if hostname == "serienstream.to" or hostname.endswith(".serienstream.to"):
            with state.sto_lock:
                data = _download(
                    get_sto_scraper().session, "https://serienstream.to/",
                )
        else:
            data = _download(get_fp_scraper().session, referer)
    except Exception as exc:
        log(f"Cover-Laden fehlgeschlagen ({url}): {exc}", "warn")
        with state.cover_cache_lock:
            state.cover_fail_cache[url] = time.time()
            state.cover_fail_cache.move_to_end(url)
            while len(state.cover_fail_cache) > 512:
                state.cover_fail_cache.popitem(last=False)
        return None
    with state.cover_cache_lock:
        state.cover_cache[url] = data
        state.cover_cache.move_to_end(url)
        while len(state.cover_cache) > 256:
            state.cover_cache.popitem(last=False)
        state.cover_fail_cache.pop(url, None)
    return data


@app.get("/api/cover")
async def api_cover(url: str):
    data = await run_in_threadpool(_fetch_cover_data, url)
    if not data:
        raise HTTPException(502, "Cover konnte nicht geladen werden.")
    content, content_type = data
    return Response(content=content, media_type=content_type)


# ── Bibliothek (Watchlist) ───────────────────────────────────────────────────
class WatchlistAddBody(BaseModel):
    base_slug: str
    title: str
    sample_url: str
    known_slugs: List[str]
    download_mode: str = WATCH_MODE_DEFAULT
    cleanup_mode: Optional[str] = None
    tmdb_id: Optional[int] = None
    aliases: Optional[List[str]] = None
    season_episode_counts: Optional[Dict[str, int]] = None
    season_counts_checked_at: float = 0.0


@app.post("/api/watchlist/add")
async def api_watchlist_add(body: WatchlistAddBody):
    if body.download_mode not in WATCH_MODE_LABELS:
        raise HTTPException(400, "Unbekannte Abo-Regel.")
    if body.cleanup_mode is not None and body.cleanup_mode not in CLEANUP_MODE_LABELS:
        raise HTTPException(400, "Unbekannte Löschregel.")
    incoming_id = str(body.tmdb_id or "").strip()
    incoming_tmdb = None
    if incoming_id:
        incoming_tmdb = await run_in_threadpool(
            get_tmdb_series, body.title, incoming_id,
        )
    entry = None
    with state.watchlist_lock:
        if watchlist_lookup(body.base_slug) is None:
            direct_incoming_titles = {
                _norm_title(value)
                for value in (body.title, *(body.aliases or []))
                if _norm_title(value)
            }
            canonical_incoming_titles = {
                _norm_title(value)
                for value in (
                    (incoming_tmdb or {}).get("title", ""),
                    (incoming_tmdb or {}).get("original_title", ""),
                )
                if _norm_title(value)
            }
            incoming_titles = direct_incoming_titles | canonical_incoming_titles
            duplicate = None
            duplicate_can_migrate = False
            for current in state.watchlist:
                current_id = str(current.get("tmdb_id") or "").strip()
                if incoming_id and current_id:
                    if incoming_id == current_id:
                        duplicate = current
                        break
                    continue
                current_titles = {
                    _norm_title(value)
                    for value in (current.get("title", ""), *(current.get("aliases") or []))
                    if _norm_title(value)
                }
                if incoming_titles & current_titles:
                    duplicate = current
                    duplicate_can_migrate = bool(
                        incoming_id
                        and not current_id
                        and not (direct_incoming_titles & current_titles)
                        and (canonical_incoming_titles & current_titles)
                    )
                    break
            if duplicate is not None:
                if duplicate_can_migrate:
                    duplicate["tmdb_id"] = body.tmdb_id
                    duplicate["aliases"] = list(dict.fromkeys(filter(None, (
                        duplicate.get("title", ""),
                        *(duplicate.get("aliases") or []),
                        body.title,
                        *(body.aliases or []),
                        (incoming_tmdb or {}).get("title", ""),
                        (incoming_tmdb or {}).get("original_title", ""),
                    ))))
                    if (incoming_tmdb or {}).get("season_episode_counts"):
                        duplicate["season_episode_counts"] = incoming_tmdb[
                            "season_episode_counts"
                        ]
                        duplicate["season_counts_checked_at"] = float(
                            incoming_tmdb.get("season_counts_checked_at") or 0
                        )
                    appconfig.save_watchlist(state.watchlist)
                raise HTTPException(
                    409, f"Serie ist bereits als «{duplicate.get('title', body.title)}» abonniert.",
                )
            entry = body.model_dump()
            entry["aliases"] = list(dict.fromkeys(
                alias.strip() for alias in (body.aliases or []) if alias and alias.strip()
            ))
            entry["season_episode_counts"] = {
                str(season): max(0, int(count))
                for season, count in (body.season_episode_counts or {}).items()
            }
            entry["season_counts_checked_at"] = max(0.0, float(body.season_counts_checked_at or 0))
            entry["download_mode"] = normalize_watch_mode(body.download_mode)
            entry["cleanup_mode"] = normalize_cleanup_mode(
                body.cleanup_mode
                if body.cleanup_mode is not None
                else state.jellyfin_cfg.get("cleanup_default")
            )
            entry["cleanup_history"] = []
            entry["cleanup_deleted_count"] = 0
            entry["cleanup_last_error"] = ""
            entry["failed_downloads"] = {}
            entry["last_error"] = ""
            entry["mode_generation"] = 0
            entry["check_generation"] = 0
            state.watchlist.append(entry)
            appconfig.save_watchlist(state.watchlist)
    if entry is not None:
        log(f"«{body.title}» zur Bibliothek hinzugefügt.")

        # Nicht erst bis zum nächsten 30-Minuten-Intervall warten: sofort prüfen
        # und bei eingeschalteter Automatik den Download anstoßen. Die Arbeit
        # läuft außerhalb des API-Requests, damit die Oberfläche direkt reagiert.
        def _initial_watchlist_check():
            try:
                with state.watchlist_lock:
                    if entry not in state.watchlist:
                        return
                check_watchlist_entries([entry])
                broadcast({"type": "watchlist_update", **watchlist_payload()})
                _auto_download_new_episodes()
            except Exception as exc:
                log(f"Erstprüfung von «{body.title}» fehlgeschlagen: {exc}", "warn")

        threading.Thread(target=_initial_watchlist_check, daemon=True).start()
    return watchlist_payload()


class WatchlistModeBody(BaseModel):
    base_slug: str
    download_mode: str
    cleanup_mode: Optional[str] = None


@app.post("/api/watchlist/mode")
async def api_watchlist_mode(body: WatchlistModeBody):
    if body.download_mode not in WATCH_MODE_LABELS:
        raise HTTPException(400, "Unbekannte Abo-Regel.")
    if body.cleanup_mode is not None and body.cleanup_mode not in CLEANUP_MODE_LABELS:
        raise HTTPException(400, "Unbekannte Löschregel.")
    with state.watchlist_lock:
        entry = watchlist_lookup(body.base_slug)
        if entry is None:
            raise HTTPException(404, "Nicht in der Bibliothek.")
        previous_mode = normalize_watch_mode(entry.get("download_mode"))
        mode_changed = previous_mode != body.download_mode
        previous_pending = (
            set(state.watchlist_new_slugs.get(body.base_slug, set()))
            if mode_changed else set()
        )
        entry["download_mode"] = body.download_mode
        if body.cleanup_mode is not None:
            entry["cleanup_mode"] = normalize_cleanup_mode(body.cleanup_mode)
            if entry["cleanup_mode"] == CLEANUP_MODE_KEEP:
                entry["cleanup_last_error"] = ""
        if mode_changed:
            entry["mode_generation"] = int(entry.get("mode_generation", 0)) + 1
        entry["check_generation"] = int(entry.get("check_generation", 0)) + 1
        entry["last_error"] = "Abo-Regel wird geprüft – Auto-Download pausiert"
        appconfig.save_watchlist(state.watchlist)

    if previous_pending:
        _cancel_queue_slugs(previous_pending, "Abo-Regel geändert")

    def _mode_watchlist_check():
        try:
            check_watchlist_entries([entry])
            broadcast({"type": "watchlist_update", **watchlist_payload()})
            _auto_download_new_episodes()
            if previous_pending:
                def _reconcile_after_reap():
                    while any(
                        previous_pending & _job_queue_slugs(job)
                        for job in state.dl_queue.active_jobs()
                    ):
                        time.sleep(0.2)
                    _auto_download_new_episodes()

                threading.Thread(target=_reconcile_after_reap, daemon=True).start()
        except Exception as exc:
            log(f"Abo-Regel für «{entry['title']}» konnte nicht geprüft werden: {exc}", "warn")

    threading.Thread(target=_mode_watchlist_check, daemon=True).start()
    return watchlist_payload()


class WatchlistRemoveBody(BaseModel):
    base_slugs: List[str]


@app.post("/api/watchlist/remove")
async def api_watchlist_remove(body: WatchlistRemoveBody):
    pending_slugs: set[str] = set()
    with state.watchlist_lock:
        for base_slug in body.base_slugs:
            pending_slugs.update(state.watchlist_new_slugs.pop(base_slug, set()))
            state.series_cache.pop(base_slug, None)
        state.watchlist = [w for w in state.watchlist if w["base_slug"] not in body.base_slugs]
        appconfig.save_watchlist(state.watchlist)
    with state.queue_lifecycle_lock:
        removed = state.dl_queue.remove_pending(
            lambda job: bool(pending_slugs & _job_queue_slugs(job))
        )
        state.dl_queue.cancel_active(
            lambda job: bool(pending_slugs & _job_queue_slugs(job))
        )
        # Fallbacks, die ein gerade abbrechender Callback noch kurz eingereiht hat.
        removed.extend(state.dl_queue.remove_pending(
            lambda job: bool(pending_slugs & _job_queue_slugs(job))
        ))
        _release_removed_queue_slugs(pending_slugs)
    for slug in pending_slugs:
        _telegram_terminal_without_job(slug, False, "Abo entfernt", Path(""))
        _seerr_terminal_without_job(slug, False, "Abo entfernt", Path(""))
    broadcast({"type": "queue_update", "queue": build_queue_payload()})
    return watchlist_payload()


@app.get("/api/watchlist")
async def api_watchlist_get():
    return watchlist_payload()


class WatchlistCheckBody(BaseModel):
    base_slugs: Optional[List[str]] = None


def _calculate_watchlist_entry_state(
    entry: dict,
    series: FilmpalastSeries,
    jf_client: JellyfinClient,
    jf_episodes: Optional[List[dict]],
    jf_user_episodes: Optional[List[dict]],
    jf_series: Optional[List[dict]] = None,
) -> dict:
    """Berechnet den Zustand ohne globale Watchlist-Daten zu verändern."""
    previous_keys = {
        parsed[1:]
        for slug in entry.get("known_slugs", [])
        if (parsed := parse_episode_slug(slug)) is not None
    }
    current_keys = {(episode.season, episode.episode) for episode in series.all_episodes}
    if previous_keys and not previous_keys.issubset(current_keys):
        raise RuntimeError("Anbieterantwort unvollständig – bisher bekannte Episoden fehlen")

    downloaded = compute_downloaded_episodes(series)
    aliases = tuple(dict.fromkeys([
        entry.get("title", ""),
        *(entry.get("aliases") or []),
    ]))
    series_ids = jf_client.series_ids_for(
        series.title,
        tmdb_id=entry.get("tmdb_id", ""),
        aliases=aliases,
        items=jf_series,
    ) if jf_series is not None else set()
    if jf_client.configured and jf_series is None:
        raise RuntimeError("Jellyfin-Serienindex nicht verfügbar")
    if series_ids is None:
        raise RuntimeError("Jellyfin-Zuordnung mehrdeutig")
    jf_existing = (
        jf_client.episodes_for_series(
            series.title, items=jf_episodes, aliases=aliases, series_ids=series_ids,
        )
        if jf_episodes is not None else set()
    )
    cleanup_history = normalize_episode_history(entry.get("cleanup_history"))
    jf_existing.update(cleanup_history)
    jf_watched = (
        jf_client.watched_episodes_for_series(
            series.title, jf_user_episodes, aliases=aliases, series_ids=series_ids,
        )
        if jf_user_episodes is not None else None
    )
    if jf_watched is not None:
        jf_watched.update(cleanup_history)
    cleanup_mode = normalize_cleanup_mode(entry.get("cleanup_mode"))
    cleanup_items = []
    if cleanup_mode != CLEANUP_MODE_KEEP and jf_user_episodes is not None:
        cleanup_items = select_cleanup_items(
            jf_client.episode_items_for_series(
                series.title,
                jf_user_episodes,
                aliases=aliases,
                series_ids=series_ids,
            ),
            cleanup_mode,
            entry.get("season_episode_counts") or {},
            cleanup_history,
        )
    mode = normalize_watch_mode(entry.get("download_mode"))
    if mode == WATCH_MODE_NEXT_SEASON:
        counts_checked_at = float(entry.get("season_counts_checked_at") or 0)
        if (
            counts_checked_at <= 0
            or time.time() - counts_checked_at > SERIES_CACHE_TTL + 60
        ):
            raise RuntimeError("Staffelumfang nicht aktuell verifiziert – Auto-Download pausiert")
        expected_counts = {
            int(season): int(count)
            for season, count in (entry.get("season_episode_counts") or {}).items()
            if str(season).lstrip("-").isdigit() and str(count).isdigit()
        }
        source_seasons = sorted({episode.season for episode in series.all_episodes})
        regular_seasons = [season for season in source_seasons if season > 0]
        required_seasons = regular_seasons or source_seasons
        if any(expected_counts.get(season, 0) <= 0 for season in required_seasons):
            raise RuntimeError("Staffelumfang nicht verifizierbar – Auto-Download pausiert")
    missing_slugs = select_missing_episode_slugs(
        series.all_episodes,
        mode,
        downloaded_slugs=downloaded,
        jellyfin_existing=jf_existing,
        jellyfin_watched=jf_watched,
        season_episode_counts=entry.get("season_episode_counts") or {},
    )
    return {
        "mode": mode,
        "cleanup_mode": cleanup_mode,
        "known_slugs": [episode.slug for episode in series.all_episodes],
        "missing_slugs": missing_slugs,
        "cleanup_items": cleanup_items,
    }


def _apply_watchlist_entry_state(entry: dict, calculated: dict) -> set[str]:
    """Übernimmt ein Ergebnis und meldet nicht mehr benötigte Queue-Slugs."""
    entry["download_mode"] = calculated["mode"]
    entry["cleanup_mode"] = calculated["cleanup_mode"]
    entry["known_slugs"] = calculated["known_slugs"]
    previous_slugs = set(state.watchlist_new_slugs.get(entry["base_slug"], set()))
    missing_slugs = set(calculated["missing_slugs"])
    if missing_slugs:
        state.watchlist_new_slugs[entry["base_slug"]] = missing_slugs
    else:
        state.watchlist_new_slugs.pop(entry["base_slug"], None)
    failed = entry.get("failed_downloads")
    if not isinstance(failed, dict):
        failed = {}
    entry["failed_downloads"] = {
        slug: failure for slug, failure in failed.items() if slug in missing_slugs
    }
    entry["last_checked"] = time.time()
    entry["last_error"] = ""
    return previous_slugs - missing_slugs


def _update_watchlist_entry_state(
    entry: dict,
    series: FilmpalastSeries,
    jf_client: JellyfinClient,
    jf_episodes: Optional[List[dict]],
    jf_user_episodes: Optional[List[dict]],
    jf_series: Optional[List[dict]] = None,
) -> set[str]:
    calculated = _calculate_watchlist_entry_state(
        entry, series, jf_client, jf_episodes, jf_user_episodes, jf_series,
    )
    return _apply_watchlist_entry_state(entry, calculated)


def _execute_watchlist_cleanup(
    jobs: List[dict], jf_client: JellyfinClient, jellyfin_generation: int,
) -> int:
    """Löscht freigegebene Jellyfin-Episoden und merkt ihren Abo-Fortschritt.

    Die Historie verhindert, dass absichtlich gelöschte Folgen beim nächsten
    Abo-Lauf wieder als fehlend erkannt werden. Vor jedem externen DELETE wird
    geprüft, ob die Löschregel noch unverändert aktiv ist.
    """
    deleted_total = 0
    deleted_ids: set[str] = set()
    changed = False
    for job in jobs:
        entry = job["entry"]
        revision = int(job["revision"])
        cleanup_mode = normalize_cleanup_mode(job["cleanup_mode"])
        successful_pairs: set[tuple[int, int]] = set()
        failed = 0
        seen_ids: set[str] = set()

        for item in job.get("items") or []:
            item_id = str(item.get("id") or "").strip()
            if not item_id or item_id in seen_ids:
                continue
            seen_ids.add(item_id)
            with state.jellyfin_cache_lock:
                config_is_current = jellyfin_generation == state.jellyfin_config_generation
                with state.watchlist_lock:
                    rule_is_current = bool(
                        any(current is entry for current in state.watchlist)
                        and int(entry.get("check_generation", 0)) == revision
                        and normalize_cleanup_mode(entry.get("cleanup_mode")) == cleanup_mode
                        and cleanup_mode != CLEANUP_MODE_KEEP
                    )
            if not config_is_current or not rule_is_current:
                break
            if jf_client.delete_item(item_id):
                successful_pairs.add((int(item["season"]), int(item["episode"])))
                deleted_ids.add(item_id)
                deleted_total += 1
            else:
                failed += 1

        with state.watchlist_lock:
            if not any(current is entry for current in state.watchlist):
                continue
            if successful_pairs:
                history = normalize_episode_history(entry.get("cleanup_history"))
                history.update(successful_pairs)
                entry["cleanup_history"] = serialize_episode_history(history)
                entry["cleanup_deleted_count"] = int(entry.get("cleanup_deleted_count", 0)) + len(
                    successful_pairs
                )
                changed = True
            if (
                int(entry.get("check_generation", 0)) == revision
                and normalize_cleanup_mode(entry.get("cleanup_mode")) == cleanup_mode
            ):
                entry["cleanup_last_run"] = time.time()
                entry["cleanup_last_error"] = (
                    f"{failed} Jellyfin-Element(e) konnten nicht gelöscht werden"
                    if failed else ""
                )
                changed = True

    if changed:
        with state.watchlist_lock:
            appconfig.save_watchlist(state.watchlist)
    if deleted_ids:
        with state.jellyfin_cache_lock:
            if state.jellyfin_episodes is not None:
                state.jellyfin_episodes = [
                    item for item in state.jellyfin_episodes
                    if str(item.get("id") or "") not in deleted_ids
                ]
            if state.jellyfin_user_episodes is not None:
                state.jellyfin_user_episodes = [
                    item for item in state.jellyfin_user_episodes
                    if str(item.get("id") or "") not in deleted_ids
                ]
            state.jellyfin_episodes_time = 0.0
            state.jellyfin_user_episodes_time = 0.0
            state.jellyfin_episode_data_generation += 1
        log(f"Jellyfin-Aufräumen: {deleted_total} gesehene Episode(n) gelöscht.")
    return deleted_total


def check_watchlist_entries(entries: List[dict], refresh_jellyfin: bool = False) -> int:
    """Prüft die übergebenen Watchlist-Einträge auf fehlende Episoden und
    aktualisiert state.watchlist_new_slugs. Gibt die Anzahl erfolgreich
    geprüfter Einträge zurück. Wird sowohl vom manuellen Check-Endpoint
    als auch vom automatischen Hintergrund-Check genutzt.

    Welche fehlenden Episoden berücksichtigt werden, bestimmt die pro Serie
    gespeicherte Abo-Regel. Jellyfin und lokale Videodateien werden immer als
    bereits vorhanden behandelt."""
    with state.watchlist_lock:
        tracked = []
        for entry in entries:
            if not any(current is entry for current in state.watchlist):
                continue
            entry["check_generation"] = int(entry.get("check_generation", 0)) + 1
            entry["last_error"] = "Prüfung läuft – Auto-Download pausiert"
            tracked.append((entry, entry["check_generation"]))
    if not tracked:
        return 0

    with state.jellyfin_cache_lock:
        jellyfin_generation = state.jellyfin_config_generation
        cfg = dict(state.jellyfin_cfg)
    jf_client = JellyfinClient(cfg.get("url", ""), cfg.get("api_key", ""))
    jf_episodes = get_jellyfin_episodes(force=refresh_jellyfin) if jf_client.configured else None
    jf_series = get_jellyfin_series(force=refresh_jellyfin) if jf_client.configured else None
    with state.jellyfin_cache_lock:
        if jellyfin_generation != state.jellyfin_config_generation:
            return 0
        episodes_available = state.jellyfin_episodes_available
        series_available = state.jellyfin_series_available
        jellyfin_data_generation = state.jellyfin_episode_data_generation

    def _set_error(entry: dict, revision: int, message: str) -> bool:
        with state.jellyfin_cache_lock:
            if (
                jellyfin_generation != state.jellyfin_config_generation
                or jellyfin_data_generation != state.jellyfin_episode_data_generation
            ):
                return False
            with state.watchlist_lock:
                if (
                    not any(current is entry for current in state.watchlist)
                    or int(entry.get("check_generation", 0)) != revision
                ):
                    return False
                entry["last_checked"] = time.time()
                entry["last_error"] = message[:240]
                return True

    if jf_client.configured and (jf_episodes is None or not episodes_available):
        for entry, revision in tracked:
            _set_error(entry, revision, "Jellyfin nicht erreichbar – Auto-Download pausiert")
        with state.watchlist_lock:
            appconfig.save_watchlist(state.watchlist)
        log("Watchlist-Prüfung pausiert: Jellyfin ist nicht erreichbar.", "warn")
        return 0
    if jf_client.configured and (jf_series is None or not series_available):
        for entry, revision in tracked:
            _set_error(entry, revision, "Jellyfin-Serienindex nicht verfügbar")
        with state.watchlist_lock:
            appconfig.save_watchlist(state.watchlist)
        log("Watchlist-Prüfung pausiert: Jellyfin-Serienindex nicht verfügbar.", "warn")
        return 0

    needs_watched_status = any(
        normalize_watch_mode(entry.get("download_mode")) == WATCH_MODE_NEXT_SEASON
        or normalize_cleanup_mode(entry.get("cleanup_mode")) != CLEANUP_MODE_KEEP
        for entry, _revision in tracked
    )
    jf_user_episodes = get_jellyfin_user_episodes(force=refresh_jellyfin) if needs_watched_status else None
    with state.jellyfin_cache_lock:
        if jellyfin_generation != state.jellyfin_config_generation:
            return 0
        user_available = state.jellyfin_user_episodes_available
        jellyfin_data_generation = state.jellyfin_episode_data_generation

    checked = 0
    withdrawn_slugs: set[str] = set()
    cleanup_jobs: List[dict] = []
    for entry, revision in tracked:
        with state.jellyfin_cache_lock:
            if (
                jellyfin_generation != state.jellyfin_config_generation
                or jellyfin_data_generation != state.jellyfin_episode_data_generation
            ):
                break
            with state.watchlist_lock:
                if (
                    not any(current is entry for current in state.watchlist)
                    or int(entry.get("check_generation", 0)) != revision
                ):
                    continue
                entry_snapshot = dict(entry)
        mode = normalize_watch_mode(entry_snapshot.get("download_mode"))
        cleanup_mode = normalize_cleanup_mode(entry_snapshot.get("cleanup_mode"))
        cleanup_status_missing = bool(
            cleanup_mode != CLEANUP_MODE_KEEP
            and (jf_user_episodes is None or not user_available)
        )
        if mode == WATCH_MODE_NEXT_SEASON and (jf_user_episodes is None or not user_available):
            _set_error(entry, revision, "Jellyfin-Benutzerstatus nicht verfügbar")
            continue
        try:
            series = get_series_for_value(entry_snapshot["sample_url"])
            if series is None:
                _set_error(entry, revision, "Serie beim Anbieter nicht abrufbar")
                log(f"«{entry_snapshot['title']}»: konnte nicht geprüft werden.", "warn")
                continue
            tmdb = get_tmdb_series(
                series.title, entry_snapshot.get("tmdb_id", ""),
            )
            if tmdb:
                if not entry_snapshot.get("tmdb_id"):
                    entry_snapshot["tmdb_id"] = tmdb.get("tmdb_id")
                entry_snapshot["aliases"] = list(dict.fromkeys(filter(None, (
                    entry_snapshot.get("title", ""),
                    series.title,
                    tmdb.get("title", ""),
                    tmdb.get("original_title", ""),
                ))))
                entry_snapshot["season_episode_counts"] = tmdb.get("season_episode_counts") or {}
                entry_snapshot["season_counts_checked_at"] = float(
                    tmdb.get("season_counts_checked_at") or 0
                )
            calculated = _calculate_watchlist_entry_state(
                entry_snapshot, series, jf_client, jf_episodes, jf_user_episodes, jf_series,
            )
            with state.jellyfin_cache_lock:
                if (
                    jellyfin_generation != state.jellyfin_config_generation
                    or jellyfin_data_generation != state.jellyfin_episode_data_generation
                ):
                    break
                with state.watchlist_lock:
                    if (
                        not any(current is entry for current in state.watchlist)
                        or int(entry.get("check_generation", 0)) != revision
                    ):
                        continue
                    if entry_snapshot.get("tmdb_id"):
                        entry["tmdb_id"] = entry_snapshot["tmdb_id"]
                        entry["aliases"] = entry_snapshot.get("aliases", [])
                        entry["season_episode_counts"] = entry_snapshot.get("season_episode_counts", {})
                        entry["season_counts_checked_at"] = entry_snapshot.get(
                            "season_counts_checked_at", 0,
                        )
                    entry["cleanup_mode"] = cleanup_mode
                    entry["cleanup_last_error"] = (
                        "Jellyfin-Benutzerstatus nicht verfügbar"
                        if cleanup_status_missing else ""
                    )
                    state.series_cache[entry["base_slug"]] = series
                    withdrawn_slugs.update(
                        _apply_watchlist_entry_state(entry, calculated)
                    )
                    if not cleanup_status_missing and calculated.get("cleanup_items"):
                        cleanup_jobs.append({
                            "entry": entry,
                            "revision": revision,
                            "cleanup_mode": cleanup_mode,
                            "items": calculated["cleanup_items"],
                        })
                    checked += 1
        except Exception as exc:
            _set_error(entry, revision, str(exc))
            log(f"Fehler beim Prüfen von «{entry_snapshot.get('title', '')}»: {exc}", "warn")
    with state.jellyfin_cache_lock:
        data_is_current = (
            jellyfin_generation == state.jellyfin_config_generation
            and jellyfin_data_generation == state.jellyfin_episode_data_generation
        )
        if data_is_current:
            with state.watchlist_lock:
                appconfig.save_watchlist(state.watchlist)
    if data_is_current and withdrawn_slugs:
        _cancel_withdrawn_watchlist_slugs(
            withdrawn_slugs,
            "In Jellyfin vorhanden oder nicht mehr Teil der Abo-Regel",
        )
    if data_is_current and cleanup_jobs:
        _execute_watchlist_cleanup(cleanup_jobs, jf_client, jellyfin_generation)
    return checked


@app.post("/api/watchlist/check")
async def api_watchlist_check(body: WatchlistCheckBody):
    def _work():
        with state.watchlist_lock:
            entries = list(state.watchlist) if not body.base_slugs else [
                w for w in state.watchlist if w["base_slug"] in body.base_slugs
            ]
        checked = check_watchlist_entries(entries, refresh_jellyfin=True)
        return checked, len(entries)

    checked, total = await run_in_threadpool(_work)
    payload = watchlist_payload()
    payload["checked"] = checked
    payload["total"] = total
    broadcast({"type": "watchlist_update", **payload})
    return payload


class WatchlistOpenBody(BaseModel):
    base_slug: str


@app.post("/api/watchlist/open")
async def api_watchlist_open(body: WatchlistOpenBody):
    with state.watchlist_lock:
        entry = watchlist_lookup(body.base_slug)
        if not entry:
            raise HTTPException(404, "Nicht in der Bibliothek.")
        entry["check_generation"] = int(entry.get("check_generation", 0)) + 1
        entry["last_error"] = "Prüfung läuft – Auto-Download pausiert"
        open_revision = entry["check_generation"]

    def _work():
        series = state.series_cache.get(body.base_slug)
        if series is None:
            try:
                series = get_series_for_value(entry["sample_url"])
            except Exception as exc:
                log(f"Fehler beim Laden von «{entry['title']}»: {exc}", "warn")
                series = None
        return series

    series = await run_in_threadpool(_work)
    if series is None:
        raise HTTPException(500, "Serie konnte nicht geladen werden.")

    with state.watchlist_lock:
        if not any(current is entry for current in state.watchlist):
            raise HTTPException(404, "Nicht mehr in der Bibliothek.")
        state.series_cache[body.base_slug] = series

    payload = await run_in_threadpool(series_to_dict, series, True)
    with state.watchlist_lock:
        if (
            any(current is entry for current in state.watchlist)
            and int(entry.get("check_generation", 0)) == open_revision
        ):
            if payload.get("tmdb_id"):
                entry["tmdb_id"] = payload["tmdb_id"]
            if payload.get("aliases"):
                entry["aliases"] = payload["aliases"]
            if payload.get("season_episode_counts"):
                entry["season_episode_counts"] = payload["season_episode_counts"]
                entry["season_counts_checked_at"] = float(
                    payload.get("season_counts_checked_at") or 0
                )

    def _sync_entry_from_loaded_series():
        withdrawn_slugs: set[str] = set()
        cleanup_jobs: List[dict] = []
        with state.jellyfin_cache_lock:
            jellyfin_generation = state.jellyfin_config_generation
        jf_client = get_jellyfin_client()
        jf_episodes = get_jellyfin_episodes() if jf_client.configured else None
        jf_series = get_jellyfin_series() if jf_client.configured else None
        with state.watchlist_lock:
            if (
                not any(current is entry for current in state.watchlist)
                or int(entry.get("check_generation", 0)) != open_revision
            ):
                return
            snapshot = dict(entry)
        mode = normalize_watch_mode(snapshot.get("download_mode"))
        cleanup_mode = normalize_cleanup_mode(snapshot.get("cleanup_mode"))
        needs_user_status = (
            mode == WATCH_MODE_NEXT_SEASON or cleanup_mode != CLEANUP_MODE_KEEP
        )
        user_episodes = get_jellyfin_user_episodes() if needs_user_status else None
        with state.jellyfin_cache_lock:
            jellyfin_data_generation = state.jellyfin_episode_data_generation
            episodes_available = state.jellyfin_episodes_available
            series_available = state.jellyfin_series_available
            user_available = state.jellyfin_user_episodes_available
        if jf_client.configured and (jf_episodes is None or not episodes_available):
            error = "Jellyfin nicht erreichbar – Auto-Download pausiert"
            calculated = None
        elif jf_client.configured and (
            jf_series is None or not series_available
        ):
            error = "Jellyfin-Serienindex nicht verfügbar"
            calculated = None
        else:
            if mode == WATCH_MODE_NEXT_SEASON and (
                user_episodes is None or not user_available
            ):
                error = "Jellyfin-Benutzerstatus nicht verfügbar"
                calculated = None
            else:
                try:
                    calculated = _calculate_watchlist_entry_state(
                        snapshot, series, jf_client, jf_episodes, user_episodes,
                        jf_series,
                    )
                    error = ""
                except Exception as exc:
                    calculated = None
                    error = str(exc)[:240]
        with state.jellyfin_cache_lock:
            if (
                jellyfin_generation != state.jellyfin_config_generation
                or jellyfin_data_generation != state.jellyfin_episode_data_generation
            ):
                return
            with state.watchlist_lock:
                if (
                    not any(current is entry for current in state.watchlist)
                    or int(entry.get("check_generation", 0)) != open_revision
                    or normalize_watch_mode(entry.get("download_mode")) != mode
                ):
                    return
                if error:
                    entry["last_checked"] = time.time()
                    entry["last_error"] = error
                elif calculated is not None:
                    withdrawn_slugs.update(
                        _apply_watchlist_entry_state(entry, calculated)
                    )
                    entry["cleanup_last_error"] = (
                        "Jellyfin-Benutzerstatus nicht verfügbar"
                        if cleanup_mode != CLEANUP_MODE_KEEP
                        and (user_episodes is None or not user_available)
                        else ""
                    )
                    if not entry["cleanup_last_error"] and calculated.get("cleanup_items"):
                        cleanup_jobs.append({
                            "entry": entry,
                            "revision": open_revision,
                            "cleanup_mode": cleanup_mode,
                            "items": calculated["cleanup_items"],
                        })
                appconfig.save_watchlist(state.watchlist)
        if withdrawn_slugs:
            _cancel_withdrawn_watchlist_slugs(
                withdrawn_slugs,
                "In Jellyfin vorhanden oder nicht mehr Teil der Abo-Regel",
            )
        if cleanup_jobs:
            _execute_watchlist_cleanup(cleanup_jobs, jf_client, jellyfin_generation)

    await run_in_threadpool(_sync_entry_from_loaded_series)
    with state.watchlist_lock:
        new_slugs = set(state.watchlist_new_slugs.get(body.base_slug, set()))
    known_now = {episode.slug for episode in series.all_episodes}
    preselect = sorted(new_slugs & known_now)
    payload["preselect_slugs"] = preselect
    return payload


# ── WebSocket (Log / Fortschritt / Queue-Events) ────────────────────────────
@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    if not _authorized_header(websocket.headers.get("authorization", "")):
        await websocket.close(code=1008, reason="Anmeldung erforderlich")
        return
    await ws_manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)


# Statische Web-Oberfläche (muss NACH allen /api- und /ws-Routen gemountet
# werden, sonst würde der Catch-all-Mount sie verdecken).
class NoCacheStaticFiles(StaticFiles):
    """Liefert die Oberfläche mit `Cache-Control: no-cache` aus. Grund: die
    Dateien (index.html/app.js/style.css) werden bei Updates einfach im
    gemounteten Ordner überschrieben. Ohne no-cache serviert der Browser die
    ALTE app.js aus dem Cache (Button da, aber Handler fehlt) → „Einstellungen
    öffnen sich nicht". `no-cache` erzwingt eine Revalidierung (per ETag/
    Last-Modified → 304 wenn unverändert), sodass Updates sofort greifen."""

    async def get_response(self, path, scope):
        response = await super().get_response(path, scope)
        response.headers["Cache-Control"] = "no-cache, must-revalidate"
        return response


app.mount("/", NoCacheStaticFiles(directory=str(WEB_DIR), html=True), name="web")


def _open_browser(port: int):
    time.sleep(1.0)
    webbrowser.open(f"http://127.0.0.1:{port}")


if __name__ == "__main__":
    import os
    import uvicorn
    # Env-gesteuert, damit dieselbe Datei lokal (Windows: Browser öffnet sich,
    # nur lokal erreichbar) UND im Docker-Container (kein Browser, im Netzwerk
    # erreichbar) läuft.
    PORT = int(os.environ.get("PORT", "8765"))
    HOST = os.environ.get("HOST", "127.0.0.1")
    open_browser = os.environ.get("OPEN_BROWSER", "1").lower() not in ("0", "false", "no")
    if open_browser:
        threading.Thread(target=_open_browser, args=(PORT,), daemon=True).start()
    uvicorn.run(app, host=HOST, port=PORT, log_level="warning")
