"""
Persistente Einstellungen.

Speicherort: %APPDATA%/FilmeDownloader/settings.ini (Windows)
             ~/.config/FilmeDownloader/settings.ini (Linux/macOS)

Inhalt: Speicherorte, Integrationen und Automatik-Einstellungen.
"""

import json
import logging
import os
import sys
import threading
from pathlib import Path
from typing import List, Optional

from runtime_paths import data_dir
from watchlist_policy import normalize_watch_mode

logger = logging.getLogger(__name__)
_config_lock = threading.RLock()

APP_NAME = "FilmeDownloader"
MOVIE_PROVIDER_DEFAULTS = ("filmpalast", "moflix", "einschalten", "kinox")
SERIES_PROVIDER_DEFAULTS = ("serienstream", "filmpalast", "moflix")


def _config_dir() -> Path:
    # Docker/NAS: liegt ein zentrales Daten-Verzeichnis vor (SERIENDL_DATA_DIR),
    # werden Einstellungen + Watchlist dort abgelegt (persistentes Volume).
    if os.environ.get("SERIENDL_DATA_DIR", "").strip():
        return data_dir() / APP_NAME
    if sys.platform == "win32":
        base = os.environ.get("APPDATA") or str(Path.home() / "AppData" / "Roaming")
        return Path(base) / APP_NAME
    base = os.environ.get("XDG_CONFIG_HOME") or str(Path.home() / ".config")
    return Path(base) / APP_NAME


def _config_file() -> Path:
    return _config_dir() / "settings.ini"


def _watchlist_file() -> Path:
    return _config_dir() / "series_watchlist.json"


def _queue_file() -> Path:
    return _config_dir() / "download_queue.json"


def _seerr_requests_file() -> Path:
    return _config_dir() / "seerr_requests.json"


def _default_path() -> str:
    # Docker/NAS: Zielordner für Downloads per Env vorgeben (Bind-Mount auf den
    # NAS-Medienordner). Ohne die Variable bleibt der bisherige Default.
    env = os.environ.get("DOWNLOAD_DIR", "").strip()
    if env:
        return env
    return str(Path.home() / "Downloads" / "Filme")


# ---------------------------------------------------------------------------
def _read_all() -> dict:
    """Liest alle key=value Zeilen der settings.ini in ein dict."""
    path = _config_file()
    result: dict = {}
    with _config_lock:
        if not path.exists():
            return result
        try:
            text = path.read_text(encoding="utf-8").strip()
            for line in text.splitlines():
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                result[k.strip().lower()] = v.strip()
        except Exception as exc:
            logger.warning("Config nicht lesbar (%s): %s", path, exc)
    return result


def _write_all(values: dict) -> bool:
    cfg_dir = _config_dir()
    path = _config_file()
    tmp = path.with_name(f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
    with _config_lock:
        try:
            cfg_dir.mkdir(parents=True, exist_ok=True)
            with open(tmp, "w", encoding="utf-8") as f:
                f.write("# FilmeDownloader – persistente Einstellungen\n")
                for k, v in values.items():
                    f.write(f"{k} = {v}\n")
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, path)
            try:
                path.chmod(0o600)
            except OSError:
                pass
            return True
        except Exception as exc:
            logger.warning("Config konnte nicht geschrieben werden: %s", exc)
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass
            return False


def _update_all(updates: dict, ensure_save_path: bool = True) -> bool:
    """Atomarer Read-Modify-Write für konkurrierende Einstellungs-Endpunkte."""
    with _config_lock:
        values = _read_all()
        values.update(updates)
        values.setdefault("schema_version", "1")
        if ensure_save_path and "save_path" not in values:
            values["save_path"] = _default_path()
        return _write_all(values)


def load() -> str:
    """
    Lädt den gespeicherten Download-Pfad. Fallback: ~/Downloads/Filme.
    """
    values = _read_all()
    save_path = values.get("save_path")
    if save_path:
        return save_path
    logger.info("Keine Config unter %s – Default wird genutzt.", _config_file())
    return _default_path()


def save(save_path: str) -> bool:
    """
    Speichert den Download-Pfad. Erstellt den Ordner falls nötig.
    """
    return _update_all({"save_path": save_path}, ensure_save_path=False)


def load_series_path() -> str:
    """Lädt den separaten Zielordner für SERIEN. Priorität: gespeicherter Wert
    (UI) > Umgebungsvariable SERIES_DIR > Film-Pfad (Rückwärtskompatibilität:
    ohne eigene Serien-Einstellung landen Serien wie bisher im Film-Ordner)."""
    values = _read_all()
    series_path = values.get("series_path") or os.environ.get("SERIES_DIR", "").strip()
    if series_path:
        return series_path
    return load()  # Fallback: gleicher Ordner wie Filme


def save_series_path(series_path: str) -> bool:
    return _update_all({"series_path": series_path})


def normalize_provider_order(value, supported) -> List[str]:
    """Behält nur bekannte Anbieter, entfernt Duplikate und ergänzt fehlende."""
    if isinstance(value, str):
        requested = value.split(",")
    else:
        requested = value or []
    allowed = tuple(str(provider).strip().casefold() for provider in supported)
    normalized: List[str] = []
    for provider in requested:
        key = str(provider).strip().casefold()
        if key in allowed and key not in normalized:
            normalized.append(key)
    normalized.extend(provider for provider in allowed if provider not in normalized)
    return normalized


def load_provider_priorities() -> dict:
    """Lädt die Reihenfolge, in der Katalogquellen gesucht und versucht werden."""
    values = _read_all()
    return {
        "movies": normalize_provider_order(
            values.get("movie_provider_priority", ""), MOVIE_PROVIDER_DEFAULTS,
        ),
        "series": normalize_provider_order(
            values.get("series_provider_priority", ""), SERIES_PROVIDER_DEFAULTS,
        ),
    }


def save_provider_priorities(movies, series) -> bool:
    movie_order = normalize_provider_order(movies, MOVIE_PROVIDER_DEFAULTS)
    series_order = normalize_provider_order(series, SERIES_PROVIDER_DEFAULTS)
    return _update_all({
        "movie_provider_priority": ",".join(movie_order),
        "series_provider_priority": ",".join(series_order),
    })


def load_jellyfin() -> dict:
    """Lädt die Jellyfin-Verbindungsdaten (URL + API-Key) für den
    Duplikat-Check. Im Container kann statt der UI auch per Umgebungsvariable
    (JELLYFIN_URL / JELLYFIN_API_KEY / JELLYFIN_USER_ID) vorbelegt werden – praktisch, weil die
    settings.ini bei einem frischen Container leer startet."""
    values = _read_all()
    return {
        "url": values.get("jellyfin_url") or os.environ.get("JELLYFIN_URL", "").strip(),
        "api_key": values.get("jellyfin_api_key") or os.environ.get("JELLYFIN_API_KEY", "").strip(),
        "user_id": values.get("jellyfin_user_id") or os.environ.get("JELLYFIN_USER_ID", "").strip(),
        "user_name": values.get("jellyfin_user_name") or os.environ.get("JELLYFIN_USER_NAME", "").strip(),
    }


def save_jellyfin(url: str, api_key: str, user_id: str = "", user_name: str = "") -> bool:
    return _update_all({
        "jellyfin_url": url,
        "jellyfin_api_key": api_key,
        "jellyfin_user_id": user_id.strip(),
        "jellyfin_user_name": user_name.strip(),
    })


def load_tmdb() -> dict:
    values = _read_all()
    return {
        "api_key": values.get("tmdb_api_key") or os.environ.get("TMDB_API_KEY", "").strip(),
        "language": values.get("tmdb_language") or os.environ.get("TMDB_LANGUAGE", "de-DE").strip() or "de-DE",
    }


def save_tmdb(api_key: str, language: str = "de-DE") -> bool:
    return _update_all({
        "tmdb_api_key": api_key.strip(),
        "tmdb_language": (language or "de-DE").strip(),
    })


def load_telegram() -> dict:
    """Telegram-Bot-Konfiguration; ohne erlaubte Chat-ID nur Einrichtungsmodus."""
    values = _read_all()
    enabled_raw = values.get("telegram_enabled")
    if enabled_raw is None:
        enabled = _env_bool("TELEGRAM_ENABLED") or False
    else:
        enabled = enabled_raw.strip().lower() in ("1", "true", "yes", "on", "ja")
    return {
        "enabled": enabled,
        "bot_token": values.get("telegram_bot_token") or os.environ.get("TELEGRAM_BOT_TOKEN", "").strip(),
        "chat_id": values.get("telegram_chat_id") or os.environ.get("TELEGRAM_CHAT_ID", "").strip(),
    }


def save_telegram(enabled: bool, bot_token: str, chat_id: str) -> bool:
    return _update_all({
        "telegram_enabled": "true" if enabled else "false",
        "telegram_bot_token": bot_token.strip(),
        "telegram_chat_id": chat_id.strip(),
    })


def load_seerr() -> dict:
    """Lädt die Verbindung zur nativen Seerr-Instanz.

    Der API-Schlüssel wird ausschließlich serverseitig gespeichert. Die
    Weboberfläche erhält nur ``has_api_key`` zurück.
    """
    values = _read_all()
    enabled_raw = values.get("seerr_enabled")
    if enabled_raw is None:
        enabled = _env_bool("SEERR_ENABLED") or False
    else:
        enabled = enabled_raw.strip().lower() in ("1", "true", "yes", "on", "ja")
    interval = _opt_int(values.get("seerr_poll_interval_seconds", ""))
    if interval is None:
        interval = _env_int("SEERR_POLL_INTERVAL_SECONDS") or 60
    return {
        "enabled": enabled,
        "url": values.get("seerr_url") or os.environ.get("SEERR_URL", "").strip(),
        "api_key": values.get("seerr_api_key") or os.environ.get("SEERR_API_KEY", "").strip(),
        "poll_interval_seconds": max(15, min(3600, int(interval))),
    }


def save_seerr(enabled: bool, url: str, api_key: str, poll_interval_seconds: int = 60) -> bool:
    return _update_all({
        "seerr_enabled": "true" if enabled else "false",
        "seerr_url": (url or "").strip().rstrip("/"),
        "seerr_api_key": (api_key or "").strip(),
        "seerr_poll_interval_seconds": str(max(15, min(3600, int(poll_interval_seconds or 60)))),
    })


def config_path() -> Path:
    """Diagnose: wo liegt die Config?"""
    return _config_file()


def is_initialized() -> bool:
    """Eine vorhandene settings.ini markiert eine abgeschlossene Ersteinrichtung.

    Der übergeordnete DATA-Ordner allein reicht nicht als Merkmal: Docker legt
    leere Bind-Mounts bereits vor dem App-Start an.
    """
    if not _config_file().is_file():
        return False
    values = _read_all()
    return bool(values.get("save_path", "").strip())


def save_initial_setup(
    save_path: str,
    series_path: str,
    jellyfin_url: str = "",
    jellyfin_api_key: str = "",
    jellyfin_user_id: str = "",
    jellyfin_user_name: str = "",
    tmdb_api_key: str = "",
    telegram_enabled: bool = False,
    telegram_bot_token: str = "",
    telegram_chat_id: str = "",
    auto_download: bool = False,
    check_interval_min: int = 30,
    dl_window_start: Optional[int] = None,
    dl_window_end: Optional[int] = None,
) -> bool:
    """Speichert die komplette Ersteinrichtung in einem einzigen Schreibvorgang."""
    return _update_all({
        "save_path": save_path.strip(),
        "series_path": series_path.strip(),
        "jellyfin_url": jellyfin_url.strip(),
        "jellyfin_api_key": jellyfin_api_key.strip(),
        "jellyfin_user_id": jellyfin_user_id.strip(),
        "jellyfin_user_name": jellyfin_user_name.strip(),
        "tmdb_api_key": tmdb_api_key.strip(),
        "tmdb_language": "de-DE",
        "telegram_enabled": "true" if telegram_enabled else "false",
        "telegram_bot_token": telegram_bot_token.strip(),
        "telegram_chat_id": telegram_chat_id.strip(),
        "auto_download": "true" if auto_download else "false",
        "check_interval_min": str(max(5, int(check_interval_min or 30))),
        "dl_window_start": "" if dl_window_start is None else str(int(dl_window_start)),
        "dl_window_end": "" if dl_window_end is None else str(int(dl_window_end)),
    }, ensure_save_path=False)


# ---------------------------------------------------------------------------
# Automatik-Einstellungen (24/7-Betrieb): Auto-Download abonnierter Serien +
# Zeitsteuerung. Priorität: gespeicherter Wert (UI) > Umgebungsvariable
# (Container-Vorbelegung) > Default.
def _env_bool(name: str) -> Optional[bool]:
    v = os.environ.get(name)
    if v is None or v.strip() == "":
        return None
    return v.strip().lower() in ("1", "true", "yes", "on", "ja")


def _env_int(name: str) -> Optional[int]:
    v = os.environ.get(name)
    if v is None or v.strip() == "":
        return None
    try:
        return int(v.strip())
    except ValueError:
        return None


def _opt_int(value: str) -> Optional[int]:
    try:
        return int(str(value).strip())
    except (ValueError, AttributeError):
        return None


def load_automation() -> dict:
    """Lädt die Automatik-Einstellungen für den 24/7-Betrieb.

    - auto_download:      neue Episoden abonnierter Serien automatisch laden
    - check_interval_min: Prüf-/Download-Intervall in Minuten
    - dl_window_start/end: Stunde 0–23; nur in diesem Fenster wird automatisch
                           geladen. Beide None/leer = jederzeit. start>end = über
                           Mitternacht (z.B. 1–7 = nachts).
    """
    values = _read_all()

    auto = values.get("auto_download")
    if auto is not None:
        auto_download = auto.strip().lower() in ("1", "true", "yes", "on", "ja")
    else:
        env = _env_bool("AUTO_DOWNLOAD")
        auto_download = env if env is not None else False

    interval = _opt_int(values.get("check_interval_min", "")) \
        or _env_int("CHECK_INTERVAL_MIN") or 30
    interval = max(5, interval)  # unter 5 Min. macht keinen Sinn (Rate-Limits)

    start = _opt_int(values.get("dl_window_start", ""))
    if start is None:
        start = _env_int("DL_WINDOW_START")
    end = _opt_int(values.get("dl_window_end", ""))
    if end is None:
        end = _env_int("DL_WINDOW_END")

    return {
        "auto_download": auto_download,
        "check_interval_min": interval,
        "dl_window_start": start if (start is not None and 0 <= start <= 23) else None,
        "dl_window_end": end if (end is not None and 0 <= end <= 23) else None,
    }


def save_automation(auto_download: bool, check_interval_min: int,
                    dl_window_start: Optional[int], dl_window_end: Optional[int]) -> bool:
    return _update_all({
        "auto_download": "true" if auto_download else "false",
        "check_interval_min": str(max(5, int(check_interval_min or 30))),
        "dl_window_start": "" if dl_window_start is None else str(int(dl_window_start)),
        "dl_window_end": "" if dl_window_end is None else str(int(dl_window_end)),
    })


# ---------------------------------------------------------------------------
# Serien-Bibliothek (Watchlist): welche Serien werden auf neue Episoden geprüft.
# Eintrag-Format: {"base_slug", "title", "sample_url", "known_slugs": [...],
#                  "download_mode": "all|latest_season|next_season"}
def load_watchlist() -> List[dict]:
    path = _watchlist_file()
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, list):
            return []
        valid = []
        for entry in data:
            if not isinstance(entry, dict):
                continue
            if not all(isinstance(entry.get(key), str) and entry.get(key) for key in ("base_slug", "title", "sample_url")):
                logger.warning("Ungültiger Watchlist-Eintrag übersprungen: %r", entry)
                continue
            entry["download_mode"] = normalize_watch_mode(entry.get("download_mode"))
            entry["known_slugs"] = [
                str(slug) for slug in entry.get("known_slugs", []) if isinstance(slug, str)
            ]
            valid.append(entry)
        return valid
    except Exception as exc:
        logger.warning("Watchlist nicht lesbar (%s): %s", path, exc)
        return []


def save_watchlist(entries: List[dict]) -> bool:
    cfg_dir = _config_dir()
    path = _watchlist_file()
    tmp = path.with_name(f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
    with _config_lock:
        try:
            cfg_dir.mkdir(parents=True, exist_ok=True)
            payload = json.dumps(entries, ensure_ascii=False, indent=2)
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(payload)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, path)
            return True
        except Exception as exc:
            logger.warning("Watchlist konnte nicht gespeichert werden: %s", exc)
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass
            return False


def load_queue() -> List[str]:
    path = _queue_file()
    if not path.exists():
        return []
    with _config_lock:
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(data, list):
                return []
            return list(dict.fromkeys(slug for slug in data if isinstance(slug, str) and slug.strip()))
        except Exception as exc:
            logger.warning("Download-Queue nicht lesbar (%s): %s", path, exc)
            return []


def save_queue(slugs) -> bool:
    cfg_dir = _config_dir()
    path = _queue_file()
    tmp = path.with_name(f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
    with _config_lock:
        try:
            cfg_dir.mkdir(parents=True, exist_ok=True)
            payload = json.dumps(sorted(set(str(slug) for slug in slugs if slug)), ensure_ascii=False, indent=2)
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(payload)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, path)
            return True
        except Exception as exc:
            logger.warning("Download-Queue konnte nicht gespeichert werden: %s", exc)
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass
            return False


def load_seerr_requests() -> dict:
    """Persistenter Bearbeitungsstand der aus Seerr übernommenen Wünsche."""
    path = _seerr_requests_file()
    if not path.exists():
        return {}
    with _config_lock:
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(data, dict):
                return {}
            return {
                str(request_id): entry
                for request_id, entry in data.items()
                if str(request_id).strip() and isinstance(entry, dict)
            }
        except Exception as exc:
            logger.warning("Seerr-Requeststatus nicht lesbar (%s): %s", path, exc)
            return {}


def save_seerr_requests(entries: dict) -> bool:
    cfg_dir = _config_dir()
    path = _seerr_requests_file()
    tmp = path.with_name(f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
    with _config_lock:
        try:
            cfg_dir.mkdir(parents=True, exist_ok=True)
            payload = json.dumps(entries, ensure_ascii=False, indent=2, sort_keys=True)
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(payload)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, path)
            return True
        except Exception as exc:
            logger.warning("Seerr-Requeststatus konnte nicht gespeichert werden: %s", exc)
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass
            return False
