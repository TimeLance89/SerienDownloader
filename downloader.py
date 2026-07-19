"""
Download manager.
Uses yt-dlp (subprocess) as the primary engine – it handles HLS (M3U8),
MP4, and most other formats without extra setup beyond FFmpeg being on PATH.
Falls back to a browser-compatible curl_cffi download for direct MP4 links.
"""

import errno
import math
import os
import queue
import re
import signal
import sys
import json
import shutil
import subprocess
import threading
import time
import logging
import uuid
from pathlib import Path
from typing import Callable, Dict, Optional
from urllib.parse import urlparse

# Fallback, falls das bevorzugte Staging direkt neben dem Ziel nicht angelegt
# werden kann. Normalerweise liegt jeder Job in
#   <Zielordner>/.downloading/<eindeutige-job-id>/
# und damit auf demselben Dateisystem wie die fertige Datei. Das ermöglicht
# eine atomare Finalisierung und verhindert Namenskollisionen zwischen Jobs.
APP_DIR = Path(__file__).parent.resolve()
STAGING_DIR = APP_DIR / ".downloading"

logger = logging.getLogger(__name__)

BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/136.0.0.0 Safari/537.36"
)

MIN_MEDIA_BYTES = 1024 * 1024
MIN_MEDIA_DURATION_SECONDS = 60.0


def _env_number(name: str, default: float, minimum: float, maximum: float) -> float:
    try:
        value = float(os.environ.get(name, default))
    except (TypeError, ValueError):
        value = default
    return min(maximum, max(minimum, value))


# HLS/DASH wird von yt-dlp sonst fragmentweise seriell geladen. Vier parallele
# Fragmente sind schnell genug, ohne kleine Hoster mit zu vielen Verbindungen zu
# ueberfahren.
HLS_CONCURRENT_FRAGMENTS = int(_env_number("HLS_CONCURRENT_FRAGMENTS", 4, 1, 16))
MP4_HTTP_CHUNK_SIZE = os.environ.get("MP4_HTTP_CHUNK_SIZE", "4M").strip()
if not re.fullmatch(r"\d+(?:\.\d+)?[KMG]?", MP4_HTTP_CHUNK_SIZE, re.IGNORECASE):
    MP4_HTTP_CHUNK_SIZE = "4M"

# Ein tröpfelnder Stream gilt erst nach Startpuffer UND durchgehend langsamem
# Fenster als Slow-Kandidat. Der Server kann dann einen anderen Hoster testen;
# ist keiner besser, wird die langsame Quelle ohne Watchdog als Reserve benutzt.
SLOW_DOWNLOAD_MIN_BPS = int(
    _env_number("SLOW_DOWNLOAD_MIN_KIBPS", 384, 0, 10240) * 1024
)
SLOW_DOWNLOAD_GRACE_SECONDS = _env_number(
    "SLOW_DOWNLOAD_GRACE_SECONDS", 45, 0, 1800,
)
SLOW_DOWNLOAD_WINDOW_SECONDS = _env_number(
    "SLOW_DOWNLOAD_WINDOW_SECONDS", 90, 5, 3600,
)
NO_OUTPUT_TIMEOUT_SECONDS = _env_number(
    "DOWNLOAD_NO_OUTPUT_TIMEOUT_SECONDS", 300, 15, 3600,
)
SLOW_FAILURE_PREFIX = "Stream dauerhaft zu langsam"

# Kleine Hoster (z.B. filmfrei24.com inkl. tv.filmfrei24.com) drosseln pro IP,
# sobald mehrere Downloads gleichzeitig laufen. Deshalb laeuft pro Host-Gruppe
# nur ein Download; der zweite Queue-Slot nimmt derweil einen anderen Host.
PER_HOST_MAX_PARALLEL = int(_env_number("DOWNLOAD_PER_HOST_PARALLEL", 1, 1, 16))


def host_group_for_url(url: str) -> str:
    """Gruppiert Stream-URLs nach registrierbarer Domain (tv.x.com == x.com)."""
    host = (urlparse(str(url or "")).hostname or "").lower()
    parts = host.split(".")
    return ".".join(parts[-2:]) if len(parts) > 2 else host


class _LowSpeedWatchdog:
    """Erkennt ausschließlich dauerhaft niedrige gemeldete Transferraten."""

    def __init__(
        self,
        minimum_bps: float = SLOW_DOWNLOAD_MIN_BPS,
        grace_seconds: float = SLOW_DOWNLOAD_GRACE_SECONDS,
        window_seconds: float = SLOW_DOWNLOAD_WINDOW_SECONDS,
        started_at: Optional[float] = None,
    ):
        self.minimum_bps = max(0.0, float(minimum_bps))
        self.grace_seconds = max(0.0, float(grace_seconds))
        self.window_seconds = max(0.0, float(window_seconds))
        self.started_at = time.monotonic() if started_at is None else float(started_at)
        self.low_since: Optional[float] = None

    def observe(self, speed_bps: Optional[float], now: Optional[float] = None) -> bool:
        if self.minimum_bps <= 0 or speed_bps is None:
            return False
        now = time.monotonic() if now is None else float(now)
        if now - self.started_at < self.grace_seconds:
            return False
        if speed_bps >= self.minimum_bps:
            self.low_since = None
            return False
        if self.low_since is None:
            self.low_since = now
            return False
        return now - self.low_since >= self.window_seconds


class _ByteGrowthWatchdog:
    """Fensterbasierte Variante für den direkten MP4-Downloader."""

    def __init__(
        self,
        minimum_bps: float = SLOW_DOWNLOAD_MIN_BPS,
        grace_seconds: float = SLOW_DOWNLOAD_GRACE_SECONDS,
        window_seconds: float = SLOW_DOWNLOAD_WINDOW_SECONDS,
        started_at: Optional[float] = None,
    ):
        self.minimum_bps = max(0.0, float(minimum_bps))
        self.grace_seconds = max(0.0, float(grace_seconds))
        self.window_seconds = max(0.0, float(window_seconds))
        self.started_at = time.monotonic() if started_at is None else float(started_at)
        self.window_started = self.started_at
        self.window_bytes = 0
        self.last_rate_bps = 0.0

    def observe(self, downloaded_bytes: int, now: Optional[float] = None) -> bool:
        if self.minimum_bps <= 0:
            return False
        now = time.monotonic() if now is None else float(now)
        downloaded_bytes = max(0, int(downloaded_bytes))
        if now - self.started_at < self.grace_seconds:
            self.window_started = now
            self.window_bytes = downloaded_bytes
            return False
        elapsed = now - self.window_started
        if elapsed < self.window_seconds:
            return False
        self.last_rate_bps = max(0, downloaded_bytes - self.window_bytes) / max(elapsed, 0.001)
        self.window_started = now
        self.window_bytes = downloaded_bytes
        return self.last_rate_bps < self.minimum_bps


def cleanup_stale_staging(target_roots=(), older_than_seconds: int = 24 * 60 * 60) -> int:
    """Entfernt nach einem Absturz zurückgebliebene Staging-Artefakte."""
    cutoff = time.time() - max(0, older_than_seconds)
    roots = {STAGING_DIR.resolve(strict=False)}
    for target in target_roots or ():
        try:
            base = Path(target).expanduser().resolve(strict=False)
            # Filme liegen direkt im Ziel, Serien üblicherweise in
            # <Serie>/<Staffel>. DownloadJob legt sein Staging jeweils direkt
            # neben dem endgültigen Ziel ab. Deshalb diese begrenzten Ebenen
            # mitprüfen, ohne den gesamten NAS-Baum rekursiv zu durchlaufen.
            candidates = [base / ".downloading"]
            if base.is_dir():
                candidates.extend(base.glob("*/.downloading"))
                candidates.extend(base.glob("*/*/.downloading"))
            for candidate in candidates:
                resolved = candidate.resolve(strict=False)
                if resolved == base or base in resolved.parents:
                    roots.add(resolved)
        except (OSError, TypeError):
            continue
    removed = 0
    for root in roots:
        if not root.is_dir():
            continue
        try:
            children = list(root.iterdir())
        except OSError:
            continue
        for child in children:
            try:
                # Nur eindeutig markierte eigene Jobverzeichnisse anfassen.
                # `.downloading` kann auch von anderen Anwendungen genutzt werden.
                if (
                    child.is_symlink()
                    or not child.is_dir()
                    or re.fullmatch(r"[0-9a-f]{32}", child.name) is None
                ):
                    continue
                marker = child / ".royal-downloader-job"
                if not marker.is_file() or marker.read_text(encoding="ascii").strip() != child.name:
                    continue
                if child.stat(follow_symlinks=False).st_mtime > cutoff:
                    continue
                if child.parent.resolve(strict=False) == root:
                    shutil.rmtree(child)
                else:
                    continue
                removed += 1
            except OSError as exc:
                logger.warning("Altes Staging-Artefakt nicht löschbar (%s): %s", child, exc)
        try:
            root.rmdir()
        except OSError:
            pass
    return removed


def validate_media_file(path: Path) -> tuple:
    """Akzeptiert nur abspielbare Videos, keine HTML-/JSON-Fehlerseiten."""
    try:
        size = path.stat().st_size
    except OSError as exc:
        return False, f"Datei nicht lesbar: {exc}"
    if size < MIN_MEDIA_BYTES:
        return False, f"Datei ist zu klein ({size} Bytes)"

    cmd = [
        "ffprobe", "-v", "error",
        "-show_entries", "stream=codec_type,duration:format=duration",
        "-of", "json",
        str(path),
    ]
    try:
        proc = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=30,
        )
    except FileNotFoundError:
        return False, "ffprobe fehlt"
    except subprocess.TimeoutExpired:
        return False, "ffprobe-Timeout"

    if proc.returncode != 0:
        detail = (proc.stderr or "").strip().splitlines()
        return False, (detail[-1][:200] if detail else "ffprobe konnte die Datei nicht lesen")
    try:
        info = json.loads(proc.stdout or "{}")
        streams = info.get("streams") or []
    except (TypeError, json.JSONDecodeError):
        return False, "ffprobe lieferte keine gültigen Mediendaten"
    durations = []
    for raw_duration in [
        (info.get("format") or {}).get("duration"),
        *(stream.get("duration") for stream in streams if isinstance(stream, dict)),
    ]:
        try:
            duration_value = float(raw_duration)
        except (TypeError, ValueError):
            continue
        if math.isfinite(duration_value) and duration_value >= 0:
            durations.append(duration_value)
    duration = max(durations, default=0)
    if not math.isfinite(duration):
        return False, "ffprobe lieferte keine gültige Mediendauer"
    if not any(stream.get("codec_type") == "video" for stream in streams):
        return False, "kein Videostream gefunden"
    if not any(stream.get("codec_type") == "audio" for stream in streams):
        return False, "kein Audiostream gefunden"
    if duration < MIN_MEDIA_DURATION_SECONDS:
        return False, f"unplausible Videodauer ({duration:.1f} Sekunden)"
    return True, f"{duration:.0f} Sekunden, {size} Bytes"


def probe_stream_url(
    stream_url: str,
    referer: str = "",
    origin: str = "",
    timeout: int = 25,
) -> tuple:
    """
    Prüft per yt-dlp-Simulation, ob eine URL grundsätzlich ladbar ist.
    Lädt keine Medien herunter.
    """
    if not stream_url:
        return False, "leere URL"
    cmd = [
        sys.executable, "-m", "yt_dlp",
        "--simulate",
        "--skip-download",
        "--no-warnings",
        "--socket-timeout", "10",
        "--retries", "0",
        "--fragment-retries", "0",
        # Hilft generischen Playern mit Cloudflare-Challenge (z.B. Easyload).
        # Ohne diese Option empfiehlt yt-dlp sie nur im Fehlertext und jeder
        # Queue-Eintrag läuft erneut in denselben 403.
        "--extractor-args", "generic:impersonate",
        "--user-agent", BROWSER_USER_AGENT,
    ]
    if referer:
        cmd += ["--referer", referer]
    if origin:
        cmd += ["--add-header", f"Origin:{origin}"]
    cmd.append(stream_url)
    try:
        proc = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        return False, "Probe-Timeout"
    out = (proc.stdout or "").strip().splitlines()
    msg = out[-1][:160] if out else ""
    return proc.returncode == 0, msg or f"Code {proc.returncode}"


def _sanitize(name: str) -> str:
    """Remove filesystem-illegal characters."""
    return re.sub(r'[\\/:*?"<>|]', "", name).strip()


def build_filename(
    series_title: str, season: int, episode: int, ep_title: str = ""
) -> str:
    """Return a filename like  Breaking.Bad.S01E03.mp4"""
    base = _sanitize(series_title).replace(" ", ".")
    code = f"S{season:02d}E{episode:02d}"
    title_part = f".{_sanitize(ep_title)}" if ep_title else ""
    return f"{base}.{code}{title_part}.mp4"


def build_movie_filename(movie_title: str, year: str = "") -> str:
    """
    Dateinamen-Schema für Filme (filmpalast.to / Standalone-Movies).

    Beispiele:
      build_movie_filename("Undertone", "2025")
        → "Undertone.2025.mp4"
      build_movie_filename("The Chronology of Water", "2025")
        → "The.Chronology.of.Water.2025.mp4"
    """
    base = _sanitize(movie_title).replace(" ", ".")
    year_part = f".{year}" if year else ""
    return f"{base}{year_part}.mp4"


class DownloadJob:
    def __init__(
        self,
        stream_url: str,
        stream_type: str,  # "hls" or "mp4"
        out_path: Path,
        referer: str = "",
        origin: str = "",
        on_progress: Optional[Callable[[float, str], None]] = None,
        on_done: Optional[Callable[[bool, str], None]] = None,
        queue_slug: Optional[str] = None,
        allow_slow: bool = False,
        provider: str = "",
        content_language: str = "",
    ):
        self.stream_url = stream_url
        self.stream_type = stream_type
        self.out_path = out_path
        self.referer = referer
        self.origin = origin
        # Stabiler fachlicher Schluessel fuer Queue-Aktionen. Der Downloader
        # selbst wertet ihn nicht aus, damit bestehende Aufrufer kompatibel
        # bleiben.
        self.queue_slug = queue_slug
        self.provider = str(provider or "").strip().casefold()
        self.content_language = str(content_language or "").strip().casefold()
        self.allow_slow = bool(allow_slow)
        self.host_group = host_group_for_url(stream_url)
        self.failure_kind = ""
        self.average_speed_bps = 0.0
        self.job_id = uuid.uuid4().hex
        self._preferred_staging_root = out_path.parent / ".downloading"
        self._fallback_staging_root = STAGING_DIR
        self._staging_root = self._preferred_staging_root
        self.staging_dir = self._staging_root / self.job_id
        self.staging_path = self.staging_dir / ("download" + (out_path.suffix or ".mp4"))
        self.on_progress = on_progress or (lambda pct, msg: None)
        self.on_done = on_done or (lambda ok, msg: None)
        self._proc: Optional[subprocess.Popen] = None
        self._cancelled = False

    @property
    def staging_name(self) -> str:
        """Anzeige-Name wo die Datei gerade liegt (zur Log-Ausgabe)."""
        try:
            return str(self.staging_path.relative_to(APP_DIR))
        except ValueError:
            return str(self.staging_path)

    def cancel(self):
        self._cancelled = True
        self._terminate_process_tree()

    def start(self):
        thread = threading.Thread(target=self._run, daemon=True)
        thread.start()
        return thread

    def _run(self):
        try:
            if self._cancelled:
                self.on_done(False, "Abgebrochen")
                return
            # Die Hoster-Probe läuft bereits über yt-dlp. Deshalb denselben Weg
            # auch für den echten Download zuerst nutzen; der generische
            # Extraktor verarbeitet direkte MP4-URLs zuverlässiger als ein
            # einfacher HTTP-Client (Redirects, Referer, CDN-Header).
            success, msg = self._download_ytdlp()
            if (
                not success
                and not self._cancelled
                and self.stream_type == "mp4"
                and ".m3u8" not in self.stream_url.lower()
                and self.failure_kind != "slow"
            ):
                ytdlp_msg = msg
                self._cleanup_staging()
                success, msg = self._download_direct()
                if not success:
                    msg = f"{ytdlp_msg}; Browser-Fallback: {msg}"
            if success and self._cancelled:
                success, msg = False, "Abgebrochen"
            if success:
                # Staging -> Ziel verschieben
                success, msg = self._finalize()
            if not success:
                # Bei Download-, Prüf- oder Finalize-Fehlern Staging aufräumen.
                self._cleanup_staging()
            self.on_done(success, msg)
        except Exception as exc:
            self._cleanup_staging()
            self.on_done(False, str(exc))

    def _finalize(self) -> tuple:
        """Validiert und veroeffentlicht eine Datei atomar am Ziel."""
        try:
            if self._cancelled:
                return False, "Abgebrochen"
            self.out_path.parent.mkdir(parents=True, exist_ok=True)
            # Ausschliesslich im eindeutigen Verzeichnis dieses Jobs suchen.
            candidates = []
            if self.staging_dir.exists():
                for f in self.staging_dir.rglob("*"):
                    if (
                        f.is_file()
                        and not f.is_symlink()
                        and f.suffix.casefold() in (".mp4", ".mkv", ".webm")
                    ):
                        candidates.append((f.stat().st_size, f))
            if not candidates:
                return False, "Finalize-Fehler: keine Datei gefunden"

            # Den expliziten yt-dlp-Ausgabepfad zuerst pruefen, danach Dateien
            # mit der Ziel-Endung und zuletzt die groessten Alternativen. Jede
            # Kandidatin muss Video UND Audio enthalten.
            candidates.sort(
                key=lambda item: (
                    item[1] == self.staging_path,
                    item[1].suffix.casefold() == self.out_path.suffix.casefold(),
                    item[0],
                ),
                reverse=True,
            )
            final = None
            validation_errors = []
            for _, candidate in candidates:
                if self._cancelled:
                    return False, "Abgebrochen"
                valid, validation_msg = self._validate_media(candidate)
                if valid:
                    final = candidate
                    break
                validation_errors.append(validation_msg)
            if final is None:
                detail = validation_errors[0] if validation_errors else "keine gültige Mediendatei"
                return False, f"Ungültiger Download: {detail}"

            target_path = self.out_path if final.suffix == self.out_path.suffix else self.out_path.with_suffix(final.suffix)
            if self._cancelled:
                return False, "Abgebrochen"
            self._commit_file(final, target_path)
            self._cleanup_staging()
            return True, f"Fertig: {target_path.name}"
        except Exception as exc:
            if self._cancelled:
                return False, "Abgebrochen"
            return False, f"Finalize-Fehler: {exc}"

    def _validate_media(self, path: Path) -> tuple:
        if self._cancelled:
            return False, "Abgebrochen"
        result = validate_media_file(path)
        if self._cancelled:
            return False, "Abgebrochen"
        return result

    def _set_staging_root(self, root: Path):
        self._staging_root = root
        self.staging_dir = root / self.job_id
        self.staging_path = self.staging_dir / ("download" + (self.out_path.suffix or ".mp4"))

    def _prepare_staging(self) -> tuple:
        """Legt ein isoliertes Job-Staging an, bevorzugt am Ziel."""
        errors = []
        roots = [self._preferred_staging_root]
        if self._fallback_staging_root != self._preferred_staging_root:
            roots.append(self._fallback_staging_root)
        for root in roots:
            created = False
            try:
                self._set_staging_root(root)
                self.staging_dir.mkdir(parents=True, exist_ok=False)
                created = True
                marker = self.staging_dir / ".royal-downloader-job"
                with marker.open("x", encoding="ascii") as handle:
                    handle.write(self.job_id)
                    handle.flush()
                    os.fsync(handle.fileno())
                return True, ""
            except FileExistsError:
                # UUID-Kollision ist extrem unwahrscheinlich, darf aber niemals
                # ein fremdes Job-Verzeichnis wiederverwenden.
                errors.append(f"{root}: Job-Verzeichnis existiert bereits")
            except OSError as exc:
                errors.append(f"{root}: {exc}")
                if created:
                    try:
                        shutil.rmtree(self.staging_dir)
                    except OSError:
                        pass
        return False, "; ".join(errors)

    def _commit_file(self, source: Path, target: Path):
        """Atomarer Replace; bei Dateisystemwechsel Copy+fsync+Replace."""
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.replace(source, target)
            return
        except OSError as exc:
            if exc.errno != errno.EXDEV:
                raise

        temp_target = target.parent / f".{target.name}.{self.job_id}.tmp"
        try:
            with source.open("rb") as src, temp_target.open("xb") as dst:
                while True:
                    if self._cancelled:
                        raise RuntimeError("Abgebrochen")
                    chunk = src.read(1024 * 1024)
                    if not chunk:
                        break
                    dst.write(chunk)
                dst.flush()
                os.fsync(dst.fileno())
            if temp_target.stat().st_size != source.stat().st_size:
                raise OSError("Staging-Kopie ist unvollständig")
            os.replace(temp_target, target)
            source.unlink()
        except Exception:
            try:
                temp_target.unlink(missing_ok=True)
            except OSError:
                pass
            raise

    def _cleanup_staging(self):
        """Entfernt ausschliesslich das eindeutige Verzeichnis dieses Jobs."""
        allowed_roots = {
            self._preferred_staging_root.resolve(strict=False),
            self._fallback_staging_root.resolve(strict=False),
        }
        try:
            staging_parent = self.staging_dir.parent.resolve(strict=False)
            if self.staging_dir.name != self.job_id or staging_parent not in allowed_roots:
                logger.error("Unsicheres Staging-Cleanup verweigert: %s", self.staging_dir)
                return
            if self.staging_dir.is_symlink():
                self.staging_dir.unlink(missing_ok=True)
            elif self.staging_dir.exists():
                shutil.rmtree(self.staging_dir)
            try:
                self._staging_root.rmdir()  # nur wenn kein anderer Job mehr existiert
            except OSError:
                pass
        except OSError as exc:
            logger.warning("Job-Staging konnte nicht vollständig bereinigt werden: %s", exc)

    # ------------------------------------------------------------------
    # yt-dlp engine (handles HLS + most formats)
    # ------------------------------------------------------------------
    def _terminate_process_tree(self, force: bool = False):
        proc = self._proc
        if proc is None or proc.poll() is not None:
            return
        try:
            if os.name == "nt":
                cmd = ["taskkill", "/PID", str(proc.pid), "/T"]
                if force:
                    cmd.append("/F")
                subprocess.run(
                    cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                    timeout=5, check=False,
                )
            else:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL if force else signal.SIGTERM)
        except (OSError, subprocess.SubprocessError):
            try:
                proc.kill() if force else proc.terminate()
            except OSError:
                pass

    def _download_ytdlp(self) -> tuple:
        if self._cancelled:
            return False, "Abgebrochen"
        self.failure_kind = ""
        self.average_speed_bps = 0.0
        prepared, detail = self._prepare_staging()
        if not prepared:
            return False, f"Staging nicht nutzbar: {detail}"
        cmd = [
            sys.executable, "-m", "yt_dlp",
            "--no-warnings",
            "--newline",
            # CDN-Ausfälle dürfen einen Queue-Slot nicht minutenlang bei 0 %
            # blockieren. Nach zwei kurzen Versuchen übernimmt die serverseitige
            # Hoster-/Quellen-Fallbackkette.
            "--socket-timeout", "15",
            "--retries", "2",
            "--fragment-retries", "2",
            "--abort-on-unavailable-fragments",
            "--file-access-retries", "1",
            "--retry-sleep", "1",
            "--concurrent-fragments", str(HLS_CONCURRENT_FRAGMENTS),
            "--progress-delta", "1",
            "--output", str(self.staging_path),
            "--merge-output-format", "mp4",
            # Quality-Preference: 1080p bevorzugt, sonst runter bis 480p.
            # ext:mp4:m4a = bevorzugt mp4 Video + m4a Audio
            "-S", "res:1080,ext:mp4:m4a",
            "--ffmpeg-location", "ffmpeg",
            "--extractor-args", "generic:impersonate",
            "--user-agent", BROWSER_USER_AGENT,
        ]
        if (
            MP4_HTTP_CHUNK_SIZE
            and self.stream_type == "mp4"
            and ".m3u8" not in self.stream_url.lower()
        ):
            # Einige Doodstream-/CDN-Endpunkte drosseln lange Verbindungen stark,
            # liefern kleine Range-Bloecke aber mit voller Rate.
            cmd += ["--http-chunk-size", MP4_HTTP_CHUNK_SIZE]
        if self.referer:
            cmd += ["--referer", self.referer]
        if self.origin:
            cmd += ["--add-header", f"Origin:{self.origin}"]
        cmd.append(self.stream_url)
        logger.debug("yt-dlp cmd: %s", " ".join(cmd))
        popen_kwargs = {}
        if os.name == "nt":
            popen_kwargs["creationflags"] = (
                getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
                | getattr(subprocess, "CREATE_NO_WINDOW", 0)
            )
        else:
            popen_kwargs["start_new_session"] = True
        if self._cancelled:
            return False, "Abgebrochen"
        self._proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            **popen_kwargs,
        )
        output_queue: "queue.Queue[Optional[str]]" = queue.Queue()

        def _read_output():
            try:
                for raw_line in self._proc.stdout:
                    output_queue.put(raw_line)
            finally:
                output_queue.put(None)

        threading.Thread(target=_read_output, daemon=True).start()
        last_error = ""
        last_output = time.monotonic()
        stalled = False
        slow = False
        speed_watchdog = None if self.allow_slow else _LowSpeedWatchdog()
        while True:
            try:
                raw_line = output_queue.get(timeout=1)
            except queue.Empty:
                if self._cancelled:
                    break
                if time.monotonic() - last_output > NO_OUTPUT_TIMEOUT_SECONDS:
                    stalled = True
                    self._terminate_process_tree()
                    break
                continue
            if raw_line is None:
                break
            last_output = time.monotonic()
            line = raw_line.strip()
            if not line:
                continue
            if self._is_ytdlp_error(line):
                last_error = self._clean_ytdlp_error(line)
            speed_bps = self._parse_speed_bps(line)
            if speed_bps is not None:
                self.average_speed_bps = (
                    speed_bps if self.average_speed_bps <= 0
                    else self.average_speed_bps * 0.8 + speed_bps * 0.2
                )
                if speed_watchdog and speed_watchdog.observe(speed_bps):
                    slow = True
                    self.failure_kind = "slow"
                    self.on_progress(
                        self._parse_progress(line),
                        f"Stream zu langsam ({self._format_speed(self.average_speed_bps)}) – wechsle Anbieter …",
                    )
                    self._terminate_process_tree()
                    break
            pct = self._parse_progress(line)
            self.on_progress(pct, self._friendly_ytdlp_message(line))

        try:
            self._proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self._terminate_process_tree(force=True)
            self._proc.wait()
        if self._cancelled:
            return False, "Abgebrochen"
        if slow:
            return False, f"{SLOW_FAILURE_PREFIX} ({self._format_speed(self.average_speed_bps)})"
        if stalled:
            return False, "Stream lieferte zu lange keinen Fortschritt"
        if self._proc.returncode == 0:
            return True, "yt-dlp OK"
        detail = last_error or f"Prozesscode {self._proc.returncode}"
        return False, f"Stream nicht erreichbar: {detail}"

    def _parse_progress(self, line: str) -> float:
        m = re.search(r"(\d+\.?\d*)\s*%", line)
        return float(m.group(1)) if m else -1.0

    @staticmethod
    def _parse_speed_bps(line: str) -> Optional[float]:
        match = re.search(
            r"\bat\s+(\d+(?:\.\d+)?)\s*([KMGTPE]?i?B)/s\b",
            line,
            flags=re.IGNORECASE,
        )
        if not match:
            return None
        value = float(match.group(1))
        unit = match.group(2).casefold()
        binary = "i" in unit
        prefix = unit[0] if len(unit) > 1 else ""
        power = {"": 0, "k": 1, "m": 2, "g": 3, "t": 4, "p": 5, "e": 6}.get(prefix, 0)
        return value * ((1024 if binary else 1000) ** power)

    @staticmethod
    def _format_speed(speed_bps: float) -> str:
        speed_bps = max(0.0, float(speed_bps or 0))
        if speed_bps >= 1024 * 1024:
            return f"{speed_bps / 1024 / 1024:.2f} MiB/s"
        return f"{speed_bps / 1024:.0f} KiB/s"

    @staticmethod
    def _is_ytdlp_error(line: str) -> bool:
        low = line.lower()
        return "got error:" in low or low.startswith("error:") or "unable to download" in low

    @staticmethod
    def _clean_ytdlp_error(line: str) -> str:
        cleaned = re.sub(r"^\s*(?:error:\s*)?", "", line, flags=re.IGNORECASE)
        cleaned = re.sub(r"^\[download\]\s*got error:\s*", "", cleaned, flags=re.IGNORECASE)
        return " ".join(cleaned.split())[:260]

    @classmethod
    def _friendly_ytdlp_message(cls, line: str) -> str:
        """Keine internen Python-/HTTPS-Fehlerketten ungefiltert ins UI geben."""
        low = line.lower()
        if cls._is_ytdlp_error(line):
            return "Stream-Verbindung gestört – kurzer Wiederholungsversuch …"
        if "retry" in low or "wiederhol" in low:
            return "Stream-Verbindung wird erneut aufgebaut …"
        return line

    # ------------------------------------------------------------------
    # Direct download fallback (MP4 only)
    # ------------------------------------------------------------------
    def _download_direct(self) -> tuple:
        from curl_cffi import requests as cr
        try:
            if self._cancelled:
                return False, "Abgebrochen"
            self.failure_kind = ""
            self.average_speed_bps = 0.0
            headers = {
                "User-Agent": BROWSER_USER_AGENT,
                "Accept": "video/webm,video/mp4,video/*;q=0.9,*/*;q=0.8",
                "Accept-Language": "de-DE,de;q=0.9,en;q=0.7",
                "Sec-Fetch-Dest": "video",
                "Sec-Fetch-Mode": "no-cors",
                "Sec-Fetch-Site": "cross-site",
            }
            if self.referer:
                headers["Referer"] = self.referer
            if self.origin:
                headers["Origin"] = self.origin
            resp = cr.get(
                self.stream_url,
                stream=True,
                headers=headers,
                timeout=30,
                allow_redirects=True,
                impersonate="chrome136",
            )
            resp.raise_for_status()
            content_type = (resp.headers.get("Content-Type") or "").lower()
            if any(bad in content_type for bad in ("text/", "html", "json", "xml")):
                return False, f"Stream lieferte keinen Film ({content_type or 'unbekannter Content-Type'})"
            total = int(resp.headers.get("Content-Length", 0))
            downloaded = 0
            started = time.monotonic()
            speed_watchdog = None if self.allow_slow else _ByteGrowthWatchdog(started_at=started)
            prepared, detail = self._prepare_staging()
            if not prepared:
                return False, f"Staging nicht nutzbar: {detail}"
            with open(self.staging_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=1024 * 256):
                    if self._cancelled:
                        return False, "Abgebrochen"
                    f.write(chunk)
                    downloaded += len(chunk)
                    now = time.monotonic()
                    self.average_speed_bps = downloaded / max(now - started, 0.001)
                    if speed_watchdog and speed_watchdog.observe(downloaded, now):
                        self.failure_kind = "slow"
                        return False, (
                            f"{SLOW_FAILURE_PREFIX} "
                            f"({self._format_speed(speed_watchdog.last_rate_bps)})"
                        )
                    if total > 0:
                        pct = downloaded / total * 100
                        self.on_progress(pct, f"{downloaded // 1024 // 1024} MB")
            if total > 0 and downloaded < total:
                return False, f"Stream endete vorzeitig ({downloaded}/{total} Bytes)"
            return True, "Direct OK"
        except Exception as exc:
            detail = str(exc).casefold()
            if "timeout" in detail or "timed out" in detail:
                return False, "Zeitüberschreitung beim Stream-Server"
            if any(marker in detail for marker in (
                "connection", "httpsconnectionpool", "ssl", "name resolution", "resolve",
            )):
                return False, "Stream-Verbindung konnte nicht aufgebaut werden"
            return False, "Direkter Stream-Download fehlgeschlagen"


class DownloadQueue:
    """
    Download-Queue mit konfigurierbarer Parallelität.

    `max_parallel` = Anzahl gleichzeitiger Downloads (default 2).
    Intelligenter Default: 2 ist gut weil:
    - 1 zu langsam (zwei 1GB-Filme hintereinander wären doppelt so lang)
    - 3+ problematisch (Browser-Pool serialisiert VOE-Extraktionen ohnehin)

    `per_host_limit` = gleichzeitige Downloads je Host-Gruppe (default 1,
    via DOWNLOAD_PER_HOST_PARALLEL konfigurierbar). Verhindert, dass beide
    Slots denselben drosselnden Server treffen.

    `on_queue_done` wird NUR aufgerufen wenn ALLE Jobs durch sind (Erfolg oder Abbruch).
    """

    def __init__(self, max_parallel: int = 2, per_host_limit: int = PER_HOST_MAX_PARALLEL):
        self._jobs: list = []
        self._active: Dict[int, tuple] = {}  # job_id -> (job, thread, start_time)
        self._lock = threading.Lock()
        self._running = False
        self._max_parallel = max(1, max_parallel)
        self._per_host_limit = max(1, per_host_limit)
        self._next_job_id = 0
        self._scheduler_generation = 0
        self.on_queue_done: Optional[Callable] = None
        self._scheduler_thread: Optional[threading.Thread] = None

    def add(self, job: DownloadJob):
        with self._lock:
            self._next_job_id += 1
            self._jobs.append((self._next_job_id, job))

    def add_front(self, job: DownloadJob):
        """Fuegt einen Job als naechsten noch nicht gestarteten Download ein."""
        with self._lock:
            self._next_job_id += 1
            self._jobs.insert(0, (self._next_job_id, job))

    def remove_pending(self, predicate: Callable[[DownloadJob], bool]) -> list:
        """Entfernt passende, noch nicht gestartete Jobs und gibt sie zurueck.

        Aktive Jobs werden absichtlich nicht beruehrt; sie muessen explizit
        ueber ``cancel`` beendet werden.
        """
        with self._lock:
            kept = []
            removed = []
            for queued in self._jobs:
                _, job = queued
                if predicate(job):
                    removed.append(job)
                else:
                    kept.append(queued)
            self._jobs = kept
            return removed

    def cancel_active(self, predicate: Callable[[DownloadJob], bool]) -> list:
        """Bricht passende aktive Jobs ab und gibt die Jobobjekte zurück."""
        with self._lock:
            matched = [
                job for job, _thread, _started in self._active.values()
                if predicate(job)
            ]
            for job in matched:
                job.cancel()
            return matched

    def pending_count(self) -> int:
        with self._lock:
            return len(self._jobs)

    def active_count(self) -> int:
        with self._lock:
            return len(self._active)

    def active_jobs(self) -> list:
        with self._lock:
            return [job for job, _thread, _started in self._active.values()]

    def start(self):
        with self._lock:
            if self._running:
                return
            self._running = True
            self._scheduler_generation += 1
            generation = self._scheduler_generation
            self._scheduler_thread = threading.Thread(
                target=self._scheduler, args=(generation,), daemon=True,
            )
            self._scheduler_thread.start()

    def cancel_all(self):
        with self._lock:
            self._running = False
            self._scheduler_generation += 1
            cancelled = []
            cancelled_jobs = []
            for jid, (job, thread, _t) in list(self._active.items()):
                job.cancel()
                cancelled.append((jid, thread))
                cancelled_jobs.append(job)
            self._jobs.clear()
        if cancelled:
            threading.Thread(
                target=self._reap_cancelled,
                args=(cancelled,),
                daemon=True,
            ).start()
        return cancelled_jobs

    def _reap_cancelled(self, cancelled):
        """Entfernt abgebrochene aktive Jobs auch ohne laufenden Scheduler."""
        for _jid, thread in cancelled:
            thread.join()
        with self._lock:
            for jid, thread in cancelled:
                current = self._active.get(jid)
                if current and current[1] is thread and not thread.is_alive():
                    self._active.pop(jid, None)

    def _scheduler(self, generation: int):
        """
        Scheduler-Loop: startet neue Jobs sobald ein Slot frei wird.
        Läuft bis alle Jobs fertig sind und nichts mehr in der Queue liegt.
        """
        completed_normally = False
        while True:
            with self._lock:
                if not self._running or generation != self._scheduler_generation:
                    break
            # 1. Sammle fertige Jobs ein
            finished = []
            with self._lock:
                for jid, (job, thread, start_t) in list(self._active.items()):
                    if not thread.is_alive():
                        finished.append(jid)
            for jid in finished:
                with self._lock:
                    self._active.pop(jid, None)

            # 2. Starte neue Jobs wenn Slot frei. Pro Host-Gruppe laeuft nur
            #    eine begrenzte Anzahl gleichzeitig, damit sich zwei Slots am
            #    selben (drosselnden) Server nicht gegenseitig ausbremsen.
            #    Gesperrte Jobs bleiben in Reihenfolge liegen; der Slot nimmt
            #    den naechsten Job eines anderen Hosts.
            with self._lock:
                while (
                    self._running
                    and generation == self._scheduler_generation
                    and len(self._active) < self._max_parallel
                    and self._jobs
                ):
                    active_hosts: Dict[str, int] = {}
                    for active_job, _thread, _started in self._active.values():
                        group = getattr(active_job, "host_group", "")
                        if group:
                            active_hosts[group] = active_hosts.get(group, 0) + 1
                    index = next(
                        (
                            i for i, (_jid, queued) in enumerate(self._jobs)
                            if active_hosts.get(
                                getattr(queued, "host_group", ""), 0
                            ) < self._per_host_limit
                        ),
                        None,
                    )
                    if index is None:
                        break
                    jid, job = self._jobs.pop(index)
                    thread = job.start()
                    self._active[jid] = (job, thread, time.monotonic())

            # 3. Nichts zu tun + Queue leer + Queue done → raus
            with self._lock:
                if generation != self._scheduler_generation:
                    break
                if not self._active and not self._jobs:
                    self._running = False
                    completed_normally = True
                    break

            time.sleep(0.2)

        with self._lock:
            owns_generation = generation == self._scheduler_generation
            if owns_generation:
                self._running = False
        if completed_normally and owns_generation and self.on_queue_done:
            try:
                self.on_queue_done()
            except Exception as exc:
                logger.error("on_queue_done Fehler: %s", exc)
