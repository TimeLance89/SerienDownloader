# Royal Downloader – Container-Image für den 24/7-Betrieb (NAS/Docker).
FROM python:3.12-slim

# System-Abhängigkeiten:
#  - chromium:        echter Browser für nodriver (VOE-Extraktion +
#                     Cloudflare-/Turnstile-Bypass via CDP). Der Extractor
#                     startet ihn im Root-Container explizit ohne Sandbox.
#  - ffmpeg:          von yt-dlp für HLS/M3U8-Streams (VOE u.a.) zwingend nötig.
#  - ca-certificates: TLS-Wurzelzertifikate für curl_cffi/HTTPS.
#  - fonts-liberation: Zeichensatz, damit Chromium headless sauber rendert.
RUN apt-get update && apt-get install -y --no-install-recommends \
        chromium \
        ffmpeg \
        ca-certificates \
        fonts-liberation \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Python-Abhängigkeiten zuerst (bessere Layer-Cache-Nutzung).
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Anwendungscode (siehe .dockerignore – Game/, debug/, lokale Daten bleiben außen).
COPY . .

# nodriver 0.50.3 liefert cdp/network.py mit ungültigem UTF-8 aus → reparieren,
# sonst scheitert `import nodriver` (VOE-Extraktion).
RUN python -c "import nodriver_patch; nodriver_patch.ensure_cdp_utf8()" || true

# Betriebsmodus im Container:
#  - SERIENDL_DATA_DIR: persistenter State (Cookies, Hoster-Intel, Einstellungen,
#                       Watchlist) → per Volume gesichert.
#  - DOWNLOAD_DIR:      Ziel der fertigen Downloads → Bind-Mount auf NAS-Medien.
#  - HOST/PORT:         im Netzwerk erreichbar machen (0.0.0.0).
#  - OPEN_BROWSER=0:    im Container KEINEN Browser öffnen.
#  - CHROME_PATH:       Explizites Chromium-Binary für den VOE-Browser-Pool.
ARG APP_COMMIT_SHA=""
ENV SERIENDL_DATA_DIR=/app/data \
    DOWNLOAD_DIR=/movies \
    SERIES_DIR=/serien \
    HOST=0.0.0.0 \
    PORT=8765 \
    OPEN_BROWSER=0 \
    CHROME_PATH=/usr/bin/chromium \
    APP_COMMIT_SHA=${APP_COMMIT_SHA} \
    PYTHONUNBUFFERED=1

EXPOSE 8765

CMD ["python", "server.py"]
