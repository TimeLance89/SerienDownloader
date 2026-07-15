# Docker / NAS-Betrieb (24/7)

## Moonfin + Seerr (Fire TV)

Für Wünsche aus Moonfin läuft ein **echter Seerr-Container**. Royal Downloader
prüft genehmigte Seerr-Anfragen, lädt fehlende Medien in die NAS-Bibliothek und
startet danach den Jellyfin-Scan. Radarr und Sonarr sind dafür nicht nötig.

```text
Moonfin (Fire TV) → Seerr → Royal Downloader → /movies oder /serien → Jellyfin
```

Seerr ist dabei Katalog und Wunschoberfläche, nicht die Medienquelle. Ein Inhalt
erscheint erst nach Download und Jellyfin-Scan in der Bibliothek. 4K-Wünsche
werden sicher als nicht unterstützt markiert, solange die Downloadquelle keine
garantierte 4K-Auswahl bietet; sie werden nicht versehentlich als normale Fassung geladen.

### Auf dem NAS einrichten

1. Projektordner auf das NAS kopieren und `.env.example` als `.env` kopieren.
2. In `.env` `MOVIES_HOST_DIR` und `SERIES_HOST_DIR` auf die echten
   Jellyfin-Medienordner setzen. Downloader und Jellyfin müssen jeweils
   denselben Host-Ordner sehen.
3. Seerr-Datenordner vorbereiten:

   ```bash
   mkdir -p data/seerr
   sudo chown -R 1000:1000 data/seerr
   docker compose up -d --build
   ```

4. `http://<NAS-IP>:5055` öffnen und den Seerr-Assistenten einmal mit Jellyfin
   abschließen. Radarr/Sonarr überspringen.
5. In Seerr unter **Einstellungen → Allgemein** den API-Schlüssel kopieren.
6. Royal Downloader unter **Einstellungen → Seerr** öffnen, URL
   `http://<NAS-IP>:5055`, API-Schlüssel eintragen, aktivieren und speichern.
   Dabei werden Moonfin-Plugin und Fire-TV-Benutzerprofil automatisch gesetzt.
7. Für den Fire-TV-Benutzer in Seerr die automatische Freigabe für Filme und
   Serien aktivieren.
8. Moonfin am Fire TV öffnen und sich einmal im Seerr-Bereich mit den normalen
   Jellyfin-Zugangsdaten anmelden.

### Späterer Umzug

- Alten Container zuerst stoppen; besonders Seerrs SQLite-Daten nie im laufenden
  Betrieb kopieren.
- `data/` vollständig kopieren. Darin liegen Einstellungen, Queue, Cookies,
  Watchlist, Seerr-Daten und Zugangsdaten.
- Filme/Serien separat in die in `.env` gesetzten Medienordner kopieren.
- `.downloading`-Dateien und `debug/` nicht übernehmen.
- `data/FilmeDownloader/download_queue.json` wird beim Start fortgesetzt. Vor
  dem Umzug die Queue leer laufen lassen, wenn nichts sofort starten soll.

Die Docker-Build-Datei schließt `data/`, `.env` und Downloads aus; Zugangsdaten
landen dadurch nicht im Image.

Zwei Wege – beide bringen **Chromium** (für VOE-Extraktion + Cloudflare-Bypass
via nodriver) und **ffmpeg** (für HLS/M3U8-Streams via yt-dlp) mit.

---

## Weg A – Ordner mounten + `start.sh` (wie beim Game-Projekt) ← empfohlen

Genau dein Muster: Ordner ins NAS ziehen, einen Python-Container darauf mounten,
als Startbefehl `start.sh` setzen. `start.sh` installiert beim Boot die
Abhängigkeiten + den Browser und startet dann den Server.

1. Diesen Projektordner ins NAS ziehen, z. B. nach `/Deluxe`.
2. Container aus einem Python-Image (z. B. `python:3.12`) anlegen und den Ordner
   in den Container mounten, sodass er dort z. B. unter `/Deluxe` liegt.
3. Als **Ausführungsbefehl** setzen:
   ```
   bash /Deluxe/start.sh
   ```
   (`/Deluxe` = dein gemounteter Ordner. Heißt er anders, den Pfad anpassen.)
4. Port **8765** nach außen freigeben.

`start.sh` legt `data/` und `downloads/` **direkt im gemounteten Ordner** an –
also z. B. `/Deluxe/data` und `/Deluxe/downloads`. Genau dein „ich zieh die Daten
einfach selbst in einen Ordner"-Ansatz.

Beim ersten Öffnen der Weboberfläche erscheint automatisch der Einrichtungs-
Wizard. Er fragt Speicherorte, Jellyfin/TMDB, Automatik und Telegram ab und legt
anschließend `data/FilmeDownloader/settings.ini` an. Solange diese Datei fehlt,
starten weder Katalog-Warmup noch Watchlist-Automatik.

> Wichtig: `bash /Deluxe/start.sh` verwenden (nicht nur `/Deluxe/start.sh`), dann
> ist das Ausführungs-Bit egal. Der Container muss als **root** laufen (Standard),
> damit `apt-get` + der Chromium-Sandbox-Verzicht funktionieren.

---

## Weg B – Fertiges Image bauen (`docker compose`)

Wenn du lieber ein self-contained Image baust (Abhängigkeiten im Image, kein
Boot-Install):

```bash
cp .env.example .env          # optional: Zeitzone
docker compose up -d --build
```

Web-Oberfläche: `http://<NAS-IP>:8765`

---

## Volumes / Datenablage

| Im Container | Zweck |
|--------------|-------|
| `…/data`      | Persistenter State: Cloudflare-Cookies, gelernte Hoster-Rankings, Einstellungen + Serien-Watchlist. |
| `/movies`      | Ziel für Filme (Bind-Mount auf Jellyfins Filmordner). |
| `/serien`      | Ziel für Serien (Bind-Mount auf Jellyfins Serienordner). |
| `/app/config`  | Persistente Seerr-Datenbank und Einstellungen. |

Bei **Weg A** liegen beide direkt im gemounteten Ordner. Bei **Weg B** werden sie
über `docker-compose.yml` als `./data` / `./downloads` gemountet.

## Umgebungsvariablen (alle optional, sinnvolle Defaults)

| Variable            | Default          | Bedeutung |
|---------------------|------------------|-----------|
| `SERIENDL_DATA_DIR` | `<ordner>/data`  | Persistenter State. |
| `DOWNLOAD_DIR`      | `<ordner>/downloads` | Download-Ziel für **Filme**. |
| `SERIES_DIR`        | (= `DOWNLOAD_DIR`)   | Getrenntes Download-Ziel für **Serien**. Nicht gesetzt → Serien landen im Film-Ordner. Auch im UI unter *Einstellungen → Speicherorte*. |
| `MOVIES_HOST_DIR`   | `./downloads/Filme` | Nur Compose: Filmordner auf dem NAS, gemountet nach `/movies`. |
| `SERIES_HOST_DIR`   | `./downloads/Serien` | Nur Compose: Serienordner auf dem NAS, gemountet nach `/serien`. |
| `SEERR_URL`         | `http://seerr:5055` | Interne Seerr-Adresse für die Request-Brücke. |
| `SEERR_API_KEY`     | leer | API-Schlüssel aus Seerr; alternativ im Web-UI speichern. |
| `SEERR_ENABLED`     | `false` | Request-Brücke nach abgeschlossener Seerr-Einrichtung aktivieren. |
| `HOST` / `PORT`     | `0.0.0.0` / `8765` | Bind-Adresse/Port. |
| `OPEN_BROWSER`      | `0`              | Im Container aus. |
| `HLS_CONCURRENT_FRAGMENTS` | `4` | Parallele HLS/DASH-Fragmente. |
| `MP4_HTTP_CHUNK_SIZE` | `4M` | Range-Blockgröße gegen CDN-Drosselung langer MP4-Verbindungen. |
| `SLOW_DOWNLOAD_MIN_KIBPS` | `384` | Untergrenze; dauerhaft langsamere Quellen werden gewechselt. `0` deaktiviert. |
| `SLOW_DOWNLOAD_GRACE_SECONDS` | `45` | Startpuffer vor der Geschwindigkeitsprüfung. |
| `SLOW_DOWNLOAD_WINDOW_SECONDS` | `90` | So lange muss die Rate durchgehend zu niedrig sein. |
| `DNS_PRIMARY`       | `1.1.1.1` | Bevorzugter Container-DNS; eigener lokaler Resolver ist möglich. |
| `DNS_SECONDARY`     | `9.9.9.9`        | Fallback-DNS. |
| `DNS_OVERRIDE`      | `1`              | Nur `start.sh`: `0` behält Dockers DNS-Konfiguration unverändert. |
| `APP_USERNAME`      | leer             | HTTP-Basic-Benutzer für die Weboberfläche. Im LAN setzen. |
| `APP_PASSWORD`      | leer             | HTTP-Basic-Passwort. Im LAN setzen. |
| `APP_COMMIT_SHA`    | leer             | Build-Revision für den Updatevergleich; bei einem Git-Checkout wird sie automatisch erkannt. |
| `UPDATE_GITHUB_REPOSITORY` | `TimeLance89/SerienDownloader` | Repository für die Updateprüfung. |
| `UPDATE_GITHUB_BRANCH` | `main` | Verglichener Branch. |

### DNS / Provider-Sperren

`docker-compose.yml` setzt die Resolver über die offizielle `dns`-Option direkt
am Container. Beim Synology-Betrieb mit `bash /Deluxe/start.sh` schreibt das
Startskript dieselben Resolver vor allen Netzwerkzugriffen nach
`/etc/resolv.conf` und prüft anschließend die Auflösung von `serienstream.to`.

DNS-over-TLS läuft nicht zwischen Anwendung und `DNS_PRIMARY`, sondern optional auf
einem eigenen lokalen Resolver: Dieser nimmt normale DNS-Anfragen aus dem LAN entgegen und
leitet sie verschlüsselt an dnsforge weiter.

### Jellyfin + 24/7-Automatik (optional per Env vorbelegen)

Alle auch im Web-UI unter *Einstellungen* setzbar (dann in `data/` gespeichert).
Per Env vorbelegen ist praktisch, damit der Einrichtungs-Wizard bei einem
**frischen** Container bereits sinnvolle Werte vorschlägt.

| Variable            | Beispiel | Bedeutung |
|---------------------|----------|-----------|
| `JELLYFIN_URL`      | `http://192.168.178.47:8096` | Jellyfin-Server (Duplikat-Check). |
| `JELLYFIN_API_KEY`  | `21ead1…` | Jellyfin API-Key (Dashboard → API-Schlüssel). |
| `JELLYFIN_USER_ID`  | `abc123…` | Jellyfin-Benutzer für den Gesehen-Status. |
| `JELLYFIN_USER_NAME`| `Max` | Anzeigename des gewählten Benutzers. |
| `TMDB_API_KEY`      | `abc123…` | Optional: TMDB v3 API-Key oder API Read Access Token für Metadaten. |
| `TMDB_LANGUAGE`     | `de-DE`   | Sprache der TMDB-Metadaten. |
| `AUTO_DOWNLOAD`     | `true`   | Neue Folgen abonnierter Serien automatisch laden. |
| `CHECK_INTERVAL_MIN`| `30`     | Prüf-/Download-Intervall in Minuten (min. 5). |
| `DL_WINDOW_START`   | `1`      | Stunde 0–23: nur ab hier automatisch laden. |
| `DL_WINDOW_END`     | `7`      | Stunde 0–23: bis hier. `start>end` = über Mitternacht (1–7 = nachts). Beide leer = jederzeit. |
| `TELEGRAM_ENABLED`  | `true`   | Telegram-Filmwünsche aktivieren. |
| `TELEGRAM_BOT_TOKEN`| `123…:AA…` | Bot-Token von `@BotFather`. |
| `TELEGRAM_CHAT_ID`  | `123456789` | Einzige Chat-ID, die Downloads auslösen darf. |

> Ein im UI gesetzter Wert hat Vorrang vor der Env-Variable (er wird in `data/`
> persistiert). Env dient der Erstbelegung.

### TMDB-Metadaten

Mit gesetztem TMDB-Key kommen Cover, Beschreibung, Genres, Erscheinungsjahr und
Laufzeit bevorzugt von TMDB. Anbieter bleiben ausschließlich Quelle für Suche,
Hoster und Downloads. Ist TMDB nicht konfiguriert, nicht erreichbar oder findet
keinen eindeutigen Treffer, werden automatisch die bisherigen Anbieterdaten
verwendet. Antworten werden pro Film/Serie im Arbeitsspeicher gecacht.

### Anbieter-Priorität

Unter *Einstellungen → Anbieter-Priorität* lässt sich die Reihenfolge getrennt
für Filme und Serien festlegen. Die erste Quelle wird bevorzugt; Suche,
automatische Anfragen und Download-Fallbacks verwenden dieselbe Reihenfolge.

### Telegram-Filmwünsche

1. Bei `@BotFather` einen Bot anlegen und den Token unter *Einstellungen → Telegram* eintragen.
2. Bot aktivieren, speichern und ihm `/start` senden.
3. Solange keine Chat-ID gespeichert ist, antwortet er ausschließlich mit der eigenen ID.
4. Chat-ID eintragen und erneut speichern. Danach reicht ein Filmtitel wie `Titanic`.

Der Bot prüft zuerst Jellyfin, sucht andernfalls den Film, startet den Download,
stößt anschließend einen Jellyfin-Bibliotheksscan an und meldet die Verfügbarkeit.

Weitere Befehle: `/status`, `/speicher`, `/pfade`, `/abos`, `/jellyfin`, `/hilfe`.

Serienwünsche:

- `The Rookie ALLES` – alle lokal und in Jellyfin fehlenden Episoden.
- `The Rookie Staffel 8` – alle fehlenden Episoden dieser Staffel.
- `The Rookie Staffel 8 EP 3` – genau eine Episode.

## Hinweise

- **Erststart dauert länger** (Chromium + ffmpeg + pip). Danach: bei Weg A wird
  bei erhaltenem Container übersprungen; bei Weg B greift der Layer-Cache.
- **serienstream-Captcha:** Fällt eine Episode am Turnstile-Gate aus, holt der
  Downloader sie automatisch von Filmpalast/Moflix (Serien-Download-Fallback).
- Der lokale Windows-Start (`python server.py`, öffnet den Browser) läuft
  unverändert – alle Container-Anpassungen sind rein env-gesteuert.
