const state = {
  tab: "filme",
  fp: {
    results: [], moviesCache: {}, category: null, page: 1, lastPageFull: false,
    activeGenre: "Alle Genres", selectedSlug: null, pendingPreload: null,
    metadataCache: {}, requestSeq: 0, sources: [],
  },
  series: {
    results: [], browseMode: null, page: 1, lastPageFull: false,
    sources: [], browseRequestSeq: 0, loadingBrowse: false,
    current: null, currentSampleSlug: "", epPicked: new Set(),
    cache: {}, pendingBaseSlug: "", requestSeq: 0, viewGeneration: 0,
    jellyfinRefreshSeq: 0, jellyfinRefreshByBase: new Map(),
  },
  wl: { items: [], selected: new Set(), loaded: false },
  queue: { count: 0, groups: [], loaded: false },
  download: { active: false, percent: 0, completed: 0, total: 0, failed: 0 },
  providers: { movies: [], series: [], labels: {} },
  queuedSlugs: new Set(),
  jellyfinUserConfigured: false,
  watchlistCleanupDefault: "keep",
};

const WATCH_MODE_DEFAULT = "latest_season";
const WATCH_MODE_LABELS = {
  all: "Alles Fehlende",
  latest_season: "Neueste Staffel",
  next_season: "Nächste Staffel nach Gesehen-Status",
};
const WATCH_MODE_EXPLANATIONS = {
  all: {
    title: "Das Abo hält die komplette Serie vollständig",
    copy: "Royal prüft sofort alle Staffeln und danach regelmäßig weiter. Bei aktivem Auto-Download landen Treffer in der Queue, sonst in der Abo-Inbox.",
  },
  latest_season: {
    title: "Die neueste Staffel bleibt im Fokus",
    copy: "Royal prüft sofort die höchste Staffel. Sobald eine neue Staffel erscheint, wird diese zum neuen Ziel. Treffer landen je nach Automatik in der Queue oder Abo-Inbox.",
  },
  next_season: {
    title: "Das Abo folgt deinem Sehfortschritt",
    copy: "Royal prüft den gewählten Jellyfin-Benutzer regelmäßig. Eine weitere Staffel wird erst freigegeben, wenn die vorherige vollständig als gesehen markiert ist.",
  },
};
const WATCH_CLEANUP_DEFAULT = "keep";
const WATCH_CLEANUP_LABELS = {
  keep: "Behalten",
  watched_seasons: "Staffel-Löschung",
  watched_episodes: "Episoden-Löschung",
};
const FP_METADATA_BATCH_SIZE = 4;
const FP_METADATA_BATCH_CONCURRENCY = 3;
let watchModeContext = null;
let watchModeReturnFocus = null;

function escapeHtml(s) {
  return String(s ?? "").replace(/[&<>"']/g, (c) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  }[c]));
}

// ── Tabs ─────────────────────────────────────────────────────────────────
function switchTab(name, { autoLoad = true } = {}) {
  closeAllMediaModals(false);
  document.querySelectorAll(".tab-btn").forEach((b) => b.classList.toggle("active", b.dataset.tab === name));
  document.querySelectorAll(".tab-content").forEach((s) => s.classList.toggle("active", s.id === `tab-${name}`));
  // Im Einstellungen-Bereich die Download-Sidebar ausblenden (eigener Vollbereich).
  document.body.classList.toggle("settings-active", name === "einstellungen");
  closeMobileQueue();
  if (name === "einstellungen") setQueueDockExpanded(false);
  state.tab = name;
  if (name === "bibliothek" && !state.wl.loaded) refreshWatchlist();
  if (name === "serien" && autoLoad) ensureSeriesResults();
}

// ── Log console ──────────────────────────────────────────────────────────
function appendLog(msg, level) {
  const el = document.getElementById("log-console");
  const low = (msg || "").toLowerCase();
  let tag = "";
  if (low.includes("fertig") || low.includes(" ok")) tag = "ok";
  else if (low.includes("fehler") || low.includes("error") || low.includes("nicht")) tag = "err";
  else if (low.includes("warn")) tag = "warn";
  const ts = new Date().toLocaleTimeString("de-DE");
  const line = document.createElement("div");
  line.className = "log-line " + tag;
  line.textContent = `[${ts}] ${msg}`;
  el.appendChild(line);
  el.scrollTop = el.scrollHeight;
}

// ── WebSocket ────────────────────────────────────────────────────────────
let wsReconnectTimer = null;
let wsConnectionGeneration = 0;
let queueSnapshotGeneration = 0;
let watchlistSnapshotGeneration = 0;

async function syncQueueSnapshot(context = "Queue-Synchronisierung", shouldApply = null) {
  const snapshotGeneration = ++queueSnapshotGeneration;
  try {
    const response = await api.queueGet();
    if (snapshotGeneration !== queueSnapshotGeneration || (shouldApply && !shouldApply())) return false;
    renderQueue(response.queue);
    return true;
  } catch (error) {
    console.warn(`${context} fehlgeschlagen:`, error);
    return false;
  }
}

async function syncWatchlistSnapshot(context = "Abo-Synchronisierung", shouldApply = null) {
  const snapshotGeneration = ++watchlistSnapshotGeneration;
  try {
    const response = await api.watchlistGet();
    if (snapshotGeneration !== watchlistSnapshotGeneration || (shouldApply && !shouldApply())) return false;
    applyWatchlist(response.watchlist || []);
    return true;
  } catch (error) {
    console.warn(`${context} fehlgeschlagen:`, error);
    return false;
  }
}

async function resyncAfterWsOpen(connectionGeneration) {
  const isCurrentConnection = () => connectionGeneration === wsConnectionGeneration;
  const queueSync = syncQueueSnapshot(
    "Queue-Synchronisierung nach Verbindung", isCurrentConnection,
  );
  const watchlistSync = syncWatchlistSnapshot(
    "Abo-Synchronisierung nach Verbindung", isCurrentConnection,
  );
  await Promise.allSettled([queueSync, watchlistSync]);
  if (connectionGeneration !== wsConnectionGeneration) return;
  await Promise.allSettled([
    refreshSeriesJellyfinStatus(true),
    refreshFpJellyfinStatus(),
  ]);
}

function connectWs() {
  const connectionGeneration = ++wsConnectionGeneration;
  const proto = location.protocol === "https:" ? "wss" : "ws";
  const ws = new WebSocket(`${proto}://${location.host}/ws`);
  ws.onopen = () => {
    if (connectionGeneration !== wsConnectionGeneration) {
      ws.close();
      return;
    }
    if (wsReconnectTimer) {
      clearTimeout(wsReconnectTimer);
      wsReconnectTimer = null;
    }
    resyncAfterWsOpen(connectionGeneration).catch((error) => {
      console.warn("Live-Ansicht konnte nicht vollständig synchronisiert werden:", error);
    });
  };
  ws.onmessage = (ev) => {
    let data;
    try {
      data = JSON.parse(ev.data);
    } catch (error) {
      console.warn("Ungültige WebSocket-Nachricht verworfen:", error);
      return;
    }
    try {
      if (data.type === "log") {
        appendLog(data.message, data.level);
      } else if (data.type === "progress") {
      const filePercent = Number(data.pct);
      const overallPercent = state.download.total > 0 && filePercent >= 0
        ? ((state.download.completed + filePercent / 100) / state.download.total) * 100
        : filePercent;
      const position = state.download.total
        ? `Datei ${Math.min(state.download.completed + 1, state.download.total)}/${state.download.total} · ` : "";
      setDownloadState("active", data.label || "Download läuft", `${position}${(data.msg || "").slice(0, 70)}`, overallPercent);
    } else if (data.type === "updater_install") {
      applyUpdaterInstallStatus(data.installer || {});
    } else if (data.type === "updater_config") {
      applyUpdaterConfig(data.config || {});
    } else if (data.type === "job_done") {
      state.download.completed = data.done_jobs;
      state.download.total = data.total_jobs;
      state.download.failed = data.failed_jobs || 0;
      const percent = data.total_jobs ? (data.done_jobs / data.total_jobs) * 100 : state.download.percent;
      const moreWork = Number(data.active) + Number(data.pending) > 0;
      const kind = !data.ok && !moreWork ? "error" : "active";
      const title = data.ok ? `${data.done_jobs}/${data.total_jobs} bearbeitet` : "Download fehlgeschlagen";
      const detail = data.ok
        ? `${data.active} aktiv · ${data.pending} warten`
        : String(data.msg || "Alle Anbieter sind ausgefallen").slice(0, 110);
      setDownloadState(kind, title, detail, percent);
      syncQueueSnapshot("Queue-Aktualisierung nach Download");
      if (data.ok && data.slug) markSeriesSlugDownloaded(data.slug);
    } else if (data.type === "queue_started") {
      state.download.completed = data.done_jobs;
      state.download.total = data.total_jobs;
      if (!data.done_jobs) state.download.failed = 0;
      const percent = data.total_jobs ? (data.done_jobs / data.total_jobs) * 100 : 0;
      setDownloadState("active", "Automatischer Download", `${data.done_jobs}/${data.total_jobs} fertig`, percent);
      if (data.queue) renderQueue(data.queue);
      else syncQueueSnapshot("Queue-Start-Synchronisierung");
    } else if (data.type === "queue_update") {
      if (data.queue) renderQueue(data.queue);
      else syncQueueSnapshot("Queue-Live-Synchronisierung");
    } else if (data.type === "queue_done") {
      state.download.completed = data.done_jobs;
      state.download.total = data.total_jobs;
      state.download.failed = data.failed_jobs || 0;
      document.getElementById("cancel-btn").disabled = true;
      if (state.download.failed) {
        const successful = data.successful_jobs || 0;
        const title = successful ? "Mit Fehlern beendet" : "Download fehlgeschlagen";
        setDownloadState("error", title,
          `${successful} erfolgreich · ${state.download.failed} fehlgeschlagen`, 100);
      } else {
        setDownloadState("done", "Abgeschlossen", `${data.done_jobs}/${data.total_jobs} Downloads fertig`, 100);
      }
      syncQueueSnapshot("Queue-Abschluss-Synchronisierung");
    } else if (data.type === "jellyfin_update") {
      refreshFpJellyfinStatus();
      refreshSeriesJellyfinStatus();
      if (data.watchlist) applyWatchlist(data.watchlist);
      } else if (data.type === "watchlist_update") {
        applyWatchlist(data.watchlist || []);
      }
    } catch (error) {
      console.warn("WebSocket-Aktualisierung konnte nicht verarbeitet werden:", error);
    }
  };
  ws.onerror = () => ws.close();
  ws.onclose = () => {
    if (connectionGeneration !== wsConnectionGeneration) return;
    if (wsReconnectTimer) clearTimeout(wsReconnectTimer);
    wsReconnectTimer = setTimeout(connectWs, 2000);
  };
}

// ── Queue (Warteschlange, gemeinsam für Filme + Serien) ───────────────────
function renderQueue(payload) {
  queueSnapshotGeneration += 1;
  state.queue = { ...payload, loaded: true };
  state.queuedSlugs = new Set();
  for (const g of payload.groups) for (const it of g.items) state.queuedSlugs.add(it.slug);
  syncSeriesQueueFlags();

  const count = Number(payload.count) || 0;
  document.getElementById("queue-count").textContent =
    `${count} ${count === 1 ? "Eintrag" : "Einträge"}`;
  document.getElementById("mobile-queue-count").textContent = String(count);
  document.getElementById("queue-dock").classList.toggle("has-items", count > 0);

  const list = document.getElementById("queue-list");
  list.innerHTML = "";
  if (!payload.groups.length) {
    list.innerHTML = `<div class="queue-empty"><strong>Der Downloadplan ist leer</strong><span>Filme oder Episoden erscheinen hier, sobald du sie hinzufügst.</span></div>`;
    syncFpQueueIndicators();
    return;
  }

  let queuePosition = 0;
  for (const g of payload.groups) {
    const gEl = document.createElement("div");
    gEl.className = "queue-group";
    gEl.textContent = `${g.name}  (${g.items.length})`;
    list.appendChild(gEl);
    for (const it of g.items) {
      queuePosition += 1;
      const row = document.createElement("div");
      row.className = "queue-item" + (it.done ? " done" : "");
      const position = document.createElement("span");
      position.className = "queue-position";
      position.textContent = String(queuePosition).padStart(2, "0");
      const content = document.createElement("span");
      content.className = "queue-item-content";
      const label = document.createElement("strong");
      label.className = "queue-item-title";
      label.textContent = it.title;
      const route = document.createElement("span");
      route.className = "queue-item-route";
      route.textContent = it.hoster_label;
      content.append(label, route);
      const status = document.createElement("span");
      status.className = "queue-item-status";
      status.textContent = it.done ? "Fertig" : "Wartet";
      const removeBtn = document.createElement("button");
      removeBtn.className = "remove-btn";
      removeBtn.type = "button";
      removeBtn.textContent = "✕";
      removeBtn.setAttribute("aria-label", `${it.title} aus der Queue entfernen`);
      removeBtn.addEventListener("click", async () => {
        removeBtn.disabled = true;
        try {
          const resp = await api.queueRemove(it.slug);
          renderQueue(resp.queue);
        } catch (error) {
          console.warn("Queue-Eintrag konnte nicht entfernt werden:", error);
          removeBtn.disabled = false;
        }
      });
      row.append(position, content, status, removeBtn);
      list.appendChild(row);
    }
  }
  syncFpQueueIndicators();
}

function setQueueDockExpanded(expanded) {
  if (window.matchMedia("(max-width: 820px)").matches) return;
  const dock = document.getElementById("queue-dock");
  const drawer = document.getElementById("queue-drawer");
  const toggle = document.getElementById("queue-dock-toggle");
  dock.classList.toggle("queue-expanded", expanded);
  drawer.setAttribute("aria-hidden", String(!expanded));
  toggle.setAttribute("aria-expanded", String(expanded));
  toggle.querySelector(".queue-toggle-label").textContent = expanded ? "Queue schließen" : "Queue öffnen";
}

function toggleDesktopQueue() {
  const dock = document.getElementById("queue-dock");
  setQueueDockExpanded(!dock.classList.contains("queue-expanded"));
}

function openMobileQueue() {
  document.body.classList.add("queue-open");
  document.getElementById("mobile-queue-backdrop").setAttribute("aria-hidden", "false");
  document.getElementById("queue-drawer").setAttribute("aria-hidden", "false");
  document.getElementById("mobile-queue-close").focus();
}

function closeMobileQueue() {
  document.body.classList.remove("queue-open");
  document.getElementById("mobile-queue-backdrop").setAttribute("aria-hidden", "true");
  if (window.matchMedia("(max-width: 820px)").matches) {
    document.getElementById("queue-drawer").setAttribute("aria-hidden", "true");
  }
}

function setDownloadState(kind, title, detail, percent = state.download.percent) {
  const safePercent = Number.isFinite(Number(percent)) && Number(percent) >= 0
    ? Math.max(0, Math.min(100, Number(percent))) : state.download.percent;
  state.download.active = kind === "active";
  state.download.percent = safePercent;
  const stage = document.getElementById("download-stage");
  stage.dataset.state = kind;
  document.getElementById("dl-state-icon").textContent = kind === "done" ? "✓" : kind === "active" ? "↓" : kind === "error" ? "!" : kind === "cancelled" ? "×" : "↓";
  document.getElementById("dl-state-title").textContent = title;
  document.getElementById("dl-status").textContent = detail;
  document.getElementById("dl-percent").textContent = `${Math.round(safePercent)}%`;
  document.getElementById("progress-fill").style.width = `${safePercent}%`;
  stage.querySelector(".progress-bar").setAttribute("aria-valuenow", String(Math.round(safePercent)));
  document.getElementById("mobile-queue-btn").classList.toggle("downloading", state.download.active);
  document.getElementById("cancel-btn").disabled = !state.download.active;
}

function activeMediaModal() {
  return document.querySelector(".media-modal.is-open:not([hidden])");
}

function openMediaModal(modalId, trigger = null) {
  const modal = document.getElementById(modalId);
  if (!modal) return;
  const current = activeMediaModal();
  if (current && current !== modal) closeMediaModal(current.id, false);
  if (!modal.hidden && modal.classList.contains("is-open")) return;
  modal._returnFocus = trigger instanceof HTMLElement ? trigger : document.activeElement;
  modal.hidden = false;
  modal.classList.add("is-open");
  document.body.classList.add("media-modal-open");
  requestAnimationFrame(() => modal.querySelector(".media-modal-close")?.focus());
}

function closeMediaModal(modalId, restoreFocus = true) {
  const modal = document.getElementById(modalId);
  if (!modal || modal.hidden) return;
  const returnFocus = modal._returnFocus;
  modal.classList.remove("is-open");
  modal.hidden = true;
  if (!activeMediaModal()) document.body.classList.remove("media-modal-open");
  if (restoreFocus && returnFocus instanceof HTMLElement && returnFocus.isConnected) returnFocus.focus();
}

function closeAllMediaModals(restoreFocus = true) {
  document.querySelectorAll(".media-modal:not([hidden])").forEach((modal) => {
    closeMediaModal(modal.id, restoreFocus);
  });
}

function handleMediaModalKeydown(event) {
  const modal = activeMediaModal();
  if (!modal) return false;
  if (event.key === "Escape") {
    event.preventDefault();
    closeMediaModal(modal.id);
    return true;
  }
  if (event.key !== "Tab") return false;
  const focusable = [...modal.querySelectorAll(
    'button:not([disabled]), [href], input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])',
  )].filter((element) => !element.hidden && element.getClientRects().length);
  if (!focusable.length) {
    event.preventDefault();
    modal.querySelector(".media-modal-panel")?.focus();
    return true;
  }
  const first = focusable[0];
  const last = focusable[focusable.length - 1];
  if (event.shiftKey && document.activeElement === first) {
    event.preventDefault();
    last.focus();
  } else if (!event.shiftKey && document.activeElement === last) {
    event.preventDefault();
    first.focus();
  }
  return true;
}

function refreshQueueUiAfterChange(resp) {
  renderQueue(resp.queue);
  if (resp.auto_started) {
    state.download.completed = resp.done_jobs;
    state.download.total = resp.total_jobs;
    const percent = resp.total_jobs ? (resp.done_jobs / resp.total_jobs) * 100 : 0;
    setDownloadState("active", "Automatischer Download", `${resp.done_jobs}/${resp.total_jobs} fertig`, percent);
  }
  renderFpResults();
  renderSeriesTiles();
}

// ── Filme-Tab ──────────────────────────────────────────────────────────────
function fpStatusMessage() {
  const visibleSlugs = new Set(state.fp.results.map((r) => r.slug));
  const visiblePicks = [...state.queuedSlugs].filter((s) => visibleSlugs.has(s)).length;
  const otherPicks = state.queuedSlugs.size - visiblePicks;
  let msg;
  if (state.fp.activeGenre === "Alle Genres") {
    msg = `${state.fp.results.length} Treffer`;
  } else {
    msg = `Genre: ${state.fp.activeGenre}  ·  ${state.fp.results.length} Treffer`;
  }
  if (state.queuedSlugs.size) {
    const extra = otherPicks ? `  ·  ${otherPicks} von anderen Seiten` : "";
    msg += `  ·  ${state.queuedSlugs.size} markiert${extra}`;
  }
  return msg;
}

function setActiveGenreFilter(genre) {
  const activeGenre = genre || "Alle Genres";
  state.fp.activeGenre = activeGenre;
  const activeLabel = document.getElementById("genre-active");
  if (activeLabel) activeLabel.textContent = activeGenre === "Alle Genres" ? "Alle Filme" : activeGenre;
  document.querySelectorAll("#genre-filter [data-genre]").forEach((button) => {
    const selected = button.dataset.genre === activeGenre;
    button.classList.toggle("is-active", selected);
    button.setAttribute("aria-pressed", String(selected));
  });
}

function scrollCatalogToStart(tabId, resultsId) {
  const tab = document.getElementById(tabId);
  const results = document.getElementById(resultsId);
  const panel = results?.closest(".results-panel");
  if (!tab || !results || !panel) return;

  results.scrollTop = 0;
  results.scrollLeft = 0;
  if (window.matchMedia("(max-width: 820px)").matches) {
    const topbarHeight = document.querySelector(".topbar")?.getBoundingClientRect().height || 0;
    const targetTop = window.scrollY + panel.getBoundingClientRect().top - topbarHeight - 14;
    window.scrollTo({ top: Math.max(0, targetTop), behavior: "auto" });
    return;
  }

  const targetTop = tab.scrollTop + panel.getBoundingClientRect().top - tab.getBoundingClientRect().top;
  tab.scrollTo({ top: Math.max(0, targetTop - 8), behavior: "auto" });
}

async function refreshFpJellyfinStatus() {
  const items = state.fp.results.map((r) => ({
    slug: r.slug,
    title: r.title,
    year: r.year || "",
    tmdb_id: state.fp.metadataCache[r.slug]?.tmdb_id || null,
  }));
  if (!items.length) return;
  try {
    const response = await api.jellyfinMatches(items);
    for (const result of state.fp.results) {
      if (Object.hasOwn(response.matches || {}, result.slug)) {
        result.in_jellyfin = !!response.matches[result.slug];
      }
    }
    updateFpJellyfinBadges();
  } catch (e) { /* JF bleibt optional. */ }
}

function updateSeriesStatus(series) {
  if (!series) return;
  const status = document.getElementById("series-status");
  if (series.availability_error) {
    status.textContent = `${series.episode_count} Episoden · Verfügbarkeitsprüfung fehlgeschlagen`;
    return;
  }
  if (series.availability_pending) {
    status.textContent = `${series.episode_count} Episoden · Verfügbarkeit wird geprüft …`;
    return;
  }
  if (series.jellyfin_available === false) {
    status.textContent = `${series.episode_count} Episoden · Jellyfin-Abgleich nicht verfügbar`;
    return;
  }
  if (series.jellyfin_configured) {
    const jellyfinCount = (series.seasons || []).reduce(
      (sum, season) => sum + season.episodes.filter((episode) => episode.in_jellyfin).length,
      0,
    );
    status.textContent = `${series.episode_count} Episoden · ${jellyfinCount} in Jellyfin`;
    return;
  }
  status.textContent = `${series.episode_count} Episoden`;
}

async function refreshSeriesJellyfinStatus(force = false) {
  const current = state.series.current;
  if (!current) return false;
  const baseSlug = current.base_slug;
  const sampleSlug = state.series.currentSampleSlug || firstEpisodeSlug(current) || current.url;
  const viewGeneration = state.series.viewGeneration;
  const refreshGeneration = ++state.series.jellyfinRefreshSeq;
  state.series.jellyfinRefreshByBase.set(baseSlug, refreshGeneration);
  try {
    const refreshed = await api.seriesLoad(sampleSlug, baseSlug, force);
    const isLatestForSeries = state.series.jellyfinRefreshByBase.get(baseSlug) === refreshGeneration;
    const isSameView = state.series.viewGeneration === viewGeneration;
    if (!isLatestForSeries || !isSameView || state.series.current?.base_slug !== baseSlug) return false;
    syncSeriesQueueFlags(refreshed);
    state.series.current = refreshed;
    state.series.cache[baseSlug] = refreshed;
    pruneSeriesEpisodeSelection();
    updateWatchBtn();
    renderSeriesTiles();
    updateSeriesStatus(refreshed);
    return true;
  } catch (error) {
    console.warn("Serienstatus konnte nicht live aktualisiert werden:", error);
    const isLatestForSeries = state.series.jellyfinRefreshByBase.get(baseSlug) === refreshGeneration;
    const isSameView = state.series.viewGeneration === viewGeneration;
    if (
      isLatestForSeries
      && isSameView
      && state.series.current?.base_slug === baseSlug
      && state.series.current.availability_pending
    ) {
      state.series.current.availability_error = true;
      state.series.cache[baseSlug] = state.series.current;
      renderSeriesTiles();
      updateSeriesStatus(state.series.current);
    }
    return false;
  } finally {
    if (state.series.jellyfinRefreshByBase.get(baseSlug) === refreshGeneration) {
      state.series.jellyfinRefreshByBase.delete(baseSlug);
    }
  }
}

function setFpJellyfinBadge(badge, owned) {
  badge.className = `jellyfin-badge ${owned ? "owned" : "dim"}`;
  badge.textContent = owned ? "JF · DA" : "—";
  badge.title = owned
    ? "Bereits in der Jellyfin-Bibliothek gefunden"
    : "Nicht in der Jellyfin-Bibliothek gefunden";
}

function setFpPosterJellyfinBadge(badge, owned) {
  badge.hidden = !owned;
  badge.textContent = "JF · DA";
  badge.title = "Bereits in der Jellyfin-Bibliothek gefunden";
  badge.setAttribute("aria-label", "In Jellyfin vorhanden");
}

function updateFpJellyfinBadges() {
  const resultsBySlug = new Map(state.fp.results.map((result) => [result.slug, result]));
  for (const row of document.querySelectorAll("#fp-results .row")) {
    const result = resultsBySlug.get(row.dataset.slug);
    const badge = row.querySelector(".jellyfin-badge");
    if (result && badge) setFpJellyfinBadge(badge, !!result.in_jellyfin);
    const posterBadge = row.querySelector(".result-card-library-badge");
    if (result && posterBadge) setFpPosterJellyfinBadge(posterBadge, !!result.in_jellyfin);
  }
}

function mediaCardInitials(title) {
  const words = String(title || "").trim().split(/\s+/).filter(Boolean);
  if (!words.length) return "RD";
  return (words.length === 1 ? words[0].slice(0, 2) : words.slice(0, 2).map((word) => word[0]).join(""))
    .toUpperCase();
}

function createResultCardVisual(media, title, kind, inJellyfin = false) {
  const visual = document.createElement("span");
  visual.className = "result-card-visual";

  const fallback = document.createElement("span");
  fallback.className = "result-card-fallback";
  fallback.textContent = mediaCardInitials(title);
  visual.appendChild(fallback);

  if (media?.cover_url) {
    const image = document.createElement("img");
    image.className = "result-card-poster";
    image.alt = "";
    // Die Posterwand ist bereits seitenweise begrenzt. Eager Loading verhindert,
    // dass Browser Bilder im internen Scrollbereich erst nach einem Klick anfordern.
    image.loading = kind === "movie" ? "eager" : "lazy";
    if (kind === "movie") image.fetchPriority = "auto";
    image.decoding = "async";
    image.src = api.coverUrl(media.cover_url);
    image.addEventListener("error", () => image.remove(), { once: true });
    visual.appendChild(image);
  }

  const kindMark = document.createElement("span");
  kindMark.className = "result-card-kind";
  kindMark.textContent = kind === "series" ? "S" : "F";
  const openMark = document.createElement("span");
  openMark.className = "result-card-open";
  openMark.textContent = "↗";
  openMark.setAttribute("aria-hidden", "true");
  visual.append(kindMark, openMark);
  if (kind === "movie") {
    const libraryBadge = document.createElement("span");
    libraryBadge.className = "result-card-library-badge";
    setFpPosterJellyfinBadge(libraryBadge, inJellyfin);
    visual.appendChild(libraryBadge);
  }
  return visual;
}

function activateResultCard(row, callback) {
  row.tabIndex = 0;
  row.setAttribute("role", "button");
  row.setAttribute("aria-haspopup", "dialog");
  row.addEventListener("click", callback);
  row.addEventListener("keydown", (event) => {
    if (event.target !== row || (event.key !== "Enter" && event.key !== " ")) return;
    event.preventDefault();
    callback();
  });
}

function fpResultMedia(result) {
  return state.fp.moviesCache[result.slug] || state.fp.metadataCache[result.slug] || result;
}

function fpResultAvailability(result) {
  const movie = state.fp.moviesCache[result.slug];
  const queued = state.queuedSlugs.has(result.slug);
  if (queued) return { label: "In Queue", tag: "picked" };
  if (movie) {
    if (!movie.hosters || movie.hosters.length === 0) return { label: "Kein Hoster", tag: "novoe" };
    return { label: movie.hoster_label || "Bereit", tag: "ready" };
  }
  if (state.fp.pendingPreload?.has(result.slug)) return { label: "Lädt …", tag: "pending" };
  return { label: "Wird geprüft", tag: "idle" };
}

function findFpResultCard(slug) {
  return [...document.querySelectorAll("#fp-results .result-card")]
    .find((row) => row.dataset.slug === slug) || null;
}

function updateFpResultCard(slug) {
  const result = state.fp.results.find((item) => item.slug === slug);
  const row = findFpResultCard(slug);
  if (!result || !row) return;
  const oldVisual = row.querySelector(".result-card-visual");
  oldVisual?.replaceWith(createResultCardVisual(
    fpResultMedia(result), result.title, "movie", !!result.in_jellyfin,
  ));
  const availability = fpResultAvailability(result);
  const stateLabel = row.querySelector(".result-card-state");
  if (stateLabel) {
    stateLabel.className = `result-card-state status-${availability.tag}`;
    stateLabel.textContent = availability.label;
  }
  const subtitle = row.querySelector(".result-card-subtitle");
  if (subtitle) subtitle.textContent = (fpResultMedia(result).genres || []).slice(0, 2).join(" · ") || "Film";
  const rating = row.querySelector(".result-card-rating");
  if (rating) rating.textContent = fpResultMedia(result).rating ? `★ ${fpResultMedia(result).rating}` : "★ —";
}

function syncFpDetailQueueAction() {
  const slug = state.fp.selectedSlug;
  const detailPanel = document.getElementById("fp-detail-panel");
  if (!slug || detailPanel.classList.contains("is-empty")) return;
  const movie = state.fp.moviesCache[slug];
  const metadata = state.fp.metadataCache[slug];
  if (movie) configureFpDetailAction(slug, movie, false);
  else if (metadata) configureFpDetailAction(slug, metadataPreviewMovie(metadata), true);
}

function syncFpQueueIndicators() {
  for (const result of state.fp.results) {
    const row = findFpResultCard(result.slug);
    if (!row) continue;
    const queued = state.queuedSlugs.has(result.slug);
    row.classList.toggle("queued", queued);
    const toggle = row.querySelector(".result-queue-toggle");
    if (toggle) {
      toggle.classList.toggle("is-queued", queued);
      toggle.textContent = queued ? "✓" : "+";
      toggle.setAttribute("aria-label", queued
        ? `${result.title} aus der Queue entfernen`
        : `${result.title} zur Queue hinzufügen`);
    }
    const availability = fpResultAvailability(result);
    const stateLabel = row.querySelector(".result-card-state");
    if (stateLabel) {
      stateLabel.className = `result-card-state status-${availability.tag}`;
      stateLabel.textContent = availability.label;
    }
  }
  if (state.fp.results.length) {
    document.getElementById("fp-status").textContent = fpStatusMessage();
  }
  syncFpDetailQueueAction();
}

function updateFpResultSelection() {
  for (const row of document.querySelectorAll("#fp-results .row")) {
    const selected = row.dataset.slug === state.fp.selectedSlug;
    row.classList.toggle("selected", selected);
    row.setAttribute("aria-current", String(selected));
  }
}

function renderFpResults() {
  const container = document.getElementById("fp-results");
  container.innerHTML = "";

  for (const result of state.fp.results) {
    const selected = result.slug === state.fp.selectedSlug;
    const queued = state.queuedSlugs.has(result.slug);
    const availability = fpResultAvailability(result);
    const media = fpResultMedia(result);

    const row = document.createElement("div");
    row.className = "row result-card" + (selected ? " selected" : "") + (queued ? " queued" : "");
    row.dataset.slug = result.slug;
    row.setAttribute("aria-current", String(selected));
    row.setAttribute("aria-label", [result.title, result.year].filter(Boolean).join(", "));

    const visual = createResultCardVisual(media, result.title, "movie", !!result.in_jellyfin);

    const copy = document.createElement("span");
    copy.className = "result-card-copy";
    const title = document.createElement("strong");
    title.className = "result-card-title";
    title.textContent = result.title;
    const subtitle = document.createElement("span");
    subtitle.className = "result-card-subtitle";
    subtitle.textContent = (media.genres || []).slice(0, 2).join(" · ") || "Film";
    const meta = document.createElement("span");
    meta.className = "result-card-meta";
    const rating = document.createElement("span");
    rating.className = "result-card-rating";
    rating.textContent = media.rating ? `★ ${media.rating}` : "★ —";
    const year = document.createElement("span");
    year.className = "result-card-year";
    year.textContent = result.year || "Jahr offen";
    const status = document.createElement("span");
    status.className = `result-card-state status-${availability.tag}`;
    status.textContent = availability.label;
    const jellyfin = document.createElement("span");
    setFpJellyfinBadge(jellyfin, !!result.in_jellyfin);
    meta.append(rating, year, status, jellyfin);
    copy.append(title, subtitle, meta);

    const queueToggle = document.createElement("button");
    queueToggle.type = "button";
    queueToggle.className = "pick-flag result-queue-toggle" + (queued ? " is-queued" : "");
    queueToggle.textContent = queued ? "✓" : "+";
    queueToggle.setAttribute("aria-label", queued
      ? `${result.title} aus der Queue entfernen`
      : `${result.title} zur Queue hinzufügen`);
    queueToggle.addEventListener("click", (event) => {
      event.stopPropagation();
      toggleFpPick(result.slug);
    });

    row.append(visual, copy, queueToggle);
    activateResultCard(row, () => selectFpRow(result.slug));
    container.appendChild(row);
  }

  document.getElementById("fp-status").textContent = fpStatusMessage();
}

function applyFpResults(data) {
  state.fp.results = data.results;
  state.fp.page = data.page;
  state.fp.category = data.category;
  state.fp.lastPageFull = Boolean(data.has_more ?? data.last_page_full);
  state.fp.sources = Array.isArray(data.sources)
    ? data.sources.filter((source) => Number(source.count) > 0)
    : [];
  state.fp.selectedSlug = null;
  const pendingSlugs = new Set(
    data.results
      .filter((result) => !state.fp.metadataCache[result.slug])
      .map((result) => result.slug),
  );
  state.fp.pendingPreload = pendingSlugs.size ? pendingSlugs : null;
  renderFpResults();
  const pager = document.getElementById("fp-pager");
  pager.classList.remove("hidden");
  const sourceCount = state.fp.sources.length;
  const sourceWord = sourceCount === 1 ? "Quelle" : "Quellen";
  document.getElementById("fp-pager-label").textContent = sourceCount
    ? `Seite ${data.page} · ${sourceCount} ${sourceWord}`
    : `Seite ${data.page || 1}`;
  const sourceSummary = state.fp.sources
    .map((source) => `${source.label} ${source.count}`)
    .join(" · ");
  const sourceElement = document.getElementById("fp-pager-sources");
  sourceElement.textContent = sourceSummary;
  sourceElement.title = sourceSummary;
  const canPaginate = Boolean(data.category);
  document.getElementById("fp-pager-prev").disabled = !canPaginate || data.page <= 1;
  document.getElementById("fp-pager-next").disabled = !canPaginate || !state.fp.lastPageFull;
  if (data.results.length) void preloadTmdbMetadata(state.fp.requestSeq);
}

async function loadFpMetadata(item, requestId = state.fp.requestSeq) {
  let metadata = state.fp.metadataCache[item.slug];
  if (metadata && state.fp.selectedSlug === item.slug) {
    showFpDetail(item.slug, metadataPreviewMovie(metadata), true);
  }
  try {
    if (!metadata) {
      const response = await api.tmdbMovies([{ slug: item.slug, title: item.title, year: item.year || "" }]);
      if (requestId !== state.fp.requestSeq) return null;
      metadata = response.movies?.[item.slug] || null;
      if (metadata) {
        state.fp.metadataCache[item.slug] = metadata;
        updateFpResultCard(item.slug);
      }
      if (state.fp.selectedSlug === item.slug) {
        showFpDetail(item.slug, metadataPreviewMovie(metadata || basicMovieMetadata(item)), true);
      }
    }
    if (metadata && !metadata.details_loaded) {
      const detailResponse = await api.tmdbMovie({ slug: item.slug, title: item.title, year: item.year || "" });
      if (requestId !== state.fp.requestSeq) return metadata;
      if (detailResponse.movie) {
        metadata = detailResponse.movie;
        state.fp.metadataCache[item.slug] = metadata;
        updateFpResultCard(item.slug);
        if (state.fp.selectedSlug === item.slug) showFpDetail(item.slug, metadataPreviewMovie(metadata), true);
      }
    }
    if (metadata?.tmdb_id) refreshFpJellyfinStatus();
    return metadata || null;
  } catch (e) {
    if (requestId === state.fp.requestSeq && state.fp.selectedSlug === item.slug) {
      showFpDetail(item.slug, metadataPreviewMovie(metadata || basicMovieMetadata(item)), true);
    }
    return metadata || null;
  }
}

async function preloadTmdbMetadata(requestId) {
  const items = state.fp.results
    .filter((r) => !state.fp.metadataCache[r.slug])
    .map((r) => ({ slug: r.slug, title: r.title, year: r.year || "" }));
  if (!items.length) {
    state.fp.pendingPreload = null;
    return;
  }
  const visibleSlugs = new Set(items.map((item) => item.slug));
  const batches = [];
  for (let index = 0; index < items.length; index += FP_METADATA_BATCH_SIZE) {
    batches.push(items.slice(index, index + FP_METADATA_BATCH_SIZE));
  }
  let nextBatch = 0;

  const loadNextBatch = async () => {
    while (nextBatch < batches.length) {
      const batch = batches[nextBatch++];
      let response;
      try {
        response = await api.tmdbMovies(batch);
      } catch (e) {
        continue;
      }
      if (requestId !== state.fp.requestSeq) return;
      for (const [slug, metadata] of Object.entries(response.movies || {})) {
        if (!visibleSlugs.has(slug)) continue;
        if (!state.fp.metadataCache[slug]?.details_loaded) {
          state.fp.metadataCache[slug] = metadata;
        }
        state.fp.pendingPreload?.delete(slug);
        updateFpResultCard(slug);
      }
      const selected = state.fp.selectedSlug;
      if (selected && batch.some((item) => item.slug === selected)
          && !state.fp.moviesCache[selected] && state.fp.metadataCache[selected]) {
        showFpDetail(selected, metadataPreviewMovie(state.fp.metadataCache[selected]), true);
      }
    }
  };

  try {
    const workerCount = Math.min(FP_METADATA_BATCH_CONCURRENCY, batches.length);
    await Promise.all(Array.from({ length: workerCount }, () => loadNextBatch()));
    if (requestId !== state.fp.requestSeq) return;
    refreshFpJellyfinStatus();
  } catch (e) { /* Anbieter-Metadaten bleiben als Fallback sichtbar. */ }
  finally {
    if (requestId !== state.fp.requestSeq) return;
    state.fp.pendingPreload = null;
    for (const slug of visibleSlugs) updateFpResultCard(slug);
  }
}

async function fpSearch() {
  const q = document.getElementById("fp-search").value.trim();
  if (!q) return;
  state.fp.category = null;
  document.getElementById("fp-status").textContent = `Suche nach «${q}» …`;
  setActiveGenreFilter("Alle Genres");
  const requestId = ++state.fp.requestSeq;
  const data = await api.movies({ mode: "search", query: q });
  if (requestId !== state.fp.requestSeq) return;
  applyFpResults(data);
}

async function fpShowList(category) {
  state.fp.category = category;
  setActiveGenreFilter("Alle Genres");
  document.getElementById("fp-status").textContent = `Lade ${category === "new" ? "Neu" : "Top"}-Filme …`;
  const requestId = ++state.fp.requestSeq;
  const data = await api.movies({ mode: category, page: 1 });
  if (requestId !== state.fp.requestSeq) return;
  applyFpResults(data);
}

async function fpGenreChange(genre) {
  if (genre === "Alle Genres") {
    await fpShowList("new");
    return;
  }
  state.fp.category = "genre";
  setActiveGenreFilter(genre);
  document.getElementById("fp-status").textContent = `Lade Genre ${genre} …`;
  const requestId = ++state.fp.requestSeq;
  const data = await api.movies({ mode: "genre", genre, page: 1 });
  if (requestId !== state.fp.requestSeq) return;
  applyFpResults(data);
}

async function fpPagerChange(delta) {
  if (!state.fp.category) return;
  const newPage = state.fp.page + delta;
  if (newPage < 1) return;
  const params = state.fp.category === "genre"
    ? { mode: "genre", genre: state.fp.activeGenre, page: newPage }
    : { mode: state.fp.category, page: newPage };
  const requestId = ++state.fp.requestSeq;
  const pager = document.getElementById("fp-pager");
  const previousButton = document.getElementById("fp-pager-prev");
  const nextButton = document.getElementById("fp-pager-next");
  pager.setAttribute("aria-busy", "true");
  previousButton.disabled = true;
  nextButton.disabled = true;
  document.getElementById("fp-status").textContent = `Lade Seite ${newPage} …`;
  try {
    const data = await api.movies(params);
    if (requestId !== state.fp.requestSeq) return;
    applyFpResults(data);
    scrollCatalogToStart("tab-filme", "fp-results");
  } catch (error) {
    if (requestId !== state.fp.requestSeq) return;
    document.getElementById("fp-status").textContent =
      `Seite ${newPage} konnte nicht geladen werden: ${error.message}`;
  } finally {
    if (requestId === state.fp.requestSeq) {
      pager.removeAttribute("aria-busy");
      previousButton.disabled = state.fp.page <= 1;
      nextButton.disabled = !state.fp.lastPageFull;
    }
  }
}

async function toggleFpPick(slug) {
  if (state.queuedSlugs.has(slug)) {
    const resp = await api.queueRemove(slug);
    refreshQueueUiAfterChange(resp);
    return;
  }
  const resp = await api.queueAdd([slug]);
  if (!state.fp.moviesCache[slug]) {
    try {
      state.fp.moviesCache[slug] = await api.movie(slug);
      updateFpResultCard(slug);
    } catch (e) { /* server logs */ }
  }
  refreshQueueUiAfterChange(resp);
}

async function selectFpRow(slug) {
  state.fp.selectedSlug = slug;
  updateFpResultSelection();
  const movie = state.fp.moviesCache[slug];
  const item = state.fp.results.find((r) => r.slug === slug);
  if (!item) return;
  const metadata = state.fp.metadataCache[slug];
  if (movie) showFpDetail(slug, movie);
  else if (metadata) showFpDetail(slug, metadataPreviewMovie(metadata), true);
  else {
    const detailPanel = document.getElementById("fp-detail-panel");
    detailPanel.classList.remove("is-empty");
    detailPanel.classList.add("has-no-cover");
    detailPanel.style.removeProperty("--detail-backdrop-image");
    document.getElementById("fp-detail-cover").removeAttribute("src");
    document.getElementById("fp-detail-title").textContent = "Lade Cover und Beschreibung …";
    setFpDetailAvailability("Metadaten werden geladen", "loading");
  }
  openMediaModal("fp-detail-modal", findFpResultCard(slug));
  if (movie) return;
  await loadFpMetadata(item);
}

function basicMovieMetadata(item) {
  return { title: item.title, year: item.year || "", cover_url: "", description: "", genres: [], runtime: "" };
}

function metadataPreviewMovie(metadata) {
  return {
    ...metadata,
    hosters: [],
    hoster_route: "wird geladen",
    hoster_score: null,
    hoster_fallback_count: 0,
  };
}

function renderFpDetailItems(id, values, emptyText = "") {
  const element = document.getElementById(id);
  element.innerHTML = "";
  const items = (values || []).filter(Boolean);
  if (!items.length && emptyText) items.push(emptyText);
  for (const value of items) {
    const item = document.createElement("span");
    item.textContent = value;
    element.appendChild(item);
  }
}

function setFpDetailAvailability(text, state = "ready") {
  const badge = document.getElementById("fp-detail-availability");
  badge.textContent = text;
  badge.className = `detail-availability is-${state}`;
}

function configureFpDetailAction(slug, movie, metadataOnly = false) {
  const addBtn = document.getElementById("fp-detail-add");
  const queued = state.queuedSlugs.has(slug);
  const hasHosters = Array.isArray(movie.hosters) && movie.hosters.length > 0;
  addBtn.disabled = !queued && !metadataOnly && !hasHosters;
  addBtn.textContent = queued ? "✕ Aus Queue entfernen" : "↓ Herunterladen";

  addBtn.onclick = async () => {
    const shouldRemove = state.queuedSlugs.has(slug);
    addBtn.disabled = true;
    addBtn.textContent = shouldRemove ? "Entferne …" : metadataOnly ? "Prüfe …" : "Füge hinzu …";
    try {
      if (metadataOnly) {
        await toggleFpPick(slug);
        const loaded = state.fp.moviesCache[slug];
        if (loaded && state.fp.selectedSlug === slug) showFpDetail(slug, loaded);
        else if (state.fp.selectedSlug === slug) showFpDetail(slug, movie, true);
        return;
      }
      const resp = shouldRemove ? await api.queueRemove(slug) : await api.queueAdd([slug]);
      refreshQueueUiAfterChange(resp);
      if (state.fp.selectedSlug === slug) showFpDetail(slug, movie);
    } catch (error) {
      console.warn("Film konnte nicht zur Queue hinzugefügt werden:", error);
      configureFpDetailAction(slug, movie, metadataOnly);
    }
  };
}
function showFpDetail(slug, movie, metadataOnly = false) {
  const detailPanel = document.getElementById("fp-detail-panel");
  const cover = document.getElementById("fp-detail-cover");
  detailPanel.classList.remove("is-empty");
  detailPanel.classList.toggle("has-no-cover", !movie.cover_url);
  if (movie.cover_url) {
    const coverUrl = api.coverUrl(movie.cover_url);
    if (cover.getAttribute("src") !== coverUrl) cover.src = coverUrl;
    detailPanel.style.setProperty("--detail-backdrop-image", `url("${coverUrl}")`);
  } else if (cover.hasAttribute("src")) {
    cover.removeAttribute("src");
    detailPanel.style.removeProperty("--detail-backdrop-image");
  } else {
    detailPanel.style.removeProperty("--detail-backdrop-image");
  }
  cover.alt = movie.title ? `Poster zu ${movie.title}` : "Filmplakat";
  document.getElementById("fp-detail-title").textContent = movie.title;
  const metaParts = [];
  if (movie.year) metaParts.push(movie.year);
  if (movie.runtime) metaParts.push(movie.runtime);
  if (movie.rating) metaParts.push(`★ ${movie.rating}/10`);
  if (!metadataOnly) metaParts.push(movie.hosters.length ? `${movie.hosters.length} Hoster` : "kein Hoster");
  if (movie.metadata_source) metaParts.push(movie.metadata_source);
  renderFpDetailItems("fp-detail-meta", metaParts, "Keine Metadaten");
  renderFpDetailItems("fp-detail-genres", movie.genres, "Genre unbekannt");
  if (metadataOnly) setFpDetailAvailability("Streams werden geprüft", "loading");
  else if (movie.hosters.length) setFpDetailAvailability(`${movie.hosters.length} Hoster bereit`, "ready");
  else setFpDetailAvailability("Kein Hoster verfügbar", "error");
  document.getElementById("fp-detail-route-label").textContent = metadataOnly ? "Originaltitel" : "Route";
  document.getElementById("fp-detail-score-label").textContent = metadataOnly ? "Bewertung" : "Signal";
  document.getElementById("fp-detail-fallback-label").textContent = metadataOnly ? "Veröffentlichung" : "Fallback";
  document.getElementById("fp-detail-route").textContent = metadataOnly ? (movie.original_title || "—") : (movie.hoster_route || "—");
  document.getElementById("fp-detail-score").textContent = metadataOnly
    ? (movie.rating ? `${movie.rating}/10${movie.vote_count ? ` · ${movie.vote_count} Stimmen` : ""}` : "—")
    : (movie.hoster_score != null ? String(movie.hoster_score) : "—");
  document.getElementById("fp-detail-fallback").textContent = metadataOnly
    ? (movie.release_date || "—")
    : (movie.hosters.length ? `${movie.hoster_fallback_count} Alternativen` : "—");
  document.getElementById("fp-detail-desc").textContent = movie.description || "(keine Beschreibung)";

  configureFpDetailAction(slug, movie, metadataOnly);
}

// ── Serien-Tab ─────────────────────────────────────────────────────────────
function buildAlphaBar() {
  const bar = document.getElementById("series-alpha-bar");
  const letters = ["0-9", ...Array.from({ length: 26 }, (_, i) => String.fromCharCode(65 + i))];
  for (const l of letters) {
    const btn = document.createElement("button");
    btn.textContent = l;
    btn.addEventListener("click", () => seriesBrowse(`alpha:${l}`, 1));
    bar.appendChild(btn);
  }
}

function firstEpisodeSlug(series) {
  for (const s of series.seasons) if (s.episodes.length) return s.episodes[0].slug;
  return "";
}

function seriesEpisodes(series = state.series.current) {
  return series?.seasons?.flatMap((season) => season.episodes || []) || [];
}

function isEpisodeQueued(episode) {
  return Boolean(episode?.queued || state.queuedSlugs.has(episode?.slug));
}

function isEpisodeSelectable(episode) {
  return Boolean(
    episode
    && state.series.current?.availability_pending !== true
    && state.series.current?.jellyfin_pending !== true
    && state.series.current?.jellyfin_available !== false
    && !episode.downloaded
    && !episode.in_jellyfin
    && !isEpisodeQueued(episode)
  );
}

function syncSeriesQueueFlags(series = null) {
  const candidates = series
    ? [series]
    : [state.series.current, ...Object.values(state.series.cache)];
  const visited = new Set();
  for (const candidate of candidates) {
    if (!candidate || visited.has(candidate)) continue;
    visited.add(candidate);
    if (state.queue.loaded) {
      for (const episode of seriesEpisodes(candidate)) {
        episode.queued = state.queuedSlugs.has(episode.slug);
      }
    }
  }
  if (!series || series === state.series.current) {
    pruneSeriesEpisodeSelection();
    renderSeriesTiles();
  }
}

function pruneSeriesEpisodeSelection() {
  const selectableSlugs = new Set(
    seriesEpisodes().filter(isEpisodeSelectable).map((episode) => episode.slug),
  );
  state.series.epPicked = new Set(
    [...state.series.epPicked].filter((slug) => selectableSlugs.has(slug)),
  );
}

function findCurrentEpisode(slug) {
  return seriesEpisodes().find((episode) => episode.slug === slug) || null;
}

function updateSeriesPager() {
  const pager = document.getElementById("series-pager");
  const mode = state.series.browseMode;
  pager.classList.remove("hidden");
  const canPaginate = Boolean(mode && mode !== "search");
  const sourceCount = state.series.sources.length;
  const sourceWord = sourceCount === 1 ? "Quelle" : "Quellen";
  document.getElementById("series-pager-label").textContent = sourceCount
    ? `Seite ${state.series.page} · ${sourceCount} ${sourceWord}`
    : `Seite ${state.series.page || 1}`;
  const sourceSummary = state.series.sources
    .map((source) => `${source.label} ${source.count}`)
    .join(" · ");
  const sourceElement = document.getElementById("series-pager-sources");
  sourceElement.textContent = sourceSummary;
  sourceElement.title = sourceSummary;
  document.getElementById("series-pager-prev").disabled = !canPaginate || state.series.page <= 1;
  document.getElementById("series-pager-next").disabled = !canPaginate || !state.series.lastPageFull;
}

function renderSeriesResults() {
  const container = document.getElementById("series-results");
  container.innerHTML = "";

  for (const result of state.series.results) {
    const selectedBase = state.series.pendingBaseSlug || state.series.current?.base_slug;
    const selected = selectedBase === result.base_slug;
    const loading = state.series.pendingBaseSlug === result.base_slug;
    const resultSources = Array.isArray(result.sources) ? result.sources : [];
    const sourceLabels = resultSources.map((source) => source.label).filter(Boolean);
    const sourceSummary = sourceLabels.length > 1
      ? `${sourceLabels.length} Quellen`
      : (sourceLabels[0] || result.provider_label || "Quelle offen");

    const row = document.createElement("div");
    row.className = "series-row result-card" + (selected ? " selected" : "") + (loading ? " loading" : "");
    row.dataset.baseSlug = result.base_slug;
    row.setAttribute("aria-current", String(selected));
    row.setAttribute("aria-label", [result.title, result.year].filter(Boolean).join(", "));
    if (loading) row.setAttribute("aria-busy", "true");

    const visual = createResultCardVisual(result, result.title, "series");
    const copy = document.createElement("span");
    copy.className = "result-card-copy";
    const title = document.createElement("strong");
    title.className = "result-card-title";
    title.textContent = result.title;
    const subtitle = document.createElement("span");
    subtitle.className = "result-card-subtitle";
    subtitle.textContent = sourceSummary;
    subtitle.title = sourceLabels.join(" · ");
    const meta = document.createElement("span");
    meta.className = "result-card-meta";
    const year = document.createElement("span");
    year.textContent = result.year || "Jahr offen";
    const stateLabel = document.createElement("span");
    stateLabel.className = "result-card-state status-ready";
    stateLabel.textContent = loading ? "Öffnet …" : "Staffeln öffnen";
    meta.append(year, stateLabel);
    copy.append(title, subtitle, meta);

    row.append(visual, copy);
    activateResultCard(row, () => loadSeries(result));
    container.appendChild(row);
  }
}

function findSeriesResultCard(baseSlug) {
  return [...document.querySelectorAll("#series-results .series-row")]
    .find((row) => row.dataset.baseSlug === baseSlug) || null;
}

function updateSeriesResultSelection() {
  const selectedBase = state.series.pendingBaseSlug || state.series.current?.base_slug;
  document.querySelectorAll("#series-results .series-row").forEach((row) => {
    const loading = state.series.pendingBaseSlug === row.dataset.baseSlug;
    const selected = selectedBase === row.dataset.baseSlug;
    row.classList.toggle("selected", selected);
    row.classList.toggle("loading", loading);
    row.setAttribute("aria-current", String(selected));
    if (loading) row.setAttribute("aria-busy", "true");
    else row.removeAttribute("aria-busy");
  });
}

function applySeriesResults(data) {
  state.series.results = Array.isArray(data.results) ? data.results : [];
  state.series.page = data.page || 1;
  state.series.lastPageFull = Boolean(data.has_more ?? data.last_page_full);
  state.series.sources = Array.isArray(data.sources)
    ? data.sources.filter((source) => Number(source.count) > 0)
    : [];
  renderSeriesResults();
  updateSeriesPager();
  const sourceCount = state.series.sources.length;
  document.getElementById("series-status").textContent =
    state.series.results.length
      ? (sourceCount
        ? `${state.series.results.length} Serie(n) · ${sourceCount} ${sourceCount === 1 ? "Quelle" : "Quellen"}`
        : `${state.series.results.length} Serie(n) gefunden`)
      : "Keine Serie gefunden.";
}

async function seriesSearch() {
  const q = document.getElementById("series-search").value.trim();
  if (!q) return;
  const requestId = ++state.series.browseRequestSeq;
  const previousMode = state.series.browseMode;
  state.series.browseMode = "search";
  state.series.loadingBrowse = true;
  updateSeriesPager();
  document.getElementById("series-status").textContent = `Suche nach «${q}» …`;
  try {
    const data = await api.series({ mode: "search", query: q });
    if (requestId !== state.series.browseRequestSeq) return;
    applySeriesResults(data);
    if (data.direct_series) {
      showSeriesDetail(data.direct_series, firstEpisodeSlug(data.direct_series));
      updateSeriesStatus(data.direct_series);
      refreshSeriesJellyfinStatus();
    }
  } catch (error) {
    if (requestId !== state.series.browseRequestSeq) return;
    state.series.browseMode = state.series.results.length ? previousMode : null;
    updateSeriesPager();
    document.getElementById("series-status").textContent = `Fehler: ${error.message}`;
  } finally {
    if (requestId === state.series.browseRequestSeq) state.series.loadingBrowse = false;
  }
}

function seriesParams(mode, page) {
  // Alpha-Modi kommen als "alpha:X"; "new"/"trending" direkt als Modusname.
  return mode.startsWith("alpha:")
    ? { mode: "alpha", letter: mode.split(":")[1], page }
    : { mode, page };
}

async function seriesBrowse(mode, page) {
  const requestId = ++state.series.browseRequestSeq;
  const previousMode = state.series.browseMode;
  state.series.browseMode = mode;
  state.series.loadingBrowse = true;
  updateSeriesPager();
  const modeLabels = { discover: "interessante Serien", new: "neue Serien", trending: "angesagte Serien" };
  document.getElementById("series-status").textContent = `Lade ${modeLabels[mode] || "Serien"} …`;
  try {
    const data = await api.series(seriesParams(mode, page));
    if (requestId !== state.series.browseRequestSeq) return false;
    applySeriesResults(data);
    return true;
  } catch (error) {
    if (requestId !== state.series.browseRequestSeq) return false;
    document.getElementById("series-status").textContent = `Fehler: ${error.message}`;
    state.series.browseMode = state.series.results.length ? previousMode : null;
    updateSeriesPager();
    return false;
  } finally {
    if (requestId === state.series.browseRequestSeq) state.series.loadingBrowse = false;
  }
}

function ensureSeriesResults() {
  if (state.series.results.length || state.series.loadingBrowse) return;
  seriesBrowse("discover", 1);
}

async function seriesPagerChange(delta) {
  const mode = state.series.browseMode;
  if (!mode || mode === "search") return;
  const newPage = state.series.page + delta;
  if (newPage < 1) return;
  if (await seriesBrowse(mode, newPage)) {
    scrollCatalogToStart("tab-serien", "series-results");
  }
}

async function loadSeries(result) {
  const cacheKey = result.base_slug || result.sample_slug;
  if (state.series.pendingBaseSlug === cacheKey) return;
  const requestId = ++state.series.requestSeq;
  state.series.pendingBaseSlug = cacheKey;
  updateSeriesResultSelection();
  showSeriesLoading(result);
  openMediaModal("series-detail-modal", findSeriesResultCard(result.base_slug));

  const cached = state.series.cache[cacheKey];
  if (cached) {
    showSeriesDetail(cached, result.sample_slug);
    updateSeriesStatus(cached);
    refreshSeriesJellyfinStatus();
    return;
  }

  document.getElementById("series-status").textContent = `Öffne Staffeln für «${result.title}» …`;
  try {
    const series = await api.seriesLoad(result.sample_slug, result.base_slug || "", false, true);
    if (requestId !== state.series.requestSeq) return;
    showSeriesDetail(series, result.sample_slug);
    updateSeriesStatus(series);
    refreshSeriesJellyfinStatus();
  } catch (e) {
    if (requestId !== state.series.requestSeq) return;
    state.series.pendingBaseSlug = "";
    updateSeriesResultSelection();
    document.getElementById("series-status").textContent = `Fehler: ${e.message}`;
    document.getElementById("series-detail-title").textContent = `${result.title} · Laden fehlgeschlagen`;
    document.getElementById("series-desc").textContent = e.message;
    const loading = document.querySelector("#series-tiles .series-loading");
    if (loading) loading.textContent = "Serie konnte nicht geladen werden";
  }
}

function showSeriesLoading(result) {
  state.series.viewGeneration += 1;
  state.series.current = null;
  document.getElementById("series-detail-title").textContent = result.title;
  const cover = document.getElementById("series-cover");
  if (result.cover_url) cover.src = api.coverUrl(result.cover_url);
  else cover.removeAttribute("src");
  const sourceLabels = (Array.isArray(result.sources) ? result.sources : [])
    .map((source) => source.label)
    .filter(Boolean);
  const previewMeta = [result.year, ...sourceLabels].filter(Boolean);
  if (!sourceLabels.length && result.provider_label) previewMeta.push(result.provider_label);
  document.getElementById("series-genres").textContent = previewMeta.join(" · ");
  document.getElementById("series-desc").textContent =
    "Die Serie ist geöffnet. Staffel- und Episodenstruktur wird beim Anbieter eingelesen.";
  const tiles = document.getElementById("series-tiles");
  tiles.replaceChildren();
  const loading = document.createElement("div");
  loading.className = "series-loading";
  loading.textContent = "Staffeln werden eingelesen …";
  tiles.appendChild(loading);
  document.getElementById("series-pick-count").textContent = "wird geladen";
  document.getElementById("series-watch-btn").disabled = true;
  document.getElementById("series-select-all").disabled = true;
  document.getElementById("series-select-none").disabled = true;
  document.getElementById("series-add-btn").disabled = true;
}

function updateWatchBtn() {
  const btn = document.getElementById("series-watch-btn");
  const series = state.series.current;
  if (!series) return;
  const tracked = series.watchlisted;
  const label = WATCH_MODE_LABELS[series.watch_mode] || WATCH_MODE_LABELS[WATCH_MODE_DEFAULT];
  btn.textContent = tracked ? `✓ Abo · ${label}` : "+ Abonnieren";
  btn.title = tracked ? "Abo-Regel ändern" : "Serie abonnieren und Downloadumfang festlegen";
  btn.classList.toggle("btn-accent", tracked);
}

function showSeriesDetail(series, sampleSlug) {
  state.series.viewGeneration += 1;
  syncSeriesQueueFlags(series);
  state.series.current = series;
  state.series.currentSampleSlug = sampleSlug;
  state.series.cache[series.base_slug] = series;
  state.series.pendingBaseSlug = "";
  state.series.epPicked = new Set();
  updateSeriesResultSelection();
  document.getElementById("series-detail-title").textContent =
    `${series.title}  ·  ${series.seasons.length} Staffel(n)  ·  ${series.episode_count} Episoden`;
  const cover = document.getElementById("series-cover");
  if (series.cover_url) cover.src = api.coverUrl(series.cover_url);
  else cover.removeAttribute("src");
  const seriesMeta = [];
  if (series.year) seriesMeta.push(series.year);
  if (series.runtime) seriesMeta.push(series.runtime);
  seriesMeta.push(...(series.genres || []));
  if (series.metadata_source) seriesMeta.push(`Metadaten: ${series.metadata_source}`);
  document.getElementById("series-genres").textContent = seriesMeta.join(" · ");
  document.getElementById("series-desc").textContent = series.description || "(keine Beschreibung verfügbar)";
  document.getElementById("series-watch-btn").disabled = false;
  document.getElementById("series-select-all").disabled = false;
  document.getElementById("series-select-none").disabled = false;
  updateWatchBtn();
  renderSeriesTiles();
  openMediaModal("series-detail-modal", findSeriesResultCard(series.base_slug));
}

function tileClass(ep) {
  if (isEpisodeQueued(ep)) return "queued";
  if (ep.downloaded) return "downloaded";
  if (state.series.epPicked.has(ep.slug) && isEpisodeSelectable(ep)) return "selected";
  return "available";
}

function renderSeriesTiles() {
  const container = document.getElementById("series-tiles");
  container.innerHTML = "";
  const series = state.series.current;
  if (!series) { document.getElementById("series-pick-count").textContent = "0 ausgewählt"; return; }
  pruneSeriesEpisodeSelection();
  if (series.availability_pending) {
    const warning = document.createElement("div");
    warning.className = "series-loading";
    warning.textContent = series.availability_error
      ? "Auswahl pausiert: Die Verfügbarkeit konnte noch nicht geprüft werden."
      : "Staffeln sind da · Bestand und Metadaten werden im Hintergrund geprüft …";
    container.appendChild(warning);
  } else if (series.jellyfin_available === false) {
    const warning = document.createElement("div");
    warning.className = "series-loading";
    warning.textContent = "Auswahl pausiert: Jellyfin konnte nicht eindeutig abgeglichen werden.";
    container.appendChild(warning);
  }
  const selectableCount = seriesEpisodes(series).filter(isEpisodeSelectable).length;
  for (const seasonObj of series.seasons) {
    const pickedCount = seasonObj.episodes.filter((e) => state.series.epPicked.has(e.slug)).length;
    const row = document.createElement("div");
    row.className = "season-row";
    const seasonBtn = document.createElement("button");
    seasonBtn.className = "season-btn";
    seasonBtn.textContent = `Staffel ${String(seasonObj.season).padStart(2, "0")}  ·  ${pickedCount}/${seasonObj.episodes.length}`;
    seasonBtn.disabled = !seasonObj.episodes.some(isEpisodeSelectable);
    seasonBtn.addEventListener("click", () => toggleSeasonTiles(seasonObj.season));
    row.appendChild(seasonBtn);
    const tiles = document.createElement("div");
    tiles.className = "ep-tiles";
    for (const ep of seasonObj.episodes) {
      const tile = document.createElement("button");
      tile.className = "ep-tile " + tileClass(ep) + (ep.in_jellyfin ? " in-jellyfin" : "");
      tile.textContent = String(ep.episode).padStart(2, "0");
      tile.disabled = !isEpisodeSelectable(ep);
      if (series.availability_error) tile.title = "Verfügbarkeitsprüfung fehlgeschlagen";
      else if (series.availability_pending) tile.title = "Verfügbarkeit wird geprüft";
      else if (ep.in_jellyfin) tile.title = "Bereits in Jellyfin vorhanden";
      else if (ep.downloaded) tile.title = "Bereits heruntergeladen";
      else if (isEpisodeQueued(ep)) tile.title = "Bereits in der Warteschlange";
      tile.addEventListener("click", () => toggleEpisodeTile(ep.slug));
      tiles.appendChild(tile);
    }
    row.appendChild(tiles);
    container.appendChild(row);
  }
  document.getElementById("series-pick-count").textContent = `${state.series.epPicked.size} ausgewählt`;
  document.getElementById("series-select-all").disabled = selectableCount === 0;
  document.getElementById("series-select-none").disabled = state.series.epPicked.size === 0;
  document.getElementById("series-add-btn").disabled = state.series.epPicked.size === 0;
}

function toggleEpisodeTile(slug) {
  const episode = findCurrentEpisode(slug);
  if (!isEpisodeSelectable(episode)) {
    state.series.epPicked.delete(slug);
    renderSeriesTiles();
    return;
  }
  if (state.series.epPicked.has(slug)) state.series.epPicked.delete(slug);
  else state.series.epPicked.add(slug);
  renderSeriesTiles();
}

function toggleSeasonTiles(season) {
  const seasonObj = state.series.current.seasons.find((s) => s.season === season);
  if (!seasonObj) return;
  const selectable = seasonObj.episodes.filter(isEpisodeSelectable);
  if (!selectable.length) return;
  const allPicked = selectable.every((episode) => state.series.epPicked.has(episode.slug));
  for (const ep of seasonObj.episodes) {
    if (!isEpisodeSelectable(ep) || allPicked) state.series.epPicked.delete(ep.slug);
    else state.series.epPicked.add(ep.slug);
  }
  renderSeriesTiles();
}

function markSeriesSlugDownloaded(slug) {
  const series = state.series.current;
  if (!series) return;
  for (const s of series.seasons) {
    for (const ep of s.episodes) {
      if (ep.slug === slug) { ep.downloaded = true; renderSeriesTiles(); return; }
    }
  }
}

async function seriesAddSelected() {
  pruneSeriesEpisodeSelection();
  if (!state.series.epPicked.size) {
    document.getElementById("series-status").textContent =
      "Keine herunterladbaren Episoden ausgewählt.";
    renderSeriesTiles();
    return;
  }
  const slugs = [...state.series.epPicked];
  document.getElementById("series-status").textContent = `Lade ${slugs.length} Episode(n) …`;
  const addButton = document.getElementById("series-add-btn");
  addButton.disabled = true;
  try {
    const resp = await api.queueAdd(slugs);
    refreshQueueUiAfterChange(resp);
    document.getElementById("series-status").textContent =
      `${resp.added}/${slugs.length} Episode(n) automatisch gestartet`;
    state.series.epPicked.clear();
  } catch (error) {
    document.getElementById("series-status").textContent =
      `Download konnte nicht gestartet werden: ${error.message}`;
  } finally {
    renderSeriesTiles();
  }
}

function closeWatchModeModal() {
  document.getElementById("watch-mode-modal").classList.add("hidden");
  document.getElementById("watch-mode-status").textContent = "";
  watchModeContext = null;
  if (watchModeReturnFocus instanceof HTMLElement && watchModeReturnFocus.isConnected) {
    watchModeReturnFocus.focus();
  }
  watchModeReturnFocus = null;
}

function openWatchModeModal(entry = null) {
  const series = state.series.current;
  const baseSlug = entry?.base_slug || series?.base_slug;
  if (!baseSlug) return;
  const stored = entry || state.wl.items.find((item) => item.base_slug === baseSlug);
  const tracked = Boolean(stored || series?.watchlisted);
  const mode = stored?.download_mode || series?.watch_mode || WATCH_MODE_DEFAULT;
  const cleanupMode = tracked
    ? (stored?.cleanup_mode || series?.cleanup_mode || WATCH_CLEANUP_DEFAULT)
    : state.watchlistCleanupDefault;
  watchModeReturnFocus = document.activeElement;
  const knownSlugs = series?.base_slug === baseSlug
    ? series.seasons.flatMap((season) => season.episodes.map((episode) => episode.slug))
    : (stored?.known_slugs || []);
  watchModeContext = {
    baseSlug,
    title: stored?.title || series?.title || baseSlug,
    sampleUrl: stored?.sample_url || series?.url || "",
    knownSlugs,
    tmdbId: stored?.tmdb_id || series?.tmdb_id || null,
    aliases: stored?.aliases || series?.aliases || [],
    seasonEpisodeCounts: stored?.season_episode_counts || series?.season_episode_counts || {},
    seasonCountsCheckedAt: stored?.season_counts_checked_at || series?.season_counts_checked_at || 0,
    tracked,
  };

  document.getElementById("watch-mode-title").textContent = watchModeContext.title;
  document.querySelectorAll('input[name="watch-mode"]').forEach((radio) => {
    radio.checked = radio.value === mode;
  });
  document.querySelectorAll('input[name="watch-cleanup"]').forEach((radio) => {
    radio.checked = radio.value === cleanupMode;
  });
  document.getElementById("watch-cleanup-description").textContent = tracked
    ? "Diese Löschregel gilt nur für diese Serie und nutzt den Gesehen-Status des gewählten Jellyfin-Profils."
    : `Vorausgewählt aus den Einstellungen: ${WATCH_CLEANUP_LABELS[cleanupMode] || WATCH_CLEANUP_LABELS[WATCH_CLEANUP_DEFAULT]}. Du kannst für diese Serie abweichen.`;
  document.getElementById("watch-mode-remove").classList.toggle("hidden", !tracked);
  document.getElementById("watch-mode-save").textContent = tracked ? "Regel übernehmen" : "Abo speichern";
  document.getElementById("watch-mode-status").textContent = "";
  document.getElementById("watch-mode-modal").classList.remove("hidden");
  updateWatchModeRequirement();
  setTimeout(() => document.querySelector('input[name="watch-mode"]:checked')?.focus(), 0);
}

function updateWatchModeRequirement() {
  const selected = document.querySelector('input[name="watch-mode"]:checked')?.value;
  const cleanupSelected = document.querySelector('input[name="watch-cleanup"]:checked')?.value
    || WATCH_CLEANUP_DEFAULT;
  const status = document.getElementById("watch-mode-status");
  const explanation = WATCH_MODE_EXPLANATIONS[selected] || WATCH_MODE_EXPLANATIONS[WATCH_MODE_DEFAULT];
  document.getElementById("watch-mode-outcome-title").textContent = explanation.title;
  document.getElementById("watch-mode-outcome-copy").textContent = explanation.copy;
  if (!state.jellyfinUserConfigured && (selected === "next_season" || cleanupSelected !== WATCH_CLEANUP_DEFAULT)) {
    const affected = selected === "next_season" && cleanupSelected !== WATCH_CLEANUP_DEFAULT
      ? "Download- und Löschregel warten"
      : (selected === "next_season" ? "Die Downloadregel wartet" : "Die Löschregel wartet");
    status.textContent = `Voraussetzung fehlt: Wähle unter Einstellungen → Jellyfin ein Wiedergabeprofil. ${affected}.`;
  } else if (status.textContent.startsWith("Diese Regel wartet")) {
    status.textContent = "";
  } else if (status.textContent.startsWith("Voraussetzung fehlt")) {
    status.textContent = "";
  }
}

async function saveWatchMode() {
  if (!watchModeContext) return;
  const selected = document.querySelector('input[name="watch-mode"]:checked')?.value;
  const cleanupSelected = document.querySelector('input[name="watch-cleanup"]:checked')?.value
    || WATCH_CLEANUP_DEFAULT;
  if (!selected) return;
  const saveBtn = document.getElementById("watch-mode-save");
  saveBtn.disabled = true;
  try {
    const data = watchModeContext.tracked
      ? await api.watchlistMode(watchModeContext.baseSlug, selected, cleanupSelected)
      : await api.watchlistAdd({
        base_slug: watchModeContext.baseSlug,
        title: watchModeContext.title,
        sample_url: watchModeContext.sampleUrl,
        known_slugs: watchModeContext.knownSlugs,
        download_mode: selected,
        cleanup_mode: cleanupSelected,
        tmdb_id: watchModeContext.tmdbId,
        aliases: watchModeContext.aliases,
        season_episode_counts: watchModeContext.seasonEpisodeCounts,
        season_counts_checked_at: watchModeContext.seasonCountsCheckedAt,
      });
    if (state.series.current?.base_slug === watchModeContext.baseSlug) {
      state.series.current.watchlisted = true;
      state.series.current.watch_mode = selected;
      state.series.current.cleanup_mode = cleanupSelected;
    }
    applyWatchlist(data.watchlist);
    closeWatchModeModal();
  } catch (error) {
    document.getElementById("watch-mode-status").textContent = error.message;
  } finally {
    saveBtn.disabled = false;
  }
}

async function removeWatchModeSubscription() {
  if (!watchModeContext?.tracked) return;
  const data = await api.watchlistRemove([watchModeContext.baseSlug]);
  applyWatchlist(data.watchlist);
  await syncQueueSnapshot("Queue-Synchronisierung nach Abo-Entfernung");
  closeWatchModeModal();
}

// ── Bibliothek-Tab ─────────────────────────────────────────────────────────
function applyWatchlist(items) {
  watchlistSnapshotGeneration += 1;
  state.wl.items = items;
  state.wl.loaded = true;
  for (const series of Object.values(state.series.cache)) {
    const entry = items.find((item) => item.base_slug === series.base_slug);
    series.watchlisted = Boolean(entry);
    series.watch_mode = entry?.download_mode || WATCH_MODE_DEFAULT;
    series.cleanup_mode = entry?.cleanup_mode || WATCH_CLEANUP_DEFAULT;
  }
  if (state.series.current) updateWatchBtn();
  renderWatchlist();
  renderSeriesSubscriptions();
  renderNotifBell();
}

function subscriptionMonogram(title) {
  const words = String(title || "").trim().split(/\s+/).filter(Boolean);
  return (words.length > 1 ? words[0][0] + words[1][0] : (words[0] || "?").slice(0, 2)).toUpperCase();
}

function watchlistStatusText(entry) {
  if (entry.status === "blocked") return entry.last_error || "Prüfung blockiert";
  if (entry.status === "failed") return `${entry.failed_count || 1} fehlgeschlagen · Retry geplant`;
  if (entry.cleanup_last_error) return `Löschen pausiert · ${entry.cleanup_last_error}`;
  if (entry.status === "queued") return `${entry.queued_count || entry.new_count} in der Queue`;
  if (entry.status === "waiting_window") return `${entry.new_count} warten auf Zeitfenster`;
  if (entry.new_count) return `${entry.new_count} fehlen`;
  return "vollständig";
}

function renderSeriesSubscriptions() {
  const container = document.getElementById("series-subscriptions-list");
  if (!container) return;
  const items = state.wl.items;
  document.getElementById("series-subscriptions-count").textContent =
    `${items.length} ${items.length === 1 ? "Serie" : "Serien"}`;
  container.innerHTML = "";

  if (!items.length) {
    const empty = document.createElement("div");
    empty.className = "subscriptions-empty";
    empty.textContent = "Noch keine Abos – Serie auswählen und auf „Abonnieren“ klicken.";
    container.appendChild(empty);
    return;
  }

  for (const entry of items) {
    const card = document.createElement("button");
    card.type = "button";
    card.className = "subscription-card" + (entry.new_count || entry.cleanup_last_error || entry.status === "blocked" || entry.status === "failed" ? " has-new" : "");
    card.title = `${entry.title} öffnen`;

    const monogram = document.createElement("span");
    monogram.className = "subscription-monogram";
    monogram.textContent = subscriptionMonogram(entry.title);

    const text = document.createElement("span");
    text.className = "subscription-text";
    const title = document.createElement("span");
    title.className = "subscription-name";
    title.textContent = entry.title;
    const meta = document.createElement("span");
    meta.className = "subscription-meta";
    const modeLabel = entry.download_mode_label || WATCH_MODE_LABELS[entry.download_mode] || WATCH_MODE_LABELS[WATCH_MODE_DEFAULT];
    const cleanupLabel = WATCH_CLEANUP_LABELS[entry.cleanup_mode] || WATCH_CLEANUP_LABELS[WATCH_CLEANUP_DEFAULT];
    meta.textContent = `${modeLabel}${entry.cleanup_mode !== WATCH_CLEANUP_DEFAULT ? ` · ${cleanupLabel}` : ""} · ${watchlistStatusText(entry)}`;
    text.append(title, meta);
    card.append(monogram, text);

    if (entry.new_count) {
      const badge = document.createElement("span");
      badge.className = "subscription-new";
      badge.textContent = `+${entry.new_count}`;
      card.appendChild(badge);
    }
    card.addEventListener("click", () => openWatchlistEntry(entry.base_slug));
    container.appendChild(card);
  }
}

async function refreshWatchlist() {
  return syncWatchlistSnapshot("Abo-Aktualisierung");
}

// ── Benachrichtigungs-Glocke ─────────────────────────────────────────────
function renderNotifBell() {
  const withNotice = state.wl.items.filter((e) => e.new_count || e.cleanup_last_error || e.status === "blocked" || e.status === "failed");
  const total = withNotice.reduce((sum, e) => sum + e.new_count, 0);
  const issueCount = withNotice.filter((entry) => entry.cleanup_last_error || entry.status === "blocked" || entry.status === "failed").length;
  const bell = document.getElementById("notif-bell");
  const badge = document.getElementById("notif-badge");
  const triggerLabel = document.getElementById("notif-trigger-label");
  badge.textContent = total ? String(total) : "!";
  badge.classList.toggle("hidden", total === 0 && issueCount === 0);
  bell.classList.toggle("is-active", total > 0 || issueCount > 0);
  bell.setAttribute("aria-label", total || issueCount
    ? `Abo-Postfach öffnen: ${total} fehlende Episoden, ${issueCount} Probleme`
    : "Abo-Postfach öffnen: alles aktuell");
  triggerLabel.textContent = total
    ? `${total} ${total === 1 ? "Episode fehlt" : "Episoden fehlen"}`
    : (issueCount ? `${issueCount} ${issueCount === 1 ? "Problem" : "Probleme"}` : "Alles aktuell");
  document.getElementById("notif-summary").textContent = total || issueCount
    ? `${total} fehlend · ${issueCount} problematisch`
    : "Alles vollständig";
  document.getElementById("notif-subscription-count").textContent =
    `${state.wl.items.length} ${state.wl.items.length === 1 ? "Abo" : "Abos"}`;

  const list = document.getElementById("notif-list");
  list.innerHTML = "";
  if (!withNotice.length) {
    list.innerHTML = `<div class="notif-empty"><span class="notif-empty-seal">✓</span><strong>Alles vollständig</strong><small>Abonnierte Serien werden weiter automatisch auf fehlende Episoden geprüft.</small></div>`;
    return;
  }
  const sorted = [...withNotice].sort((a, b) =>
    (b.failed_count || 0) - (a.failed_count || 0)
    || (b.new_count || 0) - (a.new_count || 0)
    || a.title.localeCompare(b.title, "de"));
  for (const entry of sorted) {
    const item = document.createElement("button");
    item.type = "button";
    item.className = "notif-item";
    const mark = document.createElement("span");
    mark.className = "notif-item-mark";
    mark.textContent = subscriptionMonogram(entry.title);
    const copy = document.createElement("span");
    copy.className = "notif-item-copy";
    const title = document.createElement("strong");
    title.textContent = entry.title;
    const mode = document.createElement("small");
    mode.textContent = watchlistStatusText(entry);
    copy.append(title, mode);
    const count = document.createElement("span");
    count.className = "notif-count";
    const countValue = document.createElement("strong");
    countValue.textContent = entry.status === "blocked" || entry.cleanup_last_error ? "!" : String(entry.failed_count || entry.new_count);
    const countLabel = document.createElement("small");
    countLabel.textContent = entry.status === "blocked"
      ? "Blockiert"
      : (entry.status === "failed"
        ? "Fehler"
        : (entry.cleanup_last_error ? "Löschen" : (entry.new_count === 1 ? "Episode" : "Episoden")));
    count.append(countValue, countLabel);
    const arrow = document.createElement("span");
    arrow.className = "notif-item-arrow";
    arrow.textContent = "›";
    item.append(mark, copy, count, arrow);
    item.addEventListener("click", () => {
      closeNotifDropdown();
      openWatchlistEntry(entry.base_slug);
    });
    list.appendChild(item);
  }
}

function toggleNotifDropdown() {
  const dropdown = document.getElementById("notif-dropdown");
  const open = dropdown.classList.contains("hidden");
  dropdown.classList.toggle("hidden", !open);
  document.getElementById("notif-bell").setAttribute("aria-expanded", String(open));
}

function closeNotifDropdown() {
  document.getElementById("notif-dropdown").classList.add("hidden");
  document.getElementById("notif-bell").setAttribute("aria-expanded", "false");
}

async function refreshNotifications() {
  const button = document.getElementById("notif-refresh");
  button.disabled = true;
  button.classList.add("is-loading");
  document.getElementById("notif-summary").textContent = "Abonnements werden geprüft …";
  try {
    const data = await api.watchlistCheck(null);
    applyWatchlist(data.watchlist);
  } catch (error) {
    document.getElementById("notif-summary").textContent = `Prüfung fehlgeschlagen: ${error.message}`;
  } finally {
    button.disabled = false;
    button.classList.remove("is-loading");
  }
}

function renderWatchlist() {
  const container = document.getElementById("wl-list");
  container.innerHTML = "";
  const knownSlugs = new Set(state.wl.items.map((entry) => entry.base_slug));
  for (const slug of state.wl.selected) {
    if (!knownSlugs.has(slug)) state.wl.selected.delete(slug);
  }

  const attentionCount = state.wl.items.reduce((sum, entry) => {
    if (entry.new_count) return sum + entry.new_count;
    return sum + (entry.cleanup_last_error || entry.status === "blocked" || entry.status === "failed" ? 1 : 0);
  }, 0);
  document.getElementById("wl-total-count").textContent = String(state.wl.items.length);
  document.getElementById("wl-attention-count").textContent = String(attentionCount);
  document.getElementById("wl-selected-count").textContent = String(state.wl.selected.size);
  document.getElementById("wl-check-all").disabled = state.wl.items.length === 0;
  for (const id of ["wl-check-selected", "wl-open", "wl-remove"]) {
    document.getElementById(id).disabled = state.wl.selected.size === 0;
  }

  if (!state.wl.items.length) {
    const empty = document.createElement("div");
    empty.className = "library-empty";
    empty.innerHTML = `
      <span class="library-empty-mark" aria-hidden="true">◇</span>
      <strong>Dein Serienarchiv ist noch leer</strong>
      <span>Öffne eine Serie und wähle „Abonnieren“, um sie hier zu verwalten.</span>
    `;
    container.appendChild(empty);
    return;
  }

  state.wl.items.forEach((entry, index) => {
    const isSelected = state.wl.selected.has(entry.base_slug);
    const needsAttention = Boolean(
      entry.new_count || entry.cleanup_last_error || entry.status === "blocked" || entry.status === "failed"
    );
    const row = document.createElement("div");
    row.className = "wl-row library-card"
      + (isSelected ? " selected" : "")
      + (needsAttention ? " has-new" : "");
    row.tabIndex = 0;
    row.setAttribute("role", "checkbox");
    row.setAttribute("aria-checked", String(isSelected));

    const top = document.createElement("div");
    top.className = "library-card-top";
    const select = document.createElement("label");
    select.className = "library-card-select";
    const cb = document.createElement("input");
    cb.type = "checkbox";
    cb.checked = isSelected;
    cb.setAttribute("aria-label", `${entry.title} auswählen`);
    cb.addEventListener("click", (e) => { e.stopPropagation(); toggleWlSelect(entry.base_slug); });
    const archiveNumber = document.createElement("span");
    archiveNumber.textContent = `ABO ${String(index + 1).padStart(2, "0")}`;
    select.append(cb, archiveNumber);

    const stateBadge = document.createElement("span");
    stateBadge.className = `library-state is-${entry.status || "current"}`;
    stateBadge.textContent = ({
      blocked: "Blockiert",
      failed: "Fehler",
      queued: "In Queue",
      waiting_window: "Zeitfenster",
      missing: "Offen",
      current: "Aktuell",
    })[entry.status] || "Aktuell";
    top.append(select, stateBadge);

    const identity = document.createElement("div");
    identity.className = "library-card-identity";
    const monogram = document.createElement("span");
    monogram.className = "library-card-monogram";
    monogram.textContent = subscriptionMonogram(entry.title);
    const copy = document.createElement("span");
    copy.className = "library-card-copy";
    const title = document.createElement("strong");
    title.className = "library-card-title";
    title.textContent = entry.title;
    const statusText = document.createElement("span");
    statusText.className = "library-card-status";
    statusText.textContent = watchlistStatusText(entry);
    copy.append(title, statusText);
    identity.append(monogram, copy);

    const episodeStatus = document.createElement("div");
    episodeStatus.className = "library-episode-status";
    const episodeValue = document.createElement("strong");
    episodeValue.textContent = needsAttention
      ? (entry.status === "blocked" || entry.cleanup_last_error ? "!" : String(entry.failed_count || entry.new_count || "!"))
      : "✓";
    const episodeLabel = document.createElement("span");
    episodeLabel.textContent = needsAttention
      ? (entry.new_count === 1 ? "Episode offen" : (entry.new_count ? "Episoden offen" : "Prüfung nötig"))
      : "Vollständig";
    episodeStatus.append(episodeValue, episodeLabel);

    const footer = document.createElement("div");
    footer.className = "library-card-footer";
    const rule = document.createElement("button");
    rule.type = "button";
    rule.className = "wl-rule-btn";
    const downloadLabel = entry.download_mode_label || WATCH_MODE_LABELS[entry.download_mode] || WATCH_MODE_LABELS[WATCH_MODE_DEFAULT];
    const cleanupLabel = WATCH_CLEANUP_LABELS[entry.cleanup_mode] || WATCH_CLEANUP_LABELS[WATCH_CLEANUP_DEFAULT];
    rule.textContent = `${downloadLabel}${entry.cleanup_mode !== WATCH_CLEANUP_DEFAULT ? ` · ${cleanupLabel}` : ""}`;
    rule.title = "Abo- und Löschregel ändern";
    rule.addEventListener("click", (event) => {
      event.stopPropagation();
      openWatchModeModal(entry);
    });
    const open = document.createElement("button");
    open.type = "button";
    open.className = "library-card-open";
    open.textContent = "Öffnen  →";
    open.addEventListener("click", (event) => {
      event.stopPropagation();
      openWatchlistEntry(entry.base_slug);
    });
    footer.append(rule, open);

    row.append(top, identity, episodeStatus, footer);
    row.addEventListener("click", () => toggleWlSelect(entry.base_slug));
    row.addEventListener("dblclick", () => openWatchlistEntry(entry.base_slug));
    row.addEventListener("keydown", (event) => {
      if (event.target !== row || (event.key !== " " && event.key !== "Enter")) return;
      event.preventDefault();
      toggleWlSelect(entry.base_slug);
    });
    container.appendChild(row);
  });
}

function toggleWlSelect(baseSlug) {
  if (state.wl.selected.has(baseSlug)) state.wl.selected.delete(baseSlug);
  else state.wl.selected.add(baseSlug);
  renderWatchlist();
}

async function openWatchlistEntry(baseSlug) {
  switchTab("serien", { autoLoad: false });
  state.series.browseRequestSeq += 1;
  state.series.loadingBrowse = false;
  const openGeneration = ++state.series.viewGeneration;
  document.getElementById("series-status").textContent = "Lade abonnierte Serie …";
  try {
    const series = await api.watchlistOpen(baseSlug);
    if (state.series.viewGeneration !== openGeneration) return;
    const preselect = series.preselect_slugs || [];
    delete series.preselect_slugs;
    showSeriesDetail(series, firstEpisodeSlug(series));
    const selectable = new Set(
      seriesEpisodes(series).filter(isEpisodeSelectable).map((episode) => episode.slug),
    );
    state.series.epPicked = new Set(preselect.filter((slug) => selectable.has(slug)));
    renderSeriesTiles();
    await syncWatchlistSnapshot("Abo-Aktualisierung nach Öffnen");
  } catch (error) {
    if (state.series.viewGeneration !== openGeneration) return;
    document.getElementById("series-status").textContent =
      `Serie konnte nicht geöffnet werden: ${error.message}`;
  }
}

// ── Einstellungen (Speicherort) ──────────────────────────────────────────────
let dirModalPath = "";
let dirModalTarget = "save-path";   // welches Feld der Ordner-Dialog befüllt

function fillJellyfinUserSelect(selectId, users, selectedId = "", selectedName = "") {
  const select = document.getElementById(selectId);
  select.replaceChildren();
  const placeholder = document.createElement("option");
  placeholder.value = "";
  placeholder.textContent = users.length ? "Benutzer auswählen …" : "Benutzer laden …";
  select.appendChild(placeholder);
  for (const user of users) {
    const option = document.createElement("option");
    option.value = user.id;
    option.textContent = user.name;
    option.dataset.name = user.name;
    select.appendChild(option);
  }
  if (selectedId && !users.some((user) => user.id === selectedId)) {
    const option = document.createElement("option");
    option.value = selectedId;
    option.textContent = selectedName || "Gespeicherter Benutzer";
    option.dataset.name = selectedName || "";
    select.appendChild(option);
  }
  select.value = selectedId && [...select.options].some((option) => option.value === selectedId)
    ? selectedId
    : (users.length === 1 ? users[0].id : "");
}

async function loadJellyfinUsers({ urlId, keyId, selectId, buttonId, statusId = "" }) {
  const button = document.getElementById(buttonId);
  const select = document.getElementById(selectId);
  const status = statusId ? document.getElementById(statusId) : null;
  const url = document.getElementById(urlId).value.trim();
  const apiKey = document.getElementById(keyId).value.trim();
  button.disabled = true;
  if (status) status.textContent = "Lade Jellyfin-Benutzer …";
  try {
    const data = await api.jellyfinUsers(url, apiKey);
    const previous = select.value;
    fillJellyfinUserSelect(selectId, data.users || [], previous);
    if (status) status.textContent = data.users?.length
      ? `${data.users.length} ${data.users.length === 1 ? "Benutzer" : "Benutzer"} gefunden`
      : "Keine aktiven Jellyfin-Benutzer gefunden";
  } catch (error) {
    if (status) status.textContent = error.message;
    else setSetupStatus(error.message, true);
  } finally {
    button.disabled = false;
  }
}

function renderProviderPriority(mediaType) {
  const list = document.getElementById(mediaType === "movies" ? "movie-provider-priority" : "series-provider-priority");
  const providers = state.providers[mediaType] || [];
  list.innerHTML = providers.map((provider, index) => `
    <li class="provider-priority-item" data-provider="${escapeHtml(provider)}">
      <span class="provider-position">${String(index + 1).padStart(2, "0")}</span>
      <span class="provider-name">${escapeHtml(state.providers.labels[provider] || provider)}</span>
      <span class="provider-order-actions">
        <button class="provider-order-button" type="button" data-direction="-1"
          aria-label="${escapeHtml(state.providers.labels[provider] || provider)} nach oben"
          ${index === 0 ? "disabled" : ""}>↑</button>
        <button class="provider-order-button" type="button" data-direction="1"
          aria-label="${escapeHtml(state.providers.labels[provider] || provider)} nach unten"
          ${index === providers.length - 1 ? "disabled" : ""}>↓</button>
      </span>
    </li>
  `).join("");
  list.querySelectorAll(".provider-order-button").forEach((button) => {
    button.addEventListener("click", () => {
      const item = button.closest(".provider-priority-item");
      const from = state.providers[mediaType].indexOf(item.dataset.provider);
      const to = from + Number(button.dataset.direction);
      if (from < 0 || to < 0 || to >= state.providers[mediaType].length) return;
      [state.providers[mediaType][from], state.providers[mediaType][to]] =
        [state.providers[mediaType][to], state.providers[mediaType][from]];
      renderProviderPriority(mediaType);
    });
  });
}

function applyProviderPriority(cfg) {
  state.providers.movies = [...(cfg.movies || [])];
  state.providers.series = [...(cfg.series || [])];
  state.providers.labels = { ...(cfg.labels || {}) };
  renderProviderPriority("movies");
  renderProviderPriority("series");
}

async function initSettings() {
  const cfg = await api.configGet();
  document.getElementById("save-path").value = cfg.save_path;
  document.getElementById("series-path").value = cfg.series_path || "";
  const jf = await api.jellyfinConfigGet();
  document.getElementById("jellyfin-url").value = jf.url || "";
  const jfKey = document.getElementById("jellyfin-api-key");
  jfKey.value = "";
  jfKey.placeholder = jf.has_api_key ? "Gespeichert · leer lassen zum Beibehalten" : "API-Schlüssel";
  fillJellyfinUserSelect("jellyfin-user-id", [], jf.user_id || "", jf.user_name || "");
  state.watchlistCleanupDefault = WATCH_CLEANUP_LABELS[jf.cleanup_default]
    ? jf.cleanup_default
    : WATCH_CLEANUP_DEFAULT;
  document.querySelectorAll('input[name="jellyfin-cleanup-default"]').forEach((radio) => {
    radio.checked = radio.value === state.watchlistCleanupDefault;
  });
  state.jellyfinUserConfigured = !!(jf.url && jf.has_api_key && jf.user_id);
  document.getElementById("jellyfin-user-status").textContent = jf.user_id
    ? `Gesehen-Status: ${jf.user_name || "Benutzer gewählt"}`
    : "Für „Nächste Staffel“ und automatische Löschregeln erforderlich.";
  const tmdb = await api.tmdbConfigGet();
  applyTmdbCfg(tmdb);
  const auto = await api.automationConfigGet();
  applyAutomationCfg(auto);
  applyUpdaterConfig(await api.updaterConfigGet());
  const seerr = await api.seerrConfigGet();
  applySeerrCfg(seerr);
  const telegram = await api.telegramConfigGet();
  applyTelegramCfg(telegram);
  applyProviderPriority(await api.providerPriorityGet());
  checkForUpdates(false);
}

function applySeerrCfg(cfg) {
  document.getElementById("seerr-enabled").checked = !!cfg.enabled;
  document.getElementById("seerr-url").value = cfg.url || "";
  document.getElementById("seerr-poll-interval").value = cfg.poll_interval_seconds ?? 60;
  const key = document.getElementById("seerr-api-key");
  key.value = "";
  key.placeholder = cfg.has_api_key
    ? "Gespeichert · leer lassen zum Beibehalten"
    : "Seerr → Einstellungen → Allgemein";
  const status = document.getElementById("seerr-status");
  const counts = cfg.requests || {};
  const queued = (counts.queued || 0) + (counts.resolving || 0);
  if (!cfg.enabled) status.textContent = "Seerr-Brücke aus";
  else if (cfg.last_error) status.textContent = `✗ ${cfg.last_error}`;
  else if (cfg.moonfin_error) status.textContent = `Seerr aktiv · ${cfg.moonfin_error}`;
  else if (!cfg.connected) status.textContent = "Konfiguriert · Verbindung wird beim nächsten Abgleich geprüft";
  else status.textContent = `Verbunden${cfg.moonfin_configured ? " · Moonfin bereit" : ""} · ${queued} offen · ${counts.completed || 0} abgeschlossen`;
}

function applyTmdbCfg(cfg) {
  const input = document.getElementById("tmdb-api-key");
  input.value = "";
  input.placeholder = cfg.has_api_key ? "Gespeichert · leer lassen zum Beibehalten" : "TMDB API-Key";
  const status = document.getElementById("tmdb-status");
  if (!cfg.configured) status.textContent = "TMDB aus · Anbieterdaten werden verwendet";
  else if (cfg.valid === false) status.textContent = "✗ API-Key ungültig oder TMDB nicht erreichbar";
  else status.textContent = "TMDB aktiv · Sprache Deutsch";
}

function applyTelegramCfg(cfg) {
  document.getElementById("telegram-enabled").checked = !!cfg.enabled;
  const token = document.getElementById("telegram-token");
  token.value = "";
  token.placeholder = cfg.has_bot_token ? "Gespeichert · leer lassen zum Beibehalten" : "123456789:AA…";
  document.getElementById("telegram-chat-id").value = cfg.chat_id || "";
  const status = document.getElementById("telegram-status");
  if (!cfg.enabled) status.textContent = "Telegram-Bot aus";
  else if (!cfg.has_bot_token) status.textContent = "Bot-Token fehlt";
  else if (!cfg.chat_id) status.textContent = "Einrichtungsmodus · /start an den Bot senden";
  else status.textContent = `Aktiv · nur Chat ${cfg.chat_id}`;
}

function applyAutomationCfg(auto) {
  document.getElementById("auto-download").checked = !!auto.auto_download;
  document.getElementById("check-interval").value = auto.check_interval_min ?? 30;
  document.getElementById("dl-window-start").value =
    auto.dl_window_start === null || auto.dl_window_start === undefined ? "" : auto.dl_window_start;
  document.getElementById("dl-window-end").value =
    auto.dl_window_end === null || auto.dl_window_end === undefined ? "" : auto.dl_window_end;
  const st = document.getElementById("auto-status");
  if (!auto.auto_download) {
    st.textContent = "Auto-Download aus";
  } else {
    const win = (auto.dl_window_start === null || auto.dl_window_end === null)
      ? "jederzeit"
      : `${auto.dl_window_start}–${auto.dl_window_end} Uhr` + (auto.in_window ? " (aktiv)" : " (wartet)");
    st.textContent = `Auto-Download an · alle ${auto.check_interval_min} Min · ${win}`;
  }
}

function shortRevision(value) {
  const revision = String(value || "").trim();
  return revision ? revision.slice(0, 8) : "unbekannt";
}

function applyUpdaterConfig(cfg) {
  const mode = cfg.update_mode === "automatic" ? "automatic" : "manual";
  const interval = Math.max(1, Math.min(168, Number(cfg.auto_update_interval_hours) || 6));
  const modeSelect = document.getElementById("updater-mode");
  const intervalInput = document.getElementById("updater-interval");
  const status = document.getElementById("updater-mode-status");
  modeSelect.value = mode;
  intervalInput.value = String(interval);
  intervalInput.disabled = mode !== "automatic";

  if (mode !== "automatic") {
    status.textContent = "Manuell · Updates werden nur nach Klick installiert.";
    return;
  }
  if (cfg.auto_update_state === "deferred") {
    status.textContent = `Automatisch zurückgestellt · ${cfg.auto_update_message || "Download-Queue ist belegt."}`;
    return;
  }
  if (cfg.auto_update_state === "error") {
    status.textContent = `Automatische Prüfung fehlgeschlagen · ${cfg.auto_update_message || "Neuer Versuch folgt."}`;
    return;
  }
  if (["unavailable", "manual_required"].includes(cfg.auto_update_state)) {
    status.textContent = `Automatische Installation pausiert · ${cfg.auto_update_message || "Manuelle Prüfung erforderlich."}`;
    return;
  }
  if (cfg.auto_update_state === "installing") {
    status.textContent = "Automatisch · Update wird installiert.";
    return;
  }
  status.textContent = `Automatisch · alle ${interval} Std. · Installation nur bei leerer Queue.`;
}

function applyUpdaterStatus(data) {
  const card = document.getElementById("updater-card");
  const status = document.getElementById("updater-status");
  const detail = document.getElementById("updater-detail");
  const badge = document.getElementById("updater-badge");
  const repository = document.getElementById("updater-repository");
  const installButton = document.getElementById("updater-install");
  if (data.config) applyUpdaterConfig(data.config);
  document.getElementById("updater-current").textContent = shortRevision(data.current_sha);
  document.getElementById("updater-latest").textContent = shortRevision(data.latest_sha);
  installButton.dataset.sha = String(data.latest_sha || "");
  document.getElementById("updater-branch-label").textContent = `GitHub · ${data.branch || "main"}`;
  if (String(data.repository_url || "").startsWith("https://github.com/")) {
    repository.href = data.repository_url;
  }
  const installer = data.installer || {};
  if (installer.active || installer.state === "error") {
    installButton.classList.toggle("hidden", installer.state !== "error");
    applyUpdaterInstallStatus(installer);
    return;
  }
  installButton.disabled = installer.supported === false;
  installButton.title = installer.supported === false ? (installer.reason || "Automatisches Update nicht möglich") : "";
  installButton.classList.add("hidden");

  if (data.error) {
    card.dataset.state = "error";
    badge.textContent = "!";
    status.textContent = "GitHub-Prüfung fehlgeschlagen";
    detail.textContent = data.error;
    return;
  }
  if (data.update_available === true) {
    const commits = Number(data.ahead_by || 0);
    card.dataset.state = "available";
    badge.textContent = "↑";
    status.textContent = "Update verfügbar";
    detail.textContent = commits
      ? `${commits} ${commits === 1 ? "neuer Commit" : "neue Commits"} auf ${data.branch || "main"}`
      : `Neuer Stand auf ${data.branch || "main"}`;
    installButton.classList.remove("hidden");
    if (installer.supported === false) {
      detail.textContent += ` · ${installer.reason || "Automatische Installation nicht möglich"}`;
    }
    return;
  }
  if (data.comparison === "identical") {
    card.dataset.state = "current";
    badge.textContent = "✓";
    status.textContent = "Auf dem neuesten Stand";
    detail.textContent = data.latest_message || "Lokaler Build und GitHub stimmen überein.";
    return;
  }
  if (data.comparison === "behind") {
    card.dataset.state = "current";
    badge.textContent = "DEV";
    status.textContent = "Lokaler Entwicklungsstand";
    detail.textContent = "Dieser Build liegt vor dem Main-Branch.";
    return;
  }
  card.dataset.state = "unknown";
  badge.textContent = "?";
  status.textContent = "Repository erreichbar";
  detail.textContent = data.current_sha
    ? "Der lokale Stand konnte nicht eindeutig mit main verglichen werden."
    : "Der lokale Quellstand konnte weder Git-Metadaten noch einem GitHub-Dateibaum zugeordnet werden.";
}

let updaterInstallPollTimer = null;

function applyUpdaterInstallStatus(installer) {
  const card = document.getElementById("updater-card");
  const status = document.getElementById("updater-status");
  const detail = document.getElementById("updater-detail");
  const badge = document.getElementById("updater-badge");
  const checkButton = document.getElementById("updater-check");
  const installButton = document.getElementById("updater-install");
  const active = !!installer.active;
  card.dataset.installing = active ? "true" : "false";
  checkButton.disabled = active;
  installButton.disabled = active || installer.supported === false;
  if (installer.target_sha) installButton.dataset.sha = installer.target_sha;

  if (installer.state === "error") {
    card.dataset.state = "error";
    badge.textContent = "!";
    status.textContent = "Update fehlgeschlagen";
    detail.textContent = installer.error || installer.message || "Unbekannter Fehler";
    installButton.textContent = "Erneut versuchen";
    installButton.classList.remove("hidden");
    return;
  }
  if (!active) return;
  card.dataset.state = "checking";
  badge.textContent = installer.state === "restarting" ? "↻" : "↓";
  status.textContent = installer.message || "Update läuft";
  detail.textContent = installer.state === "restarting"
    ? "Die Oberfläche verbindet sich nach dem Neustart automatisch neu."
    : "Einstellungen, Abos und Downloads bleiben erhalten.";
  installButton.textContent = "Update läuft …";
  installButton.classList.remove("hidden");
  if (installer.state === "restarting") waitForUpdatedServer();
}

function scheduleUpdaterInstallPoll() {
  if (updaterInstallPollTimer) clearTimeout(updaterInstallPollTimer);
  updaterInstallPollTimer = setTimeout(async () => {
    try {
      const response = await api.updaterInstallStatus();
      const installer = response.installer || {};
      applyUpdaterInstallStatus(installer);
      if (installer.active && installer.state !== "restarting") scheduleUpdaterInstallPoll();
    } catch (error) {
      scheduleUpdaterInstallPoll();
    }
  }, 900);
}

async function waitForUpdatedServer() {
  if (updaterInstallPollTimer) clearTimeout(updaterInstallPollTimer);
  updaterInstallPollTimer = setTimeout(async () => {
    try {
      const response = await fetch("/api/health", { cache: "no-store" });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      location.reload();
    } catch (error) {
      waitForUpdatedServer();
    }
  }, 3000);
}

async function installUpdate() {
  const button = document.getElementById("updater-install");
  const targetSha = button.dataset.sha || "";
  if (!targetSha) return;
  button.disabled = true;
  try {
    const response = await api.updaterInstall(targetSha);
    applyUpdaterInstallStatus(response.installer || {});
    scheduleUpdaterInstallPoll();
  } catch (error) {
    applyUpdaterInstallStatus({ state: "error", error: error.message, supported: true });
  }
}

async function checkForUpdates(force = false) {
  const button = document.getElementById("updater-check");
  const card = document.getElementById("updater-card");
  const status = document.getElementById("updater-status");
  const detail = document.getElementById("updater-detail");
  button.disabled = true;
  card.dataset.state = "checking";
  status.textContent = "Prüfe GitHub …";
  detail.textContent = "Neuester Stand wird geladen.";
  try {
    applyUpdaterStatus(await api.updaterStatus(force));
  } catch (error) {
    applyUpdaterStatus({ error: error.message });
  } finally {
    button.disabled = card.dataset.installing === "true";
  }
}

async function saveAllSettings() {
  const btn = document.getElementById("settings-save");
  const status = document.getElementById("settings-saved-status");
  const parseHour = (id) => {
    const v = document.getElementById(id).value.trim();
    return v === "" ? null : Math.max(0, Math.min(23, parseInt(v, 10) || 0));
  };
  btn.disabled = true;
  status.textContent = "Speichere …";
  try {
    await api.configSet(
      document.getElementById("save-path").value.trim(),
      document.getElementById("series-path").value.trim(),
    );
    applyProviderPriority(await api.providerPrioritySet({
      movies: state.providers.movies,
      series: state.providers.series,
    }));
    const jfUserSelect = document.getElementById("jellyfin-user-id");
    const cleanupDefault = document.querySelector('input[name="jellyfin-cleanup-default"]:checked')?.value
      || WATCH_CLEANUP_DEFAULT;
    const jfConfig = await api.jellyfinConfigSet(
      document.getElementById("jellyfin-url").value.trim(),
      document.getElementById("jellyfin-api-key").value.trim(),
      jfUserSelect.value,
      jfUserSelect.value
        ? (jfUserSelect.selectedOptions[0]?.dataset.name || jfUserSelect.selectedOptions[0]?.textContent || "")
        : "",
      cleanupDefault,
    );
    state.watchlistCleanupDefault = WATCH_CLEANUP_LABELS[jfConfig.cleanup_default]
      ? jfConfig.cleanup_default
      : WATCH_CLEANUP_DEFAULT;
    state.jellyfinUserConfigured = !!(jfConfig.url && jfConfig.has_api_key && jfConfig.user_id);
    document.getElementById("jellyfin-user-status").textContent = jfConfig.user_id
      ? `Gesehen-Status: ${jfConfig.user_name || "Benutzer gewählt"}`
      : "Für „Nächste Staffel“ und automatische Löschregeln erforderlich.";
    const tmdb = await api.tmdbConfigSet(
      document.getElementById("tmdb-api-key").value.trim(),
    );
    applyTmdbCfg(tmdb);
    const auto = await api.automationConfigSet({
      auto_download: document.getElementById("auto-download").checked,
      check_interval_min: Math.max(5, parseInt(document.getElementById("check-interval").value, 10) || 30),
      dl_window_start: parseHour("dl-window-start"),
      dl_window_end: parseHour("dl-window-end"),
    });
    applyAutomationCfg(auto);
    applyUpdaterConfig(await api.updaterConfigSet({
      update_mode: document.getElementById("updater-mode").value,
      auto_update_interval_hours: Math.max(
        1,
        Math.min(168, parseInt(document.getElementById("updater-interval").value, 10) || 6),
      ),
    }));
    const seerr = await api.seerrConfigSet({
      enabled: document.getElementById("seerr-enabled").checked,
      url: document.getElementById("seerr-url").value.trim(),
      api_key: document.getElementById("seerr-api-key").value.trim(),
      poll_interval_seconds: Math.max(
        15,
        Math.min(3600, parseInt(document.getElementById("seerr-poll-interval").value, 10) || 60),
      ),
    });
    applySeerrCfg(seerr);
    const telegram = await api.telegramConfigSet({
      enabled: document.getElementById("telegram-enabled").checked,
      bot_token: document.getElementById("telegram-token").value.trim(),
      chat_id: document.getElementById("telegram-chat-id").value.trim(),
    });
    applyTelegramCfg(telegram);
    const t = new Date().toLocaleTimeString("de-DE", { hour: "2-digit", minute: "2-digit" });
    status.textContent = `✓ Gespeichert (${t})`;
    if (state.wl.loaded) refreshWatchlist();
  } catch (e) {
    status.textContent = "✗ Fehler: " + e.message;
  } finally {
    btn.disabled = false;
  }
}

async function openDirModal(path) {
  const data = await api.browseDir(path);
  dirModalPath = data.path;
  document.getElementById("dir-modal").classList.remove("hidden");
  document.getElementById("dir-modal-path").textContent = data.path;
  const list = document.getElementById("dir-modal-list");
  list.innerHTML = "";
  document.getElementById("dir-modal-up").disabled = !data.parent;
  document.getElementById("dir-modal-up").onclick = () => { if (data.parent) openDirModal(data.parent); };
  for (const d of data.dirs) {
    const item = document.createElement("div");
    item.className = "dir-item";
    item.textContent = d.name;
    item.addEventListener("click", () => openDirModal(d.path));
    list.appendChild(item);
  }
}

// ── Ersteinrichtung ─────────────────────────────────────────────────────────
let setupStep = 1;
let setupRequired = false;
let initialDataStarted = false;

const setupStepCopy = {
  1: {
    title: "Wohin sollen deine Medien?",
    intro: "Die Ordner werden bei Bedarf angelegt. Beide müssen für den Downloader beschreibbar sein.",
  },
  2: {
    title: "Bibliothek und Filmdaten",
    intro: "Beide Verbindungen sind optional und können später in den Einstellungen ergänzt werden.",
  },
  3: {
    title: "Downloads automatisieren",
    intro: "Lege fest, was selbstständig laufen darf. Alle Werte bleiben später änderbar.",
  },
};

function setSetupStatus(message = "", error = false) {
  const el = document.getElementById("setup-status");
  el.textContent = message;
  el.classList.toggle("error", error);
}

function showSetupStep(nextStep) {
  setupStep = Math.max(1, Math.min(3, nextStep));
  document.querySelectorAll("[data-setup-step]").forEach((panel) => {
    panel.classList.toggle("hidden", Number(panel.dataset.setupStep) !== setupStep);
  });
  document.querySelectorAll("[data-setup-marker]").forEach((marker) => {
    const markerStep = Number(marker.dataset.setupMarker);
    marker.classList.toggle("active", markerStep === setupStep);
    marker.classList.toggle("complete", markerStep < setupStep);
    if (markerStep === setupStep) marker.setAttribute("aria-current", "step");
    else marker.removeAttribute("aria-current");
  });
  document.getElementById("setup-step-label").textContent = `SCHRITT ${setupStep} VON 3`;
  document.getElementById("setup-title").textContent = setupStepCopy[setupStep].title;
  document.getElementById("setup-intro").textContent = setupStepCopy[setupStep].intro;
  document.getElementById("setup-back").classList.toggle("hidden", setupStep === 1);
  document.getElementById("setup-next").classList.toggle("hidden", setupStep === 3);
  document.getElementById("setup-finish").classList.toggle("hidden", setupStep !== 3);
  setSetupStatus();
  const focusTarget = document.querySelector(`[data-setup-step="${setupStep}"] input:not([type="checkbox"])`);
  if (focusTarget) window.setTimeout(() => focusTarget.focus(), 40);
}

function validateSetupStep(step) {
  if (step === 1) {
    const movie = document.getElementById("setup-save-path");
    const series = document.getElementById("setup-series-path");
    movie.removeAttribute("aria-invalid");
    series.removeAttribute("aria-invalid");
    if (!movie.value.trim() || !series.value.trim()) {
      if (!movie.value.trim()) movie.setAttribute("aria-invalid", "true");
      if (!series.value.trim()) series.setAttribute("aria-invalid", "true");
      setSetupStatus("Film- und Serienordner müssen angegeben werden.", true);
      (!movie.value.trim() ? movie : series).focus();
      return false;
    }
  }
  if (step === 3 && document.getElementById("setup-telegram-enabled").checked) {
    const token = document.getElementById("setup-telegram-token");
    token.removeAttribute("aria-invalid");
    if (!token.value.trim() && token.dataset.hasSecret !== "true") {
      token.setAttribute("aria-invalid", "true");
      setSetupStatus("Für den aktivierten Telegram-Bot fehlt der Bot-Token.", true);
      token.focus();
      return false;
    }
  }
  return true;
}

function parseSetupHour(id) {
  const value = document.getElementById(id).value.trim();
  if (value === "") return null;
  return Math.max(0, Math.min(23, parseInt(value, 10) || 0));
}

async function finishSetup() {
  if (!validateSetupStep(3)) return;
  const finish = document.getElementById("setup-finish");
  const back = document.getElementById("setup-back");
  finish.disabled = true;
  back.disabled = true;
  setSetupStatus("Ordner und Einstellungen werden angelegt …");
  try {
    await api.setupComplete({
      save_path: document.getElementById("setup-save-path").value.trim(),
      series_path: document.getElementById("setup-series-path").value.trim(),
      jellyfin_url: document.getElementById("setup-jellyfin-url").value.trim(),
      jellyfin_api_key: document.getElementById("setup-jellyfin-key").value.trim(),
      jellyfin_user_id: document.getElementById("setup-jellyfin-user").value,
      jellyfin_user_name: document.getElementById("setup-jellyfin-user").value
        ? (document.getElementById("setup-jellyfin-user").selectedOptions[0]?.dataset.name
          || document.getElementById("setup-jellyfin-user").selectedOptions[0]?.textContent || "")
        : "",
      tmdb_api_key: document.getElementById("setup-tmdb-key").value.trim(),
      auto_download: document.getElementById("setup-auto-download").checked,
      check_interval_min: Math.max(5, parseInt(document.getElementById("setup-check-interval").value, 10) || 30),
      dl_window_start: parseSetupHour("setup-window-start"),
      dl_window_end: parseSetupHour("setup-window-end"),
      telegram_enabled: document.getElementById("setup-telegram-enabled").checked,
      telegram_bot_token: document.getElementById("setup-telegram-token").value.trim(),
      telegram_chat_id: document.getElementById("setup-telegram-chat").value.trim(),
    });
    setupRequired = false;
    document.body.classList.remove("setup-open");
    document.getElementById("setup-wizard").classList.add("hidden");
    await initSettings();
    startInitialData();
  } catch (e) {
    setSetupStatus(`Einrichtung fehlgeschlagen: ${e.message}`, true);
  } finally {
    finish.disabled = false;
    back.disabled = false;
  }
}

async function initSetupWizard() {
  try {
    const data = await api.setupStatus();
    if (!data.required) return false;
    setupRequired = true;
    const defaults = data.defaults || {};
    const jf = defaults.jellyfin || {};
    const tmdb = defaults.tmdb || {};
    const telegram = defaults.telegram || {};
    const automation = defaults.automation || {};
    document.getElementById("setup-save-path").value = defaults.save_path || "";
    document.getElementById("setup-series-path").value = defaults.series_path || defaults.save_path || "";
    document.getElementById("setup-jellyfin-url").value = jf.url || "";
    const setupJfKey = document.getElementById("setup-jellyfin-key");
    setupJfKey.value = "";
    setupJfKey.dataset.hasSecret = jf.has_api_key ? "true" : "false";
    if (jf.has_api_key) setupJfKey.placeholder = "Bereits hinterlegt";
    fillJellyfinUserSelect("setup-jellyfin-user", [], jf.user_id || "", jf.user_name || "");
    const setupTmdbKey = document.getElementById("setup-tmdb-key");
    setupTmdbKey.value = "";
    setupTmdbKey.dataset.hasSecret = tmdb.has_api_key ? "true" : "false";
    if (tmdb.has_api_key) setupTmdbKey.placeholder = "Bereits hinterlegt";
    document.getElementById("setup-auto-download").checked = !!automation.auto_download;
    document.getElementById("setup-check-interval").value = automation.check_interval_min || 30;
    document.getElementById("setup-window-start").value = automation.dl_window_start ?? "";
    document.getElementById("setup-window-end").value = automation.dl_window_end ?? "";
    document.getElementById("setup-telegram-enabled").checked = !!telegram.enabled;
    const setupTelegramToken = document.getElementById("setup-telegram-token");
    setupTelegramToken.value = "";
    setupTelegramToken.dataset.hasSecret = telegram.has_bot_token ? "true" : "false";
    if (telegram.has_bot_token) setupTelegramToken.placeholder = "Bereits hinterlegt";
    document.getElementById("setup-telegram-chat").value = telegram.chat_id || "";
    document.getElementById("setup-config-path").textContent = data.config_path || "DATA/FilmeDownloader/settings.ini";
    document.body.classList.add("setup-open");
    document.getElementById("setup-wizard").classList.remove("hidden");
    showSetupStep(1);
    return true;
  } catch (e) {
    console.error("Ersteinrichtung konnte nicht geprüft werden:", e);
    return false;
  }
}

function startInitialData() {
  if (initialDataStarted) return;
  initialDataStarted = true;
  api.genres().then((data) => {
    const filter = document.getElementById("genre-filter");
    for (const g of data.genres) {
      const button = document.createElement("button");
      button.type = "button";
      button.className = "genre-chip";
      button.dataset.genre = g;
      button.setAttribute("aria-pressed", "false");
      button.textContent = g;
      filter.appendChild(button);
    }
    document.getElementById("genre-count").textContent = `${data.genres.length} Genres verfügbar`;
    const genresAvailable = data.genres.length > 0;
    document.getElementById("genre-random").disabled = !genresAvailable;
    document.getElementById("genre-toggle").disabled = !genresAvailable;
    setActiveGenreFilter(state.fp.activeGenre);
  }).catch((e) => {
    document.getElementById("genre-count").textContent = "Genres nicht verfügbar";
    console.error("Genres konnten nicht geladen werden:", e);
  });
  syncQueueSnapshot("Initiale Queue-Synchronisierung");
  refreshWatchlist();
  fpShowList("new").catch((e) => {
    document.getElementById("fp-status").textContent = `Fehler: ${e.message}`;
  });
}

// ── Init ─────────────────────────────────────────────────────────────────
async function initApp() {
  document.querySelectorAll(".media-modal").forEach((modal) => document.body.appendChild(modal));
  buildAlphaBar();
  connectWs();

  document.querySelectorAll(".tab-btn").forEach((b) => b.addEventListener("click", () => switchTab(b.dataset.tab)));

  document.getElementById("session-btn").addEventListener("click", () => api.clearCookies());
  document.getElementById("mobile-queue-btn").addEventListener("click", openMobileQueue);
  document.getElementById("mobile-queue-close").addEventListener("click", closeMobileQueue);
  document.getElementById("mobile-queue-backdrop").addEventListener("click", closeMobileQueue);
  document.getElementById("queue-dock-toggle").addEventListener("click", toggleDesktopQueue);
  document.querySelectorAll("[data-modal-close]").forEach((button) => {
    button.addEventListener("click", () => closeMediaModal(button.dataset.modalClose));
  });

  // Filme
  document.getElementById("fp-search-btn").addEventListener("click", fpSearch);
  document.getElementById("fp-search").addEventListener("keydown", (e) => { if (e.key === "Enter") fpSearch(); });
  document.getElementById("fp-new-btn").addEventListener("click", () => fpShowList("new"));
  document.getElementById("fp-top-btn").addEventListener("click", () => fpShowList("top"));
  document.getElementById("genre-filter").addEventListener("click", (e) => {
    const button = e.target.closest("[data-genre]");
    if (button) fpGenreChange(button.dataset.genre);
  });
  document.getElementById("genre-toggle").addEventListener("click", (e) => {
    const filter = document.getElementById("genre-filter");
    const expanded = filter.classList.toggle("is-expanded");
    e.currentTarget.setAttribute("aria-expanded", String(expanded));
    e.currentTarget.querySelector(".genre-toggle-label").textContent = expanded ? "Weniger zeigen" : "Alle zeigen";
  });
  document.getElementById("genre-random").addEventListener("click", () => {
    const genres = [...document.querySelectorAll("#genre-filter [data-genre]")]
      .map((button) => button.dataset.genre)
      .filter((genre) => genre !== "Alle Genres" && genre !== state.fp.activeGenre);
    if (!genres.length) return;
    fpGenreChange(genres[Math.floor(Math.random() * genres.length)]);
  });
  document.getElementById("fp-pager-prev").addEventListener("click", () => fpPagerChange(-1));
  document.getElementById("fp-pager-next").addEventListener("click", () => fpPagerChange(1));

  // Serien
  document.getElementById("series-search-btn").addEventListener("click", seriesSearch);
  document.getElementById("series-search").addEventListener("keydown", (e) => { if (e.key === "Enter") seriesSearch(); });
  document.getElementById("series-discover-btn").addEventListener("click", () => seriesBrowse("discover", 1));
  document.getElementById("series-new-btn").addEventListener("click", () => seriesBrowse("new", 1));
  document.getElementById("series-trending-btn").addEventListener("click", () => seriesBrowse("trending", 1));
  document.getElementById("series-az-btn").addEventListener("click", () => {
    document.getElementById("series-alpha-bar").classList.toggle("hidden");
  });
  document.getElementById("series-pager-prev").addEventListener("click", () => seriesPagerChange(-1));
  document.getElementById("series-pager-next").addEventListener("click", () => seriesPagerChange(1));
  document.getElementById("series-select-all").addEventListener("click", () => {
    if (!state.series.current) return;
    state.series.epPicked = new Set(
      seriesEpisodes().filter(isEpisodeSelectable).map((episode) => episode.slug),
    );
    renderSeriesTiles();
  });
  document.getElementById("series-select-none").addEventListener("click", () => {
    state.series.epPicked.clear();
    renderSeriesTiles();
  });
  document.getElementById("series-add-btn").addEventListener("click", seriesAddSelected);
  document.getElementById("series-watch-btn").addEventListener("click", () => openWatchModeModal());
  document.getElementById("series-subscriptions-manage").addEventListener("click", () => switchTab("bibliothek"));
  document.getElementById("watch-mode-close").addEventListener("click", closeWatchModeModal);
  document.getElementById("watch-mode-cancel").addEventListener("click", closeWatchModeModal);
  document.getElementById("watch-mode-save").addEventListener("click", saveWatchMode);
  document.getElementById("watch-mode-remove").addEventListener("click", removeWatchModeSubscription);
  document.querySelectorAll('input[name="watch-mode"]').forEach((radio) => {
    radio.addEventListener("change", updateWatchModeRequirement);
  });
  document.querySelectorAll('input[name="watch-cleanup"]').forEach((radio) => {
    radio.addEventListener("change", updateWatchModeRequirement);
  });
  document.getElementById("watch-mode-modal").addEventListener("click", (event) => {
    if (event.target.id === "watch-mode-modal") closeWatchModeModal();
  });

  // Bibliothek
  document.getElementById("wl-check-all").addEventListener("click", async () => {
    document.getElementById("wl-status").textContent = `Prüfe ${state.wl.items.length} Serie(n) …`;
    const data = await api.watchlistCheck(null);
    applyWatchlist(data.watchlist);
    document.getElementById("wl-status").textContent = `${data.checked}/${data.total} geprüft`;
  });
  document.getElementById("wl-check-selected").addEventListener("click", async () => {
    if (!state.wl.selected.size) { alert("Bitte zuerst Serien in der Liste auswählen."); return; }
    const slugs = [...state.wl.selected];
    document.getElementById("wl-status").textContent = `Prüfe ${slugs.length} Serie(n) …`;
    const data = await api.watchlistCheck(slugs);
    applyWatchlist(data.watchlist);
    document.getElementById("wl-status").textContent = `${data.checked}/${data.total} geprüft`;
  });
  document.getElementById("wl-open").addEventListener("click", () => {
    const first = [...state.wl.selected][0];
    if (first) openWatchlistEntry(first);
  });
  document.getElementById("wl-remove").addEventListener("click", async () => {
    if (!state.wl.selected.size) return;
    const data = await api.watchlistRemove([...state.wl.selected]);
    state.wl.selected.clear();
    applyWatchlist(data.watchlist);
    await syncQueueSnapshot("Queue-Synchronisierung nach Abo-Entfernung");
  });

  // Benachrichtigungs-Glocke
  document.getElementById("notif-bell").addEventListener("click", (e) => {
    e.stopPropagation();
    toggleNotifDropdown();
  });
  document.addEventListener("click", (e) => {
    if (!e.target.closest(".bell-wrap")) closeNotifDropdown();
  });
  document.getElementById("notif-refresh").addEventListener("click", refreshNotifications);
  document.getElementById("notif-library").addEventListener("click", () => {
    closeNotifDropdown();
    switchTab("bibliothek");
  });
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && !document.getElementById("watch-mode-modal").classList.contains("hidden")) {
      event.preventDefault();
      closeWatchModeModal();
      return;
    }
    if (handleMediaModalKeydown(event)) return;
    if (event.key !== "Escape") return;
    closeNotifDropdown();
    setQueueDockExpanded(false);
    closeMobileQueue();
  });

  // Warteschlange / Downloads / Einstellungen
  document.getElementById("queue-clear").addEventListener("click", async () => {
    const resp = await api.queueClear();
    refreshQueueUiAfterChange(resp);
  });
  document.getElementById("cancel-btn").addEventListener("click", async () => {
    const resp = await api.downloadCancel();
    renderQueue(resp.queue);
    setDownloadState("cancelled", "Abgebrochen", "Downloads wurden gestoppt", state.download.percent);
  });
  document.getElementById("settings-btn").addEventListener("click", () => switchTab("einstellungen"));
  document.getElementById("settings-save").addEventListener("click", saveAllSettings);
  document.getElementById("updater-check").addEventListener("click", () => checkForUpdates(true));
  document.getElementById("updater-install").addEventListener("click", installUpdate);
  document.getElementById("updater-mode").addEventListener("change", (event) => {
    document.getElementById("updater-interval").disabled = event.target.value !== "automatic";
    document.getElementById("updater-mode-status").textContent = event.target.value === "automatic"
      ? "Automatisch · wird nach dem Speichern aktiviert."
      : "Manuell · wird nach dem Speichern aktiviert.";
  });
  document.getElementById("seerr-sync").addEventListener("click", async () => {
    const button = document.getElementById("seerr-sync");
    const status = document.getElementById("seerr-status");
    button.disabled = true;
    status.textContent = "Prüfe Seerr-Anfragen …";
    try {
      applySeerrCfg(await api.seerrSync());
    } catch (error) {
      status.textContent = `✗ ${error.message}`;
    } finally {
      button.disabled = false;
    }
  });
  document.getElementById("jellyfin-users-load").addEventListener("click", () => loadJellyfinUsers({
    urlId: "jellyfin-url", keyId: "jellyfin-api-key", selectId: "jellyfin-user-id",
    buttonId: "jellyfin-users-load", statusId: "jellyfin-user-status",
  }));
  document.getElementById("browse-dir-btn").addEventListener("click", () => {
    dirModalTarget = "save-path";
    openDirModal(document.getElementById("save-path").value);
  });
  document.getElementById("browse-series-btn").addEventListener("click", () => {
    dirModalTarget = "series-path";
    openDirModal(document.getElementById("series-path").value);
  });
  document.getElementById("dir-modal-close").addEventListener("click", () => {
    document.getElementById("dir-modal").classList.add("hidden");
  });
  document.getElementById("dir-modal-select").addEventListener("click", () => {
    // Nur ins gewählte Feld übernehmen – persistiert wird über "Speichern".
    document.getElementById(dirModalTarget).value = dirModalPath;
    document.getElementById("dir-modal").classList.add("hidden");
  });

  // Ersteinrichtung
  document.getElementById("setup-browse-movies").addEventListener("click", () => {
    dirModalTarget = "setup-save-path";
    openDirModal(document.getElementById("setup-save-path").value);
  });
  document.getElementById("setup-browse-series").addEventListener("click", () => {
    dirModalTarget = "setup-series-path";
    openDirModal(document.getElementById("setup-series-path").value);
  });
  document.getElementById("setup-jellyfin-users-load").addEventListener("click", () => loadJellyfinUsers({
    urlId: "setup-jellyfin-url", keyId: "setup-jellyfin-key", selectId: "setup-jellyfin-user",
    buttonId: "setup-jellyfin-users-load",
  }));
  document.getElementById("setup-next").addEventListener("click", () => {
    if (validateSetupStep(setupStep)) showSetupStep(setupStep + 1);
  });
  document.getElementById("setup-back").addEventListener("click", () => showSetupStep(setupStep - 1));
  document.getElementById("setup-finish").addEventListener("click", finishSetup);
  document.getElementById("setup-wizard").addEventListener("keydown", (e) => {
    if (!setupRequired || e.key !== "Enter" || e.target.closest("button") || e.target.type === "checkbox") return;
    e.preventDefault();
    if (setupStep < 3) {
      if (validateSetupStep(setupStep)) showSetupStep(setupStep + 1);
    } else {
      finishSetup();
    }
  });

  try {
    await initSettings();
  } catch (e) {
    console.error("Einstellungen konnten nicht geladen werden:", e);
  }
  const needsSetup = await initSetupWizard();
  if (!needsSetup) startInitialData();
}

document.addEventListener("DOMContentLoaded", initApp);
