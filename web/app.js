const state = {
  tab: "filme",
  fp: {
    results: [], moviesCache: {}, category: null, page: 1, lastPageFull: false,
    activeGenre: "Alle Genres", selectedSlug: null, pendingPreload: null,
    metadataCache: {}, requestSeq: 0,
  },
  series: {
    results: [], browseMode: null, page: 1, lastPageFull: false,
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
};

const WATCH_MODE_DEFAULT = "latest_season";
const WATCH_MODE_LABELS = {
  all: "Alles Fehlende",
  latest_season: "Neueste Staffel",
  next_season: "Nächste Staffel nach Gesehen-Status",
};
let watchModeContext = null;

function escapeHtml(s) {
  return String(s ?? "").replace(/[&<>"']/g, (c) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  }[c]));
}

// ── Tabs ─────────────────────────────────────────────────────────────────
function switchTab(name) {
  document.querySelectorAll(".tab-btn").forEach((b) => b.classList.toggle("active", b.dataset.tab === name));
  document.querySelectorAll(".tab-content").forEach((s) => s.classList.toggle("active", s.id === `tab-${name}`));
  // Im Einstellungen-Bereich die Download-Sidebar ausblenden (eigener Vollbereich).
  document.body.classList.toggle("settings-active", name === "einstellungen");
  closeMobileQueue();
  state.tab = name;
  if (name === "bibliothek" && !state.wl.loaded) refreshWatchlist();
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
  document.getElementById("queue-count").textContent = `${payload.count} markiert`;
  document.getElementById("mobile-queue-count").textContent = String(payload.count);
  const list = document.getElementById("queue-list");
  list.innerHTML = "";
  if (!payload.groups.length) {
    list.innerHTML = `<div class="queue-empty">Keine Downloads markiert</div>`;
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
      status.textContent = it.done ? "Fertig" : "Bereit";
      const removeBtn = document.createElement("button");
      removeBtn.className = "remove-btn";
      removeBtn.textContent = "✕";
      removeBtn.addEventListener("click", async () => {
        removeBtn.disabled = true;
        try {
          const resp = await api.queueRemove(it.slug);
          renderQueue(resp.queue);
          renderFpResults();
        } catch (error) {
          console.warn("Queue-Eintrag konnte nicht entfernt werden:", error);
          removeBtn.disabled = false;
        }
      });
      row.append(position, content, status, removeBtn);
      list.appendChild(row);
    }
  }
}

function openMobileQueue() {
  document.body.classList.add("queue-open");
  document.getElementById("mobile-queue-backdrop").setAttribute("aria-hidden", "false");
  document.getElementById("mobile-queue-close").focus();
}

function closeMobileQueue() {
  document.body.classList.remove("queue-open");
  document.getElementById("mobile-queue-backdrop").setAttribute("aria-hidden", "true");
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

function scrollToMobileDetail(selector) {
  if (!window.matchMedia("(max-width: 820px)").matches) return;
  requestAnimationFrame(() => {
    document.querySelector(selector)?.scrollIntoView({ behavior: "smooth", block: "start" });
  });
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
    renderFpResults();
  } catch (e) { /* JF bleibt optional. */ }
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
    const jellyfinCount = refreshed.seasons.reduce(
      (sum, season) => sum + season.episodes.filter((episode) => episode.in_jellyfin).length,
      0,
    );
    document.getElementById("series-status").textContent =
      refreshed.jellyfin_available === false
        ? `${refreshed.episode_count} Episoden · Jellyfin-Abgleich nicht verfügbar`
        : `${refreshed.episode_count} Episoden · ${jellyfinCount} in Jellyfin`;
    return true;
  } catch (error) {
    console.warn("Serienstatus konnte nicht live aktualisiert werden:", error);
    return false;
  } finally {
    if (state.series.jellyfinRefreshByBase.get(baseSlug) === refreshGeneration) {
      state.series.jellyfinRefreshByBase.delete(baseSlug);
    }
  }
}

function renderFpResults() {
  const container = document.getElementById("fp-results");
  container.innerHTML = "";
  for (const r of state.fp.results) {
    const movie = state.fp.moviesCache[r.slug];
    const picked = state.queuedSlugs.has(r.slug);
    let status = "—", tag = "idle";
    if (movie) {
      if (!movie.hosters || movie.hosters.length === 0) { status = "kein Hoster"; tag = "novoe"; }
      else { status = movie.hoster_label; tag = "ready"; }
    } else if (state.fp.pendingPreload && state.fp.pendingPreload.has(r.slug)) {
      status = "lade …"; tag = "pending";
    }
    if (picked) tag = "picked";
    const row = document.createElement("div");
    row.className = "row" + (r.slug === state.fp.selectedSlug ? " selected" : "");

    const pickFlag = document.createElement("span");
    pickFlag.className = `pick-flag status-${tag}`;
    pickFlag.textContent = picked ? "In" : "";
    pickFlag.addEventListener("click", (e) => { e.stopPropagation(); toggleFpPick(r.slug); });

    const title = document.createElement("span");
    title.className = `status-${tag}`;
    title.textContent = r.title;

    const year = document.createElement("span");
    year.className = "dim";
    year.textContent = r.year || "";

    const st = document.createElement("span");
    st.className = `status-${tag}`;
    st.textContent = status;

    const jf = document.createElement("span");
    if (r.in_jellyfin) {
      jf.className = "jellyfin-badge owned";
      jf.textContent = "✓ da";
      jf.title = "Bereits in der Jellyfin-Bibliothek gefunden";
    } else {
      jf.className = "jellyfin-badge dim";
      jf.textContent = "—";
      jf.title = "Nicht in der Jellyfin-Bibliothek gefunden";
    }

    row.append(pickFlag, title, year, st, jf);
    row.addEventListener("click", () => selectFpRow(r.slug));
    container.appendChild(row);
  }
  document.getElementById("fp-status").textContent = fpStatusMessage();
}

function applyFpResults(data) {
  state.fp.results = data.results;
  state.fp.page = data.page;
  state.fp.lastPageFull = data.last_page_full;
  state.fp.selectedSlug = data.results[0]?.slug || null;
  state.fp.pendingPreload = null;
  renderFpResults();
  const pager = document.getElementById("fp-pager");
  if (data.category) {
    pager.classList.remove("hidden");
    document.getElementById("fp-pager-label").textContent = `Seite ${data.page}`;
    document.getElementById("fp-pager-prev").disabled = data.page <= 1;
    document.getElementById("fp-pager-next").disabled = !data.last_page_full;
  } else {
    pager.classList.add("hidden");
  }
  const requestId = state.fp.requestSeq;
  const first = data.results[0];
  if (first) {
    document.getElementById("fp-detail-cover").removeAttribute("src");
    document.getElementById("fp-detail-title").textContent = "Lade Cover und Beschreibung …";
    loadFpMetadata(first, requestId).finally(() => preloadTmdbMetadata(requestId, first.slug));
  }
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
      if (metadata) state.fp.metadataCache[item.slug] = metadata;
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

async function preloadTmdbMetadata(requestId, excludeSlug = "") {
  const items = state.fp.results
    .filter((r) => r.slug !== excludeSlug && !state.fp.metadataCache[r.slug])
    .map((r) => ({ slug: r.slug, title: r.title, year: r.year || "" }));
  if (!items.length) return;
  const visibleSlugs = new Set(items.map((item) => item.slug));
  try {
    const response = await api.tmdbMovies(items);
    if (requestId !== state.fp.requestSeq) return;
    for (const [slug, metadata] of Object.entries(response.movies || {})) {
      if (visibleSlugs.has(slug)) state.fp.metadataCache[slug] = metadata;
    }
    refreshFpJellyfinStatus();
    const selected = state.fp.selectedSlug;
    if (selected && visibleSlugs.has(selected) && !state.fp.moviesCache[selected] && state.fp.metadataCache[selected]) {
      showFpDetail(selected, metadataPreviewMovie(state.fp.metadataCache[selected]), true);
    }
  } catch (e) { /* Anbieter-Metadaten bleiben als Fallback sichtbar. */ }
}

async function fpSearch() {
  const q = document.getElementById("fp-search").value.trim();
  if (!q) return;
  state.fp.category = null;
  document.getElementById("fp-status").textContent = `Suche nach «${q}» …`;
  document.getElementById("genre-filter").value = "Alle Genres";
  state.fp.activeGenre = "Alle Genres";
  const requestId = ++state.fp.requestSeq;
  const data = await api.movies({ mode: "search", query: q });
  if (requestId !== state.fp.requestSeq) return;
  applyFpResults(data);
}

async function fpShowList(category) {
  state.fp.category = category;
  state.fp.activeGenre = "Alle Genres";
  document.getElementById("genre-filter").value = "Alle Genres";
  document.getElementById("fp-status").textContent = `Lade ${category === "new" ? "Neu" : "Top"}-Filme …`;
  const requestId = ++state.fp.requestSeq;
  const data = await api.movies({ mode: category, page: 1 });
  if (requestId !== state.fp.requestSeq) return;
  applyFpResults(data);
}

async function fpGenreChange(genre) {
  state.fp.category = "genre";
  state.fp.activeGenre = genre;
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
  const data = await api.movies(params);
  if (requestId !== state.fp.requestSeq) return;
  applyFpResults(data);
}

async function toggleFpPick(slug) {
  if (state.queuedSlugs.has(slug)) {
    const resp = await api.queueRemove(slug);
    refreshQueueUiAfterChange(resp);
    return;
  }
  const resp = await api.queueAdd([slug]);
  if (!state.fp.moviesCache[slug]) {
    try { state.fp.moviesCache[slug] = await api.movie(slug); } catch (e) { /* server logs */ }
  }
  refreshQueueUiAfterChange(resp);
}

async function selectFpRow(slug) {
  state.fp.selectedSlug = slug;
  renderFpResults();
  const movie = state.fp.moviesCache[slug];
  if (movie) return showFpDetail(slug, movie);
  const item = state.fp.results.find((r) => r.slug === slug);
  if (!item) return;
  const metadata = state.fp.metadataCache[slug];
  if (metadata) showFpDetail(slug, metadataPreviewMovie(metadata), true);
  if (!metadata) {
    document.getElementById("fp-detail-panel").classList.remove("is-empty");
    document.getElementById("fp-detail-panel").classList.add("has-no-cover");
    document.getElementById("fp-detail-cover").removeAttribute("src");
    document.getElementById("fp-detail-title").textContent = "Lade Cover und Beschreibung …";
    setFpDetailAvailability("Metadaten werden geladen", "loading");
  }
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

function showFpDetail(slug, movie, metadataOnly = false) {
  const detailPanel = document.getElementById("fp-detail-panel");
  const cover = document.getElementById("fp-detail-cover");
  detailPanel.classList.remove("is-empty");
  detailPanel.classList.toggle("has-no-cover", !movie.cover_url);
  if (movie.cover_url) cover.src = api.coverUrl(movie.cover_url);
  else cover.removeAttribute("src");
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

  const addBtn = document.getElementById("fp-detail-add");
  if (metadataOnly) {
    const queued = state.queuedSlugs.has(slug);
    addBtn.disabled = false;
    addBtn.textContent = queued ? "✕ Entfernen" : "↓ Herunterladen";
    addBtn.onclick = async () => {
      addBtn.disabled = true;
      addBtn.textContent = queued ? "Entferne …" : "Prüfe …";
      await toggleFpPick(slug);
      const loaded = state.fp.moviesCache[slug];
      if (loaded) showFpDetail(slug, loaded);
      else if (state.fp.selectedSlug === slug) showFpDetail(slug, movie, true);
    };
    scrollToMobileDetail("#tab-filme .detail-panel");
    return;
  }
  const queued = state.queuedSlugs.has(slug);
  addBtn.disabled = !movie.hosters.length;
  addBtn.textContent = queued ? "✕ Entfernen" : "↓ Herunterladen";
  addBtn.onclick = async () => {
    const resp = queued ? await api.queueRemove(slug) : await api.queueAdd([slug]);
    refreshQueueUiAfterChange(resp);
    showFpDetail(slug, movie);
  };
  scrollToMobileDetail("#tab-filme .detail-panel");
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
  if (!mode || mode === "search") { pager.classList.add("hidden"); return; }
  pager.classList.remove("hidden");
  document.getElementById("series-pager-label").textContent = `Seite ${state.series.page}`;
  document.getElementById("series-pager-prev").disabled = state.series.page <= 1;
  document.getElementById("series-pager-next").disabled = !state.series.lastPageFull;
}

function renderSeriesResults() {
  const container = document.getElementById("series-results");
  container.innerHTML = "";
  for (const r of state.series.results) {
    const row = document.createElement("div");
    const selectedBase = state.series.pendingBaseSlug || state.series.current?.base_slug;
    const selected = selectedBase === r.base_slug;
    const loading = state.series.pendingBaseSlug === r.base_slug;
    row.className = "series-row" + (selected ? " selected" : "") + (loading ? " loading" : "");
    row.dataset.baseSlug = r.base_slug;
    if (loading) row.setAttribute("aria-busy", "true");
    const title = document.createElement("span");
    title.textContent = r.title;
    const year = document.createElement("span");
    year.className = "dim";
    year.textContent = r.year || "";
    const info = document.createElement("span");
    info.className = "dim";
    info.textContent = "—";
    row.append(title, year, info);
    row.addEventListener("click", () => loadSeries(r));
    container.appendChild(row);
  }
}

function updateSeriesResultSelection() {
  const selectedBase = state.series.pendingBaseSlug || state.series.current?.base_slug;
  document.querySelectorAll("#series-results .series-row").forEach((row) => {
    const loading = state.series.pendingBaseSlug === row.dataset.baseSlug;
    row.classList.toggle("selected", selectedBase === row.dataset.baseSlug);
    row.classList.toggle("loading", loading);
    if (loading) row.setAttribute("aria-busy", "true");
    else row.removeAttribute("aria-busy");
  });
}

function applySeriesResults(data) {
  state.series.results = data.results;
  state.series.page = data.page;
  state.series.lastPageFull = data.last_page_full;
  renderSeriesResults();
  updateSeriesPager();
  document.getElementById("series-status").textContent =
    data.results.length ? `${data.results.length} Serie(n) gefunden` : "Keine Serie gefunden.";
}

async function seriesSearch() {
  const q = document.getElementById("series-search").value.trim();
  if (!q) return;
  state.series.browseMode = "search";
  updateSeriesPager();
  document.getElementById("series-status").textContent = `Suche nach «${q}» …`;
  const data = await api.series({ mode: "search", query: q });
  applySeriesResults(data);
  if (data.direct_series) {
    showSeriesDetail(data.direct_series, firstEpisodeSlug(data.direct_series));
  }
}

function seriesParams(mode, page) {
  // Alpha-Modi kommen als "alpha:X"; "new"/"trending" direkt als Modusname.
  return mode.startsWith("alpha:")
    ? { mode: "alpha", letter: mode.split(":")[1], page }
    : { mode, page };
}

async function seriesBrowse(mode, page) {
  document.getElementById("series-status").textContent = "Lade …";
  const data = await api.series(seriesParams(mode, page));
  state.series.browseMode = mode;
  applySeriesResults(data);
}

async function seriesPagerChange(delta) {
  const mode = state.series.browseMode;
  if (!mode || mode === "search") return;
  const newPage = state.series.page + delta;
  if (newPage < 1) return;
  const data = await api.series(seriesParams(mode, newPage));
  applySeriesResults(data);
}

async function loadSeries(result) {
  const cacheKey = result.base_slug || result.sample_slug;
  if (state.series.pendingBaseSlug === cacheKey) return;
  const requestId = ++state.series.requestSeq;
  state.series.pendingBaseSlug = cacheKey;
  updateSeriesResultSelection();
  showSeriesLoading(result);

  const cached = state.series.cache[cacheKey];
  if (cached) {
    showSeriesDetail(cached, result.sample_slug);
    document.getElementById("series-status").textContent =
      `${cached.episode_count} Episoden · Jellyfin wird aktualisiert …`;
    refreshSeriesJellyfinStatus(true);
    return;
  }

  document.getElementById("series-status").textContent = `Lade Staffeln für «${result.title}» …`;
  try {
    const series = await api.seriesLoad(result.sample_slug, result.base_slug || "", true);
    if (requestId !== state.series.requestSeq) return;
    showSeriesDetail(series, result.sample_slug);
    document.getElementById("series-status").textContent = series.jellyfin_available === false
      ? `${series.episode_count} Episoden · Jellyfin-Abgleich nicht verfügbar`
      : `${series.episode_count} Episoden`;
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
  document.getElementById("series-detail-title").textContent = `${result.title} · Staffeln werden geladen …`;
  document.getElementById("series-cover").removeAttribute("src");
  document.getElementById("series-genres").textContent = result.year || "";
  document.getElementById("series-desc").textContent = "Die Serienakte ist bereits geöffnet. Episoden folgen sofort nach.";
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
  scrollToMobileDetail("#tab-serien .series-detail-panel");
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
  document.getElementById("series-cover").src = api.coverUrl(series.cover_url);
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
  scrollToMobileDetail("#tab-serien .series-detail-panel");
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
  if (series.jellyfin_available === false) {
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
      if (ep.in_jellyfin) tile.title = "Bereits in Jellyfin vorhanden";
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
}

function openWatchModeModal(entry = null) {
  const series = state.series.current;
  const baseSlug = entry?.base_slug || series?.base_slug;
  if (!baseSlug) return;
  const stored = entry || state.wl.items.find((item) => item.base_slug === baseSlug);
  const tracked = Boolean(stored || series?.watchlisted);
  const mode = stored?.download_mode || series?.watch_mode || WATCH_MODE_DEFAULT;
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
  document.getElementById("watch-mode-remove").classList.toggle("hidden", !tracked);
  document.getElementById("watch-mode-save").textContent = tracked ? "Regel übernehmen" : "Abo speichern";
  document.getElementById("watch-mode-status").textContent = "";
  document.getElementById("watch-mode-modal").classList.remove("hidden");
  updateWatchModeRequirement();
  setTimeout(() => document.querySelector('input[name="watch-mode"]:checked')?.focus(), 0);
}

function updateWatchModeRequirement() {
  const selected = document.querySelector('input[name="watch-mode"]:checked')?.value;
  const status = document.getElementById("watch-mode-status");
  if (selected === "next_season" && !state.jellyfinUserConfigured) {
    status.textContent = "Diese Regel wartet, bis unter Einstellungen ein Jellyfin-Benutzer gewählt wurde.";
  } else if (status.textContent.startsWith("Diese Regel wartet")) {
    status.textContent = "";
  }
}

async function saveWatchMode() {
  if (!watchModeContext) return;
  const selected = document.querySelector('input[name="watch-mode"]:checked')?.value;
  if (!selected) return;
  const saveBtn = document.getElementById("watch-mode-save");
  saveBtn.disabled = true;
  try {
    const data = watchModeContext.tracked
      ? await api.watchlistMode(watchModeContext.baseSlug, selected)
      : await api.watchlistAdd({
        base_slug: watchModeContext.baseSlug,
        title: watchModeContext.title,
        sample_url: watchModeContext.sampleUrl,
        known_slugs: watchModeContext.knownSlugs,
        download_mode: selected,
        tmdb_id: watchModeContext.tmdbId,
        aliases: watchModeContext.aliases,
        season_episode_counts: watchModeContext.seasonEpisodeCounts,
        season_counts_checked_at: watchModeContext.seasonCountsCheckedAt,
      });
    if (state.series.current?.base_slug === watchModeContext.baseSlug) {
      state.series.current.watchlisted = true;
      state.series.current.watch_mode = selected;
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
    card.className = "subscription-card" + (entry.new_count || entry.status === "blocked" || entry.status === "failed" ? " has-new" : "");
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
    meta.textContent = `${modeLabel} · ${watchlistStatusText(entry)}`;
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
  const withNotice = state.wl.items.filter((e) => e.new_count || e.status === "blocked" || e.status === "failed");
  const total = withNotice.reduce((sum, e) => sum + e.new_count, 0);
  const issueCount = withNotice.filter((entry) => entry.status === "blocked" || entry.status === "failed").length;
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
    countValue.textContent = entry.status === "blocked" ? "!" : String(entry.failed_count || entry.new_count);
    const countLabel = document.createElement("small");
    countLabel.textContent = entry.status === "blocked"
      ? "Blockiert"
      : (entry.status === "failed" ? "Fehler" : (entry.new_count === 1 ? "Episode" : "Episoden"));
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
  for (const entry of state.wl.items) {
    const row = document.createElement("div");
    row.className = "wl-row"
      + (state.wl.selected.has(entry.base_slug) ? " selected" : "")
      + (entry.new_count || entry.status === "blocked" || entry.status === "failed" ? " has-new" : "");
    const cb = document.createElement("input");
    cb.type = "checkbox";
    cb.checked = state.wl.selected.has(entry.base_slug);
    cb.addEventListener("click", (e) => { e.stopPropagation(); toggleWlSelect(entry.base_slug); });
    const title = document.createElement("span");
    title.textContent = entry.title;
    const status = document.createElement("span");
    status.className = "wl-rule-cell";
    const rule = document.createElement("button");
    rule.type = "button";
    rule.className = "wl-rule-btn";
    rule.textContent = entry.download_mode_label || WATCH_MODE_LABELS[entry.download_mode] || WATCH_MODE_LABELS[WATCH_MODE_DEFAULT];
    rule.title = "Abo-Regel ändern";
    rule.addEventListener("click", (event) => {
      event.stopPropagation();
      openWatchModeModal(entry);
    });
    const missing = document.createElement("span");
    missing.className = "wl-missing";
    missing.textContent = watchlistStatusText(entry);
    status.append(rule, missing);
    row.append(cb, title, status);
    row.addEventListener("click", () => toggleWlSelect(entry.base_slug));
    row.addEventListener("dblclick", () => openWatchlistEntry(entry.base_slug));
    container.appendChild(row);
  }
}

function toggleWlSelect(baseSlug) {
  if (state.wl.selected.has(baseSlug)) state.wl.selected.delete(baseSlug);
  else state.wl.selected.add(baseSlug);
  renderWatchlist();
}

async function openWatchlistEntry(baseSlug) {
  switchTab("serien");
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
  state.jellyfinUserConfigured = !!(jf.url && jf.has_api_key && jf.user_id);
  document.getElementById("jellyfin-user-status").textContent = jf.user_id
    ? `Gesehen-Status: ${jf.user_name || "Benutzer gewählt"}`
    : "Für „Nächste Staffel“ erforderlich.";
  const tmdb = await api.tmdbConfigGet();
  applyTmdbCfg(tmdb);
  const auto = await api.automationConfigGet();
  applyAutomationCfg(auto);
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

function applyUpdaterStatus(data) {
  const card = document.getElementById("updater-card");
  const status = document.getElementById("updater-status");
  const detail = document.getElementById("updater-detail");
  const badge = document.getElementById("updater-badge");
  const repository = document.getElementById("updater-repository");
  document.getElementById("updater-current").textContent = shortRevision(data.current_sha);
  document.getElementById("updater-latest").textContent = shortRevision(data.latest_sha);
  document.getElementById("updater-branch-label").textContent = `GitHub · ${data.branch || "main"}`;
  if (String(data.repository_url || "").startsWith("https://github.com/")) {
    repository.href = data.repository_url;
  }

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
    : "Lokale Build-ID fehlt; APP_COMMIT_SHA kann sie beim Containerstart setzen.";
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
    button.disabled = false;
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
    const jfConfig = await api.jellyfinConfigSet(
      document.getElementById("jellyfin-url").value.trim(),
      document.getElementById("jellyfin-api-key").value.trim(),
      jfUserSelect.value,
      jfUserSelect.value
        ? (jfUserSelect.selectedOptions[0]?.dataset.name || jfUserSelect.selectedOptions[0]?.textContent || "")
        : "",
    );
    state.jellyfinUserConfigured = !!(jfConfig.url && jfConfig.has_api_key && jfConfig.user_id);
    document.getElementById("jellyfin-user-status").textContent = jfConfig.user_id
      ? `Gesehen-Status: ${jfConfig.user_name || "Benutzer gewählt"}`
      : "Für „Nächste Staffel“ erforderlich.";
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
    const sel = document.getElementById("genre-filter");
    for (const g of data.genres) {
      const opt = document.createElement("option");
      opt.value = g;
      opt.textContent = g;
      sel.appendChild(opt);
    }
    document.getElementById("genre-count").textContent = `${data.genres.length} Genres`;
  }).catch((e) => console.error("Genres konnten nicht geladen werden:", e));
  syncQueueSnapshot("Initiale Queue-Synchronisierung");
  refreshWatchlist();
  fpShowList("new").catch((e) => {
    document.getElementById("fp-status").textContent = `Fehler: ${e.message}`;
  });
}

// ── Init ─────────────────────────────────────────────────────────────────
async function initApp() {
  buildAlphaBar();
  connectWs();

  document.querySelectorAll(".tab-btn").forEach((b) => b.addEventListener("click", () => switchTab(b.dataset.tab)));

  document.getElementById("session-btn").addEventListener("click", () => api.clearCookies());
  document.getElementById("mobile-queue-btn").addEventListener("click", openMobileQueue);
  document.getElementById("mobile-queue-close").addEventListener("click", closeMobileQueue);
  document.getElementById("mobile-queue-backdrop").addEventListener("click", closeMobileQueue);

  // Filme
  document.getElementById("fp-search-btn").addEventListener("click", fpSearch);
  document.getElementById("fp-search").addEventListener("keydown", (e) => { if (e.key === "Enter") fpSearch(); });
  document.getElementById("fp-new-btn").addEventListener("click", () => fpShowList("new"));
  document.getElementById("fp-top-btn").addEventListener("click", () => fpShowList("top"));
  document.getElementById("genre-filter").addEventListener("change", (e) => fpGenreChange(e.target.value));
  document.getElementById("fp-pager-prev").addEventListener("click", () => fpPagerChange(-1));
  document.getElementById("fp-pager-next").addEventListener("click", () => fpPagerChange(1));

  // Serien
  document.getElementById("series-search-btn").addEventListener("click", seriesSearch);
  document.getElementById("series-search").addEventListener("keydown", (e) => { if (e.key === "Enter") seriesSearch(); });
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
    if (event.key === "Escape") closeNotifDropdown();
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
