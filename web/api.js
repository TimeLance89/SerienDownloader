const api = {
  async _req(method, url, body) {
    const opts = { method, headers: {} };
    if (body !== undefined) {
      opts.headers["Content-Type"] = "application/json";
      opts.body = JSON.stringify(body);
    }
    const resp = await fetch(url, opts);
    let data = null;
    try { data = await resp.json(); } catch (e) { /* no body */ }
    if (!resp.ok) {
      const msg = (data && (data.detail || data.error)) || `HTTP ${resp.status}`;
      throw new Error(msg);
    }
    return data;
  },
  get(url) { return this._req("GET", url); },
  post(url, body) { return this._req("POST", url, body === undefined ? {} : body); },

  genres() { return this.get("/api/genres"); },
  movies(params) { return this.get("/api/movies?" + new URLSearchParams(params)); },
  movie(slug) { return this.get(`/api/movie/${encodeURIComponent(slug)}`); },
  moviesPreload(slugs) { return this.post("/api/movies/preload", { slugs }); },
  tmdbMovies(items) { return this.post("/api/tmdb/movies", { items }); },
  tmdbMovie(item) { return this.post("/api/tmdb/movie", item); },
  jellyfinMatches(items) { return this.post("/api/jellyfin/matches", { items }); },

  series(params) { return this.get("/api/series?" + new URLSearchParams(params)); },
  seriesLoad(sampleSlug, baseSlug = "", refreshJellyfin = false) {
    return this.post("/api/series/load", {
      sample_slug: sampleSlug, base_slug: baseSlug, refresh_jellyfin: refreshJellyfin,
    });
  },

  queueGet() { return this.get("/api/queue"); },
  queueAdd(slugs) { return this.post("/api/queue/add", { slugs }); },
  queueRemove(slug) { return this.post("/api/queue/remove", { slug }); },
  queueClear() { return this.post("/api/queue/clear"); },

  downloadCancel() { return this.post("/api/download/cancel"); },

  setupStatus() { return this.get("/api/setup/status"); },
  setupComplete(cfg) { return this.post("/api/setup/complete", cfg); },

  configGet() { return this.get("/api/config"); },
  configSet(savePath, seriesPath) { return this.post("/api/config", { save_path: savePath, series_path: seriesPath }); },
  providerPriorityGet() { return this.get("/api/providers/config"); },
  providerPrioritySet(cfg) { return this.post("/api/providers/config", cfg); },

  jellyfinConfigGet() { return this.get("/api/jellyfin/config"); },
  jellyfinConfigSet(url, apiKey, userId = "", userName = "") {
    return this.post("/api/jellyfin/config", { url, api_key: apiKey, user_id: userId, user_name: userName });
  },
  jellyfinUsers(url, apiKey) { return this.post("/api/jellyfin/users", { url, api_key: apiKey }); },
  tmdbConfigGet() { return this.get("/api/tmdb/config"); },
  tmdbConfigSet(apiKey) { return this.post("/api/tmdb/config", { api_key: apiKey, language: "de-DE" }); },
  automationConfigGet() { return this.get("/api/automation/config"); },
  automationConfigSet(cfg) { return this.post("/api/automation/config", cfg); },
  telegramConfigGet() { return this.get("/api/telegram/config"); },
  telegramConfigSet(cfg) { return this.post("/api/telegram/config", cfg); },
  seerrConfigGet() { return this.get("/api/seerr/config"); },
  seerrConfigSet(cfg) { return this.post("/api/seerr/config", cfg); },
  seerrSync() { return this.post("/api/seerr/sync"); },
  updaterStatus(force = false) {
    return this.get("/api/updater/status?" + new URLSearchParams({ force: String(force) }));
  },
  browseDir(path) { return this.get("/api/browse-dir?" + new URLSearchParams({ path: path || "" })); },

  clearCookies() { return this.post("/api/session/clear-cookies"); },

  watchlistGet() { return this.get("/api/watchlist"); },
  watchlistAdd(entry) { return this.post("/api/watchlist/add", entry); },
  watchlistMode(baseSlug, downloadMode) {
    return this.post("/api/watchlist/mode", { base_slug: baseSlug, download_mode: downloadMode });
  },
  watchlistRemove(baseSlugs) { return this.post("/api/watchlist/remove", { base_slugs: baseSlugs }); },
  watchlistCheck(baseSlugs) { return this.post("/api/watchlist/check", { base_slugs: baseSlugs || null }); },
  watchlistOpen(baseSlug) { return this.post("/api/watchlist/open", { base_slug: baseSlug }); },

  coverUrl(url) { return url ? "/api/cover?" + new URLSearchParams({ url }) : ""; },
};
