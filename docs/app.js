const data = window.VIDEOGAME_ATLAS_DATA || { metadata: {}, systems: [], games: [] };
const DEFAULT_VISIBLE_GAMES = 120;
const REGION_ATLAS = {
  na: { code: "na", label: "North America", shortLabel: "NA", flag: "🇺🇸", x: 205, y: 170 },
  us: { code: "us", label: "United States", shortLabel: "US", flag: "🇺🇸", x: 215, y: 173 },
  eu: { code: "eu", label: "Europe", shortLabel: "EU", flag: "🇪🇺", x: 510, y: 126 },
  jp: { code: "jp", label: "Japan", shortLabel: "JP", flag: "🇯🇵", x: 818, y: 174 },
  uk: { code: "uk", label: "United Kingdom", shortLabel: "UK", flag: "🇬🇧", x: 485, y: 114 },
  wr: { code: "wr", label: "Worldwide", shortLabel: "World", flag: "🇺🇳", x: 500, y: 220 },
  br: { code: "br", label: "Brazil", shortLabel: "BR", flag: "🇧🇷", x: 304, y: 324 },
  kr: { code: "kr", label: "South Korea", shortLabel: "KR", flag: "🇰🇷", x: 776, y: 164 },
  cn: { code: "cn", label: "China", shortLabel: "CN", flag: "🇨🇳", x: 724, y: 174 },
  in: { code: "in", label: "India", shortLabel: "IN", flag: "🇮🇳", x: 653, y: 212 },
  ru: { code: "ru", label: "Russia", shortLabel: "RU", flag: "🇷🇺", x: 665, y: 90 },
};
const SYSTEM_LOGO_OVERRIDES = {
  dreamcast: { brand: "Sega", title: "Dreamcast" },
  gamecube: { brand: "Nintendo", title: "GameCube" },
  gb: { brand: "Nintendo", title: "Game Boy" },
  gba: { brand: "Nintendo", title: "Game Boy Advance" },
  gbc: { brand: "Nintendo", title: "Game Boy Color" },
  mastersystem: { brand: "Sega", title: "Master System" },
  megadrive: { brand: "Sega", title: "Mega Drive" },
  n64: { brand: "Nintendo", title: "Nintendo 64" },
  nes: { brand: "Nintendo", title: "NES" },
  psp: { brand: "Sony", title: "PSP" },
  psx: { brand: "Sony", title: "PlayStation" },
  saturn: { brand: "Sega", title: "Saturn" },
  snes: { brand: "Nintendo", title: "Super NES" },
};
const SYSTEM_LOGO_PALETTES = {
  Nintendo: {
    start: "#aa2424",
    end: "#5b1111",
    glow: "#ffb784",
    accent: "#ffe1bf",
    detail: "#ffd29f",
    ink: "#fff8f0",
    muted: "#ffd7c2",
  },
  Sega: {
    start: "#164cce",
    end: "#0d1f69",
    glow: "#45d0c0",
    accent: "#c7f8ff",
    detail: "#7bd5ff",
    ink: "#f6fbff",
    muted: "#d4e9ff",
  },
  Sony: {
    start: "#2f3443",
    end: "#0e1017",
    glow: "#7ab3ff",
    accent: "#e4f0ff",
    detail: "#8dc9ff",
    ink: "#fbfdff",
    muted: "#ccd6e6",
  },
  Microsoft: {
    start: "#1b6b39",
    end: "#0f2e1d",
    glow: "#a6eb8d",
    accent: "#eefeda",
    detail: "#c3f0b1",
    ink: "#f6fff6",
    muted: "#d4e8d6",
  },
  Atari: {
    start: "#8f3d00",
    end: "#351500",
    glow: "#ffae6b",
    accent: "#ffe7cf",
    detail: "#ffc38c",
    ink: "#fff8f3",
    muted: "#f5d8c4",
  },
  default: {
    start: "#1c3442",
    end: "#0a131c",
    glow: "#45d0c0",
    accent: "#f4eedf",
    detail: "#8fd6cb",
    ink: "#f4eedf",
    muted: "#bfd0ca",
  },
};
const SYSTEM_LOGO_CACHE = new Map();
const EXCLUDED_TITLE_PATTERNS = [/~\s*(hack|homebrew|unlicensed|demo|prototype)\s*~/i];

function normalizeTitle(title) {
  return String(title || "").replace(/^the\s+/i, "").trim();
}

function buildSearchBlob(parts) {
  return parts
    .flatMap((part) => (Array.isArray(part) ? part : [part]))
    .filter(Boolean)
    .join(" ")
    .toLowerCase();
}

function isExcludedGame(game) {
  const title = String(game?.title || "");
  return EXCLUDED_TITLE_PATTERNS.some((pattern) => pattern.test(title));
}

function parseRegionCodes(rawRegion) {
  return Array.from(
    new Set(
      String(rawRegion || "")
        .toLowerCase()
        .split(/[\s,;|/]+/)
        .map((part) => part.trim())
        .filter(Boolean)
    )
  );
}

function getRegionEntries(rawRegion) {
  return parseRegionCodes(rawRegion)
    .map((code) => REGION_ATLAS[code] || null)
    .filter(Boolean);
}

function getProviders() {
  return data.metadata.providers || data.metadata.batoceraProviders || [];
}

function getCatalogSourceLabel() {
  const source = String(data.metadata?.catalogSource || "").toLowerCase();
  if (source === "retroachievements") return "RetroAchievements";
  if (source === "thegamesdb") return "TheGamesDB";
  return "the current catalog source";
}

function getReleaseInfo(game) {
  return game.releaseInfo || game.batocera || {};
}

function getChunkManifest() {
  if (Array.isArray(data.chunkManifest)) return data.chunkManifest;
  if (Array.isArray(data.metadata?.chunkManifest)) return data.metadata.chunkManifest;
  return [];
}

const systems = data.systems.map((system) => ({
  ...system,
  manufacturer: system.manufacturer || "Unknown",
  category: system.category || "Unspecified",
  generation: system.generation || "Unspecified",
  searchBlob: buildSearchBlob([
    system.name,
    system.shortName,
    system.manufacturer,
    system.category,
    system.generation,
    system.summary,
    system.topGenres,
  ]),
}));

const systemById = new Map(systems.map((system) => [system.id, system]));
const providerById = new Map(getProviders().map((provider) => [provider.id, provider]));
const chunkManifest = getChunkManifest();
const chunkLoadState = {
  totalChunks: chunkManifest.length,
  loadedChunks: 0,
  failedChunks: 0,
  started: false,
};
const loadedChunkKeys = new Set();
let games = [];
let gameById = new Map();

function normalizeGameRecord(game) {
  const system = systemById.get(game.systemId);
  const sortTitle = normalizeTitle(game.title);
  const releaseInfo = getReleaseInfo(game);
  const regions = getRegionEntries(releaseInfo.regionCode || releaseInfo.region);
  return {
    ...game,
    sortTitle,
    regions,
    releaseInfo,
    systemName: system?.name || "Unknown",
    systemShortName: system?.shortName || system?.name || "Unknown",
    searchBlob: buildSearchBlob([
      game.title,
      sortTitle,
      system?.name,
      system?.shortName,
      system?.manufacturer,
      game.developer,
      game.publisher,
      game.genres,
      game.summary,
      releaseInfo.regionCode || releaseInfo.region,
      regions.map((region) => region.label),
      releaseInfo.players,
      releaseInfo.family,
      releaseInfo.language,
    ]),
  };
}

function appendGames(records) {
  records.forEach((game) => {
    if (!game || gameById.has(game.id) || isExcludedGame(game)) return;
    const normalized = normalizeGameRecord(game);
    games.push(normalized);
    gameById.set(normalized.id, normalized);
  });
}

appendGames(Array.isArray(data.games) ? data.games : []);

const state = {
  search: "",
  manufacturer: "all",
  category: "all",
  generation: "all",
  sort: "alpha",
  gamesLimit: DEFAULT_VISIBLE_GAMES,
  selectedSystemId: null,
  selectedGameId: null,
};

const elements = {
  heroStats: document.querySelector("#hero-stats"),
  searchInput: document.querySelector("#search-input"),
  manufacturerFilter: document.querySelector("#manufacturer-filter"),
  categoryFilter: document.querySelector("#category-filter"),
  generationFilter: document.querySelector("#generation-filter"),
  sortSelect: document.querySelector("#sort-select"),
  systemsCountHeading: document.querySelector("#systems-count-heading"),
  systemsCaption: document.querySelector("#systems-caption"),
  systemsList: document.querySelector("#systems-list"),
  gamesCountHeading: document.querySelector("#games-count-heading"),
  gamesCaption: document.querySelector("#games-caption"),
  gamesStatus: document.querySelector("#games-status"),
  gamesList: document.querySelector("#games-list"),
  loadMoreGamesButton: document.querySelector("#load-more-games-button"),
  detailPanel: document.querySelector("#detail-panel"),
  sourcesPanel: document.querySelector("#sources-panel"),
  clearFocusButton: document.querySelector("#clear-focus-button"),
};

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function formatNumber(value) {
  return new Intl.NumberFormat("en-US").format(value || 0);
}

function formatYear(value) {
  return value == null ? "Unknown" : String(value);
}

function formatYearRange(start, end) {
  if (start && end && start !== end) return `${start}-${end}`;
  return formatYear(start || end);
}

function getInitials(text) {
  const words = String(text || "")
    .split(/[^A-Za-z0-9]+/)
    .filter(Boolean);
  return words
    .slice(0, 2)
    .map((word) => word[0].toUpperCase())
    .join("") || "VG";
}

function truncate(value, maxLength = 140) {
  if (!value || value.length <= maxLength) return value || "";
  return `${value.slice(0, maxLength - 1).trimEnd()}...`;
}

function escapeAttribute(value) {
  return String(value ?? "").replaceAll('"', "&quot;");
}

function escapeSvgText(value) {
  return escapeHtml(value);
}

function splitLogoLines(value, maxLineLength = 14, maxLines = 2) {
  const words = String(value || "System")
    .split(/\s+/)
    .map((word) => word.trim())
    .filter(Boolean);

  if (!words.length) return ["System"];

  const lines = [];
  let current = words[0];

  for (let index = 1; index < words.length; index += 1) {
    const word = words[index];
    if (`${current} ${word}`.length <= maxLineLength || lines.length >= maxLines - 1) {
      current = `${current} ${word}`;
      continue;
    }
    lines.push(current);
    current = word;
  }

  lines.push(current);
  if (lines.length <= maxLines) return lines;
  const kept = lines.slice(0, maxLines - 1);
  kept.push(lines.slice(maxLines - 1).join(" "));
  return kept;
}

function getSystemLogoProfile(system) {
  const override = SYSTEM_LOGO_OVERRIDES[system.key] || {};
  const brand = override.brand || system.manufacturer || "Platform";
  const palette =
    SYSTEM_LOGO_PALETTES[brand] ||
    SYSTEM_LOGO_PALETTES[system.manufacturer] ||
    SYSTEM_LOGO_PALETTES.default;
  const title =
    override.title ||
    (String(system.shortName || "").length <= 18 ? system.shortName : "") ||
    system.name ||
    "System";
  return {
    brand,
    title,
    subtitle: system.category || "Platform",
    year: system.releaseYear || "",
    palette,
  };
}

function buildSystemLogoDataUrl(system) {
  const cacheKey = JSON.stringify([
    system.key,
    system.name,
    system.shortName,
    system.manufacturer,
    system.category,
    system.releaseYear,
    system.logo?.url,
  ]);
  if (SYSTEM_LOGO_CACHE.has(cacheKey)) {
    return SYSTEM_LOGO_CACHE.get(cacheKey);
  }

  const explicitUrl = system.logo?.url;
  if (explicitUrl) {
    SYSTEM_LOGO_CACHE.set(cacheKey, explicitUrl);
    return explicitUrl;
  }

  const profile = getSystemLogoProfile(system);
  const titleLines = splitLogoLines(profile.title, profile.title.length > 16 ? 12 : 14, 3);
  const titleFontSize = titleLines.length > 2 || profile.title.length > 18 ? 34 : 42;
  const titleLineHeight = Math.round(titleFontSize * 0.92);
  const titleStartY = titleLines.length > 2 ? 108 : 116;
  const encodedSvg = encodeURIComponent(`
    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 320 220" role="img" aria-label="${escapeSvgText(
      `${system.name || profile.title} logo`
    )}">
      <defs>
        <linearGradient id="bg" x1="0%" y1="0%" x2="100%" y2="100%">
          <stop offset="0%" stop-color="${profile.palette.start}"/>
          <stop offset="100%" stop-color="${profile.palette.end}"/>
        </linearGradient>
        <radialGradient id="glow" cx="25%" cy="12%" r="65%">
          <stop offset="0%" stop-color="${profile.palette.glow}" stop-opacity="0.62"/>
          <stop offset="100%" stop-color="${profile.palette.glow}" stop-opacity="0"/>
        </radialGradient>
      </defs>
      <rect width="320" height="220" rx="30" fill="url(#bg)"/>
      <rect x="18" y="18" width="284" height="184" rx="22" fill="none" stroke="${profile.palette.detail}" stroke-opacity="0.22"/>
      <circle cx="84" cy="38" r="82" fill="url(#glow)"/>
      <rect x="26" y="24" width="146" height="26" rx="13" fill="${profile.palette.accent}" fill-opacity="0.14"/>
      <text x="38" y="41" fill="${profile.palette.ink}" font-size="14" font-family="Trebuchet MS, Arial, sans-serif" letter-spacing="2.4" font-weight="700">${escapeSvgText(
        profile.brand.toUpperCase()
      )}</text>
      ${titleLines
        .map(
          (line, index) => `
            <text
              x="30"
              y="${titleStartY + index * titleLineHeight}"
              fill="${profile.palette.ink}"
              font-size="${titleFontSize}"
              font-family="Arial Black, Trebuchet MS, Arial, sans-serif"
              font-weight="900"
              letter-spacing="0.8"
            >${escapeSvgText(line)}</text>
          `
        )
        .join("")}
      <rect x="30" y="167" width="260" height="2" rx="1" fill="${profile.palette.detail}" fill-opacity="0.66"/>
      <text x="30" y="191" fill="${profile.palette.muted}" font-size="15" font-family="Trebuchet MS, Arial, sans-serif" letter-spacing="1.5" font-weight="700">${escapeSvgText(
        String(profile.subtitle || "").toUpperCase()
      )}</text>
      <text x="290" y="191" text-anchor="end" fill="${profile.palette.accent}" font-size="16" font-family="Trebuchet MS, Arial, sans-serif" font-weight="700">${escapeSvgText(
        String(profile.year || "")
      )}</text>
    </svg>
  `);
  const dataUrl = `data:image/svg+xml;charset=UTF-8,${encodedSvg}`;
  SYSTEM_LOGO_CACHE.set(cacheKey, dataUrl);
  return dataUrl;
}

function getProviderName(providerId) {
  const provider = providerById.get(providerId);
  return provider?.name || provider?.legacyLabel || provider?.batoceraLabel || providerId || "Unknown";
}

function renderPoster(game, className = "game-poster") {
  const iconUrl = game.media?.boxFront?.url || game.image?.iconUrl;
  if (iconUrl) {
    return `<div class="${className}"><img src="${escapeHtml(iconUrl)}" alt="${escapeHtml(
      game.media?.boxFront?.alt || game.image?.alt || game.title
    )}" loading="lazy" decoding="async"></div>`;
  }
  return `
    <div class="${className}">
      <div>
        <strong>${escapeHtml(getInitials(game.title))}</strong>
        <small>${escapeHtml(game.systemShortName)}</small>
      </div>
    </div>
  `;
}

function formatRegionLabel(game) {
  if (!game.regions?.length) return game.releaseInfo?.regionCode || game.releaseInfo?.region || "Unknown";
  return game.regions.map((region) => region.label).join(" / ");
}

function getChunkProgressText() {
  if (!chunkManifest.length) return "";
  const pieces = [
    `Loaded ${formatNumber(games.length)} visible games`,
    `from ${formatNumber(chunkLoadState.loadedChunks)} of ${formatNumber(chunkLoadState.totalChunks)} system files`,
  ];
  if (chunkLoadState.failedChunks) {
    pieces.push(`${formatNumber(chunkLoadState.failedChunks)} failed`);
  }
  return pieces.join(" ");
}

function loadChunkScript(chunk) {
  if (!chunk?.key || loadedChunkKeys.has(chunk.key)) return Promise.resolve();

  return new Promise((resolve) => {
    const script = document.createElement("script");
    script.src = chunk.path;
    script.async = true;
    script.onload = () => {
      const payload = (window.VIDEOGAME_ATLAS_CHUNKS || {})[chunk.key];
      const chunkGames = Array.isArray(payload?.games) ? payload.games : [];
      loadedChunkKeys.add(chunk.key);
      chunkLoadState.loadedChunks += 1;
      appendGames(chunkGames);
      resetGameWindow();
      render();
      script.remove();
      resolve();
    };
    script.onerror = () => {
      loadedChunkKeys.add(chunk.key);
      chunkLoadState.failedChunks += 1;
      render();
      script.remove();
      resolve();
    };
    document.head.appendChild(script);
  });
}

function startChunkLoading() {
  if (!chunkManifest.length || chunkLoadState.started) return;
  chunkLoadState.started = true;
  Promise.allSettled(chunkManifest.map((chunk) => loadChunkScript(chunk))).then(() => {
    render();
  });
}

function renderRegionBadges(game, className = "badge-row") {
  if (!game.regions?.length) return "";
  return `
    <div class="${className}">
      ${game.regions
        .map(
          (region) => `
            <span class="badge region-badge" title="${escapeAttribute(region.label)}">
              <span class="region-flag" aria-hidden="true">${escapeHtml(region.flag)}</span>
              <span>${escapeHtml(region.shortLabel)}</span>
            </span>
          `
        )
        .join("")}
    </div>
  `;
}

function renderRegionAtlasMap(game) {
  if (!game.regions?.length) {
    return `<div class="empty-state">No release-region marker is available for this game.</div>`;
  }

  const markers = game.regions
    .map(
      (region, index) => `
        <g class="world-marker-group" style="--marker-delay: ${index * 0.12}s" transform="translate(${region.x} ${region.y})">
          <circle class="world-marker-pulse" r="18"></circle>
          <circle class="world-marker-core" r="7"></circle>
          <text class="world-marker-label" x="14" y="-12">${escapeSvgText(region.flag)} ${escapeSvgText(
            region.label
          )}</text>
        </g>
      `
    )
    .join("");

  return `
    <div class="world-map-shell">
      <svg class="world-map" viewBox="0 0 1000 520" role="img" aria-label="${escapeAttribute(
        `Approximate release region map for ${game.title}`
      )}">
        <defs>
          <pattern id="atlas-grid" width="100" height="52" patternUnits="userSpaceOnUse">
            <path d="M 100 0 L 0 0 0 52" fill="none" stroke="rgba(150, 205, 198, 0.08)" stroke-width="1"></path>
          </pattern>
        </defs>
        <rect x="0" y="0" width="1000" height="520" rx="26" fill="url(#atlas-grid)"></rect>
        <g class="world-graticule">
          <path d="M 0 260 H 1000"></path>
          <path d="M 500 0 V 520"></path>
          <path d="M 250 0 V 520"></path>
          <path d="M 750 0 V 520"></path>
          <path d="M 0 130 H 1000"></path>
          <path d="M 0 390 H 1000"></path>
        </g>
        <g class="world-land">
          <path d="M108 118 L154 86 L228 88 L286 112 L315 150 L308 196 L272 214 L239 198 L221 176 L188 175 L174 196 L142 204 L109 182 L96 146 Z"></path>
          <path d="M254 224 L288 244 L316 286 L324 342 L303 418 L270 468 L238 438 L225 372 L234 304 Z"></path>
          <path d="M420 96 L476 83 L530 95 L568 118 L598 104 L646 114 L684 137 L714 146 L744 165 L737 193 L693 200 L650 190 L617 208 L586 204 L553 176 L525 168 L508 188 L474 194 L443 173 L414 147 Z"></path>
          <path d="M586 219 L621 238 L648 279 L662 332 L642 375 L612 392 L586 370 L570 330 L556 284 Z"></path>
          <path d="M708 188 L748 176 L796 182 L842 204 L890 238 L906 274 L892 305 L856 315 L829 296 L806 266 L778 252 L738 240 L714 219 Z"></path>
          <path d="M812 355 L846 373 L880 403 L873 436 L839 452 L804 430 L790 394 Z"></path>
          <path d="M894 133 L929 117 L962 123 L972 144 L952 162 L915 161 L892 147 Z"></path>
        </g>
        ${markers}
      </svg>
    </div>
  `;
}

function renderSystemMark(system) {
  const logoUrl = buildSystemLogoDataUrl(system);
  if (logoUrl) {
    return `
      <div class="system-mark">
        <img class="system-logo-image" src="${escapeAttribute(logoUrl)}" alt="${escapeAttribute(
          `${system.name} logo`
        )}" loading="lazy" decoding="async">
      </div>
    `;
  }
  return `
    <div class="system-mark">
      <div>
        <strong>${escapeHtml(getInitials(system.shortName || system.name))}</strong>
        <small>${escapeHtml(system.manufacturer || "System")}</small>
      </div>
    </div>
  `;
}

function renderSystemPoster(system, className = "detail-poster system-poster") {
  const logoUrl = buildSystemLogoDataUrl(system);
  if (logoUrl) {
    return `
      <div class="${className}">
        <img class="system-logo-image" src="${escapeAttribute(logoUrl)}" alt="${escapeAttribute(
          `${system.name} logo`
        )}" decoding="async">
      </div>
    `;
  }
  return `
    <div class="${className}">
      <div>
        <strong>${escapeHtml(getInitials(system.shortName || system.name))}</strong>
        <small>${escapeHtml(system.manufacturer || "System")}</small>
      </div>
    </div>
  `;
}

function getDistinctSystemValues(extractor) {
  return Array.from(new Set(systems.map(extractor).filter(Boolean))).sort((a, b) =>
    a.localeCompare(b)
  );
}

function populateFilterSelect(select, label, values) {
  select.innerHTML = [
    `<option value="all">All ${escapeHtml(label)}</option>`,
    ...values.map(
      (value) => `<option value="${escapeHtml(value)}">${escapeHtml(value)}</option>`
    ),
  ].join("");
}

function sortGames(items) {
  const sorted = [...items];
  sorted.sort((left, right) => {
    if (state.sort === "year") {
      return (
        (right.releaseYear || 0) - (left.releaseYear || 0) ||
        left.sortTitle.localeCompare(right.sortTitle)
      );
    }
    if (state.sort === "system") {
      return (
        left.systemName.localeCompare(right.systemName) ||
        left.sortTitle.localeCompare(right.sortTitle)
      );
    }
    return left.sortTitle.localeCompare(right.sortTitle);
  });
  return sorted;
}

function getVisibleData() {
  const query = state.search.trim().toLowerCase();
  const eligibleSystems = systems.filter((system) => {
    if (state.manufacturer !== "all" && system.manufacturer !== state.manufacturer) return false;
    if (state.category !== "all" && system.category !== state.category) return false;
    if (state.generation !== "all" && system.generation !== state.generation) return false;
    return true;
  });

  const eligibleSystemIds = new Set(eligibleSystems.map((system) => system.id));
  const systemMatchesQuery = new Map(
    eligibleSystems.map((system) => [system.id, !query || system.searchBlob.includes(query)])
  );

  const visibleGames = sortGames(
    games.filter((game) => {
      if (!eligibleSystemIds.has(game.systemId)) return false;
      if (!query) return true;
      return systemMatchesQuery.get(game.systemId) || game.searchBlob.includes(query);
    })
  );

  const visibleSystemIds = new Set(visibleGames.map((game) => game.systemId));
  const visibleSystems = eligibleSystems.filter((system) => {
    if (systemMatchesQuery.get(system.id)) return true;
    return visibleSystemIds.has(system.id);
  });

  return { visibleSystems, visibleGames };
}

function syncSelection(views) {
  if (state.selectedSystemId != null && !views.visibleSystems.some((item) => item.id === state.selectedSystemId)) {
    state.selectedSystemId = null;
  }

  if (state.selectedGameId != null) {
    const selectedGame = views.visibleGames.find((item) => item.id === state.selectedGameId);
    if (!selectedGame) {
      state.selectedGameId = null;
    } else {
      state.selectedSystemId = selectedGame.systemId;
    }
  }
}

function getVisibleGameMetrics(visibleGames) {
  const counts = new Map();

  visibleGames.forEach((game) => {
    counts.set(game.systemId, (counts.get(game.systemId) || 0) + 1);
  });

  return { counts };
}

function getActiveGames(visibleGames) {
  if (state.selectedSystemId == null) return visibleGames;
  return visibleGames.filter((game) => game.systemId === state.selectedSystemId);
}

function buildHeroStats(visibleSystems, visibleGames) {
  const manufacturerCount = new Set(visibleSystems.map((system) => system.manufacturer).filter(Boolean)).size;
  const categoryCount = new Set(visibleSystems.map((system) => system.category).filter(Boolean)).size;

  const cards = [
    ["Filtered systems", formatNumber(visibleSystems.length)],
    ["Visible games", formatNumber(visibleGames.length)],
    ["Manufacturers", formatNumber(manufacturerCount)],
    ["Categories", formatNumber(categoryCount)],
  ];

  elements.heroStats.innerHTML = cards
    .map(
      ([label, value]) => `
        <article class="stat-card">
          <span>${escapeHtml(label)}</span>
          <strong>${escapeHtml(value)}</strong>
        </article>
      `
    )
    .join("");
}

function renderSystemsList(visibleSystems, visibleGames) {
  const metrics = getVisibleGameMetrics(visibleGames);
  elements.systemsCountHeading.textContent = `${formatNumber(visibleSystems.length)} systems`;

  if (!visibleSystems.length) {
    elements.systemsCaption.textContent =
      "No systems match the current filters. Try widening manufacturer, category, or generation.";
    elements.systemsList.innerHTML = `<div class="empty-state">No systems are visible right now.</div>`;
    return;
  }

  elements.systemsCaption.textContent =
    state.selectedSystemId == null
      ? chunkManifest.length
        ? `Select a system to focus the game list and open a platform summary. ${getChunkProgressText()}.`
        : "Select a system to focus the game list and open a platform summary."
      : chunkManifest.length
        ? `System focus is active. Click another system or clear focus to go broader. ${getChunkProgressText()}.`
        : "System focus is active. Click another system or clear focus to go broader.";

  elements.systemsList.innerHTML = visibleSystems
    .map((system) => {
      const visibleCount = metrics.counts.get(system.id) || 0;

      return `
        <button
          class="system-card ${system.id === state.selectedSystemId ? "is-selected" : ""}"
          type="button"
          data-system-id="${system.id}"
        >
          <div class="card-head">
            ${renderSystemMark(system)}
            <div class="card-copy">
              <h3>${escapeHtml(system.name)}</h3>
              <div class="meta-row">
                <span>${escapeHtml(system.manufacturer || "Unknown maker")}</span>
                <span>${escapeHtml(system.category)}</span>
                <span>${escapeHtml(formatYearRange(system.releaseYear, system.endYear))}</span>
              </div>
              <p class="subtle">${escapeHtml(system.generation)}</p>
            </div>
          </div>
          <div class="badge-row">
            <span class="badge">${escapeHtml(
              `Metadata: ${getProviderName(system.sourceAttribution?.metadataProvider)}`
            )}</span>
            <span class="badge accent">${escapeHtml(formatNumber(visibleCount))} visible games</span>
          </div>
          <div class="badge-row">
            ${system.topGenres
              .map((genre) => `<span class="badge">${escapeHtml(genre)}</span>`)
              .join("")}
          </div>
        </button>
      `;
    })
    .join("");

  elements.systemsList.querySelectorAll("[data-system-id]").forEach((button) => {
    button.addEventListener("click", () => {
      const systemId = Number(button.dataset.systemId);
      state.selectedGameId = null;
      state.selectedSystemId = state.selectedSystemId === systemId ? null : systemId;
      resetGameWindow();
      render();
    });
  });
}

function renderGamesList(activeGames, selectedSystem) {
  elements.gamesCountHeading.textContent = `${formatNumber(activeGames.length)} games`;

  if (!activeGames.length) {
    elements.gamesCaption.textContent = chunkManifest.length && chunkLoadState.loadedChunks < chunkLoadState.totalChunks
      ? "Game files are still loading. Results will expand as more system chunks arrive."
      : "No games match the current filters. Try clearing search or widening the system filters.";
    elements.gamesStatus.textContent = getChunkProgressText();
    elements.loadMoreGamesButton.hidden = true;
    elements.gamesList.innerHTML = `<div class="empty-state">${
      chunkManifest.length && chunkLoadState.loadedChunks < chunkLoadState.totalChunks
        ? "Loading game chunks for the current atlas..."
        : "No game entries are visible right now."
    }</div>`;
    return;
  }

  const renderedGames = activeGames.slice(0, state.gamesLimit);
  const hiddenCount = Math.max(activeGames.length - renderedGames.length, 0);
  elements.gamesCaption.textContent = selectedSystem
    ? `Showing game entries for ${selectedSystem.name}.`
    : "Showing game entries across all currently visible systems.";
  const renderStatus = hiddenCount
    ? `Rendering the first ${formatNumber(renderedGames.length)} of ${formatNumber(
        activeGames.length
      )} matches for speed.`
    : `Rendering all ${formatNumber(activeGames.length)} matches in the current view.`;
  elements.gamesStatus.textContent = chunkManifest.length
    ? `${renderStatus} ${getChunkProgressText()}.`
    : renderStatus;
  elements.loadMoreGamesButton.hidden = hiddenCount === 0;
  elements.loadMoreGamesButton.textContent = `Show ${formatNumber(Math.min(hiddenCount, DEFAULT_VISIBLE_GAMES))} more`;

  elements.gamesList.innerHTML = renderedGames
    .map(
      (game) => `
        <button
          class="game-card ${game.id === state.selectedGameId ? "is-selected" : ""}"
          type="button"
          data-game-id="${game.id}"
        >
          <div class="card-head">
            ${renderPoster(game)}
            <div class="card-copy">
              <h3>${escapeHtml(game.title)}</h3>
              <div class="meta-row">
                <span>${escapeHtml(game.systemName)}</span>
                <span>${escapeHtml(formatYear(game.releaseYear))}</span>
                <span>${escapeHtml(game.developer || "Developer unknown")}</span>
              </div>
              <p class="subtle">${escapeHtml(truncate(game.summary || "No summary in the current bundle.", 132))}</p>
            </div>
          </div>
          ${renderRegionBadges(game)}
          <div class="badge-row">
            <span class="badge">${escapeHtml(
              `Metadata: ${getProviderName(game.sourceAttribution?.metadataProvider)}`
            )}</span>
            <span class="badge">${escapeHtml(
              `Box art: ${getProviderName(game.sourceAttribution?.boxArtProvider)}`
            )}</span>
          </div>
          <div class="badge-row">
            ${game.genres.map((genre) => `<span class="badge">${escapeHtml(genre)}</span>`).join("")}
          </div>
        </button>
      `
    )
    .join("");

  elements.gamesList.querySelectorAll("[data-game-id]").forEach((button) => {
    button.addEventListener("click", () => {
      const gameId = Number(button.dataset.gameId);
      const game = gameById.get(gameId);
      state.selectedGameId = gameId;
      state.selectedSystemId = game?.systemId || state.selectedSystemId;
      render();
    });
  });
}

function summarizeSystemGames(systemGames) {
  const topDevelopers = Array.from(
    systemGames.reduce((map, game) => {
      if (!game.developer) return map;
      map.set(game.developer, (map.get(game.developer) || 0) + 1);
      return map;
    }, new Map())
  )
    .sort((left, right) => right[1] - left[1] || left[0].localeCompare(right[0]))
    .slice(0, 4)
    .map(([name]) => name);

  return { topDevelopers };
}

function renderDetailPanel(selectedSystem, selectedGame, visibleGames) {
  if (selectedGame) {
    elements.detailPanel.innerHTML = `
      <div class="detail-hero">
        <div class="detail-poster">
          <div>
            <strong>${escapeHtml(getInitials(selectedGame.title))}</strong>
            <small>${escapeHtml(selectedGame.systemShortName)}</small>
          </div>
        </div>
        <div class="detail-copy-block">
          <p class="panel-label">Game Release Entry</p>
          <h2>${escapeHtml(selectedGame.title)}</h2>
          <p class="detail-copy">${escapeHtml(
            selectedGame.summary || "This release entry has no longer editorial summary in the current bundle."
          )}</p>
        </div>
      </div>
      <div class="fact-grid">
        <article class="fact-card">
          <span>System</span>
          <strong>${escapeHtml(selectedGame.systemName)}</strong>
        </article>
        <article class="fact-card">
          <span>Release year</span>
          <strong>${escapeHtml(formatYear(selectedGame.releaseYear))}</strong>
        </article>
        <article class="fact-card">
          <span>Developer</span>
          <strong>${escapeHtml(selectedGame.developer || "Unknown")}</strong>
        </article>
        <article class="fact-card">
          <span>Publisher</span>
          <strong>${escapeHtml(selectedGame.publisher || "Unknown")}</strong>
        </article>
        <article class="fact-card">
          <span>Players</span>
          <strong>${escapeHtml(selectedGame.releaseInfo?.players || "Unknown")}</strong>
        </article>
        <article class="fact-card">
          <span>Region</span>
          <strong>${escapeHtml(formatRegionLabel(selectedGame))}</strong>
        </article>
        <article class="fact-card">
          <span>Metadata source</span>
          <strong>${escapeHtml(getProviderName(selectedGame.sourceAttribution?.metadataProvider))}</strong>
        </article>
        <article class="fact-card">
          <span>Box art source</span>
          <strong>${escapeHtml(getProviderName(selectedGame.sourceAttribution?.boxArtProvider))}</strong>
        </article>
      </div>
      <section class="detail-section">
        <h3>Genres</h3>
        <div class="chip-row">
          ${selectedGame.genres.length
            ? selectedGame.genres.map((genre) => `<span class="chip">${escapeHtml(genre)}</span>`).join("")
            : '<span class="chip">Unspecified</span>'}
        </div>
      </section>
      <section class="detail-section">
        <h3>Release Region Atlas</h3>
        <div class="region-atlas-card">
          <div class="chip-row">
            ${selectedGame.regions?.length
              ? selectedGame.regions
                  .map(
                    (region) => `
                      <span class="chip region-chip">
                        <span class="region-flag" aria-hidden="true">${escapeHtml(region.flag)}</span>
                        <span>${escapeHtml(region.label)}</span>
                      </span>
                    `
                  )
                  .join("")
              : '<span class="chip">Unknown release region</span>'}
          </div>
          ${renderRegionAtlasMap(selectedGame)}
          <p class="subtle">Marker placement is approximate and based on the imported release-region code.</p>
        </div>
      </section>
      <section class="detail-section">
        <h3>Release metadata</h3>
        <div class="chip-row">
          ${selectedGame.releaseInfo?.family ? `<span class="chip">${escapeHtml(`Family: ${selectedGame.releaseInfo.family}`)}</span>` : ""}
          ${selectedGame.releaseInfo?.language ? `<span class="chip">${escapeHtml(`Language: ${selectedGame.releaseInfo.language}`)}</span>` : ""}
          ${selectedGame.releaseInfo?.rating != null ? `<span class="chip">${escapeHtml(`Rating: ${selectedGame.releaseInfo.rating}`)}</span>` : ""}
          ${selectedGame.sourceAttribution?.thegamesdbGameId ? `<span class="chip">${escapeHtml(`TheGamesDB ID: ${selectedGame.sourceAttribution.thegamesdbGameId}`)}</span>` : ""}
          ${selectedGame.sourceAttribution?.scraperGameId ? `<span class="chip">${escapeHtml(`ScreenScraper ID: ${selectedGame.sourceAttribution.scraperGameId}`)}</span>` : ""}
          ${selectedGame.releaseInfo?.regionId != null ? `<span class="chip">${escapeHtml(`Region ID: ${selectedGame.releaseInfo.regionId}`)}</span>` : ""}
          ${selectedGame.releaseInfo?.countryId != null ? `<span class="chip">${escapeHtml(`Country ID: ${selectedGame.releaseInfo.countryId}`)}</span>` : ""}
        </div>
      </section>
    `;
    return;
  }

  if (selectedSystem) {
    const systemGames = visibleGames.filter((game) => game.systemId === selectedSystem.id);
    const summary = summarizeSystemGames(systemGames);
    elements.detailPanel.innerHTML = `
      <div class="detail-hero">
        ${renderSystemPoster(selectedSystem)}
        <div class="detail-copy-block">
          <p class="panel-label">System Overview</p>
          <h2>${escapeHtml(selectedSystem.name)}</h2>
          <p class="detail-copy">${escapeHtml(
            selectedSystem.summary ||
              "This system is present in the current bundle, but it does not yet have a longer editorial summary."
          )}</p>
        </div>
      </div>
      <div class="fact-grid">
        <article class="fact-card">
          <span>Manufacturer</span>
          <strong>${escapeHtml(selectedSystem.manufacturer || "Unknown")}</strong>
        </article>
        <article class="fact-card">
          <span>Generation</span>
          <strong>${escapeHtml(selectedSystem.generation)}</strong>
        </article>
        <article class="fact-card">
          <span>Metadata source</span>
          <strong>${escapeHtml(getProviderName(selectedSystem.sourceAttribution?.metadataProvider))}</strong>
        </article>
        <article class="fact-card">
          <span>Visible games</span>
          <strong>${escapeHtml(formatNumber(systemGames.length))}</strong>
        </article>
        <article class="fact-card">
          <span>Launch window</span>
          <strong>${escapeHtml(formatYearRange(selectedSystem.releaseYear, selectedSystem.endYear))}</strong>
        </article>
        <article class="fact-card">
          <span>Primary category</span>
          <strong>${escapeHtml(selectedSystem.category)}</strong>
        </article>
      </div>
      <section class="detail-section">
        <h3>Top genres</h3>
        <div class="chip-row">
          ${selectedSystem.topGenres.length
            ? selectedSystem.topGenres.map((genre) => `<span class="chip">${escapeHtml(genre)}</span>`).join("")
            : '<span class="chip">No genre data</span>'}
        </div>
      </section>
      <section class="detail-section">
        <h3>Frequent developers in the current view</h3>
        <div class="chip-row">
          ${summary.topDevelopers.length
            ? summary.topDevelopers.map((name) => `<span class="chip">${escapeHtml(name)}</span>`).join("")
            : '<span class="chip">No developer data</span>'}
        </div>
      </section>
      <section class="detail-section">
        <h3>Sample games</h3>
        <div class="chip-row">
          ${systemGames.slice(0, 6).map((game) => `<span class="chip">${escapeHtml(game.title)}</span>`).join("")}
        </div>
      </section>
      ${
        selectedSystem.wikiUrl
          ? `
      <section class="detail-section">
        <h3>Reference link</h3>
        <div class="chip-row">
          <a class="chip" href="${escapeHtml(selectedSystem.wikiUrl)}">${escapeHtml(selectedSystem.wikiUrl)}</a>
        </div>
      </section>
      `
          : ""
      }
    `;
    return;
  }

  elements.detailPanel.innerHTML = `
    <div class="detail-section">
      <p class="panel-label">Catalog Overview</p>
      <h2>${escapeHtml(formatNumber(data.metadata.systemCount))} systems in the current bundle</h2>
      <p class="detail-copy">
        This draft is built from a curated ${escapeHtml(getCatalogSourceLabel())} import: systems at the top, release
        entries beneath them, and a chunked publish bundle designed for static hosting.
      </p>
    </div>
    <div class="fact-grid">
      <article class="fact-card">
        <span>Total systems</span>
        <strong>${escapeHtml(formatNumber(data.metadata.systemCount))}</strong>
      </article>
      <article class="fact-card">
        <span>Total game entries</span>
        <strong>${escapeHtml(formatNumber(data.metadata.gameCount))}</strong>
      </article>
      <article class="fact-card">
        <span>Manufacturers</span>
        <strong>${escapeHtml(formatNumber(data.metadata.manufacturers.length))}</strong>
      </article>
      <article class="fact-card">
        <span>Tracked providers</span>
        <strong>${escapeHtml(formatNumber(getProviders().length))}</strong>
      </article>
      <article class="fact-card">
        <span>Top genres tracked</span>
        <strong>${escapeHtml(formatNumber(data.metadata.topGenres.length))}</strong>
      </article>
    </div>
    <section class="detail-section">
      <h3>Notes</h3>
      <div class="chip-row">
        ${data.metadata.notes.map((note) => `<span class="chip">${escapeHtml(note)}</span>`).join("")}
      </div>
    </section>
    <section class="detail-section">
      <h3>Top genres in the bundle</h3>
      <div class="chip-row">
        ${data.metadata.topGenres.map((genre) => `<span class="chip">${escapeHtml(genre)}</span>`).join("")}
      </div>
    </section>
    <section class="detail-section">
      <h3>Atlas scrape strategy</h3>
      <div class="chip-row">
        ${Object.entries(data.metadata.atlasStrategy || {})
          .map(
            ([slot, providerIds]) =>
              `<span class="chip">${escapeHtml(
                `${slot}: ${providerIds.map((providerId) => getProviderName(providerId)).join(" -> ")}`
              )}</span>`
          )
          .join("")}
      </div>
    </section>
  `;
}

function renderSourcesPanel() {
  elements.sourcesPanel.innerHTML = `
    <p class="panel-label">Sources</p>
    <h2>Provider catalog</h2>
    <p class="detail-copy">
      The current atlas is built around public catalog providers. This draft currently points at
      ${escapeHtml(getCatalogSourceLabel())}, and the publish bundle is intentionally chunked so it stays portable without
      oversized tracked files.
    </p>
    <div class="source-list">
      ${getProviders()
        .map((provider) => {
          const usage = data.metadata.providerUsage?.[provider.id] || 0;
          return `
            <article class="source-card">
              <h3>${escapeHtml(provider.name)}</h3>
              <p>${escapeHtml(provider.notes || "")}</p>
              <div class="badge-row">
                <span class="badge accent">${escapeHtml(`Usage: ${formatNumber(usage)} games`)}</span>
                ${provider.pricing ? `<span class="badge">${escapeHtml(provider.pricing)}</span>` : ""}
              </div>
              <div class="badge-row">
                ${(provider.capabilities || [])
                  .map((capability) => `<span class="badge">${escapeHtml(capability)}</span>`)
                  .join("")}
              </div>
              <p>${escapeHtml(
                provider.credentials?.length
                  ? `Credentials: ${provider.credentials.join(", ")}`
                  : "Credentials: none noted"
              )}</p>
              <p>
                <a href="${escapeHtml(provider.websiteUrl)}">${escapeHtml(provider.websiteUrl)}</a>
                <br>
                <a href="${escapeHtml(provider.docsUrl)}">${escapeHtml(provider.docsUrl)}</a>
              </p>
            </article>
          `;
        })
        .join("")}
      ${data.metadata.sources
        .map(
          (source) => `
            <article class="source-card">
              <h3>${escapeHtml(source.name)}</h3>
              <p>${escapeHtml(source.role)}</p>
              ${
                source.url
                  ? `<p><a href="${escapeHtml(source.url)}">${escapeHtml(source.url)}</a></p>`
                  : ""
              }
            </article>
          `
        )
        .join("")}
    </div>
  `;
}

function render() {
  const views = getVisibleData();
  syncSelection(views);

  const selectedSystem =
    views.visibleSystems.find((system) => system.id === state.selectedSystemId) || null;
  const activeGames = getActiveGames(views.visibleGames);
  const selectedGame = activeGames.find((game) => game.id === state.selectedGameId) || null;

  buildHeroStats(views.visibleSystems, views.visibleGames);
  renderSystemsList(views.visibleSystems, views.visibleGames);
  renderGamesList(activeGames, selectedSystem);
  renderDetailPanel(selectedSystem, selectedGame, views.visibleGames);
}

function resetGameWindow() {
  state.gamesLimit = DEFAULT_VISIBLE_GAMES;
}

function initializeFilters() {
  populateFilterSelect(
    elements.manufacturerFilter,
    "manufacturers",
    getDistinctSystemValues((system) => system.manufacturer)
  );
  populateFilterSelect(
    elements.categoryFilter,
    "categories",
    getDistinctSystemValues((system) => system.category)
  );
  populateFilterSelect(
    elements.generationFilter,
    "generations",
    getDistinctSystemValues((system) => system.generation)
  );
}

function attachEvents() {
  elements.searchInput.addEventListener("input", (event) => {
    state.search = event.target.value;
    resetGameWindow();
    render();
  });

  elements.manufacturerFilter.addEventListener("change", (event) => {
    state.manufacturer = event.target.value;
    resetGameWindow();
    render();
  });

  elements.categoryFilter.addEventListener("change", (event) => {
    state.category = event.target.value;
    resetGameWindow();
    render();
  });

  elements.generationFilter.addEventListener("change", (event) => {
    state.generation = event.target.value;
    resetGameWindow();
    render();
  });

  elements.sortSelect.addEventListener("change", (event) => {
    state.sort = event.target.value;
    resetGameWindow();
    render();
  });

  elements.clearFocusButton.addEventListener("click", () => {
    state.selectedSystemId = null;
    state.selectedGameId = null;
    resetGameWindow();
    render();
  });

  elements.loadMoreGamesButton.addEventListener("click", () => {
    state.gamesLimit += DEFAULT_VISIBLE_GAMES;
    render();
  });
}

initializeFilters();
renderSourcesPanel();
attachEvents();
render();
startChunkLoading();
