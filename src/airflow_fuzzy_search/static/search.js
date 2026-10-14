// Fuzzy search overlay for the Airflow 3 UI.
//
// Airflow loads this module as a React app mounted in the base layout of every page. The component
// itself renders nothing; on first load the module installs a Cmd/Ctrl+K listener that runs before
// the UI's own hotkey handler, and opens its own overlay in a shadow root instead.
//
// DAGs and tasks come from Airflow's public REST API, called with the page's session cookie, so the
// overlay only ever sees what the logged-in user may see. Nothing is written, and every string from
// the API is put into the page as text, never as HTML.

// The plugin passes its settings as query parameters of this module's URL.
const SETTINGS = new URL(import.meta.url).searchParams;
// Searching docs costs one more request per DAG (its details). [fuzzy_search] include_docs = False
// in the Airflow config turns it off for everybody; otherwise each user has a switch in the overlay.
const DOCS_AVAILABLE = SETTINGS.get("docs") !== "0";

const GLOBAL_KEY = "__airflowFuzzySearch";
const CACHE_PREFIX = "airflow-fuzzy-search:index:v1";
const RECENT_KEY = "airflow-fuzzy-search:recent:v1";
const DOCS_KEY = "airflow-fuzzy-search:docs:v1";
const CACHE_FRESH_MS = 60 * 1000;
const PAGE_LIMIT = 100;
const FETCH_CONCURRENCY = 6;
const MAX_RESULTS = 50;
const MAX_DOC_RESULTS = 20;
const MAX_RECENT = 8;

// ---------------------------------------------------------------------------------------------
// Data
// ---------------------------------------------------------------------------------------------

async function getJSON(path) {
  const response = await fetch(new URL(path, document.baseURI), {
    credentials: "same-origin",
    headers: { Accept: "application/json" },
  });
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText} for ${path}`);
  }
  return response.json();
}

async function fetchDags() {
  const dags = [];
  for (let offset = 0; ; ) {
    const page = await getJSON(`api/v2/dags?limit=${PAGE_LIMIT}&offset=${offset}&order_by=dag_id`);
    dags.push(...page.dags);
    offset += page.dags.length;
    if (page.dags.length === 0 || offset >= page.total_entries) {
      return dags;
    }
  }
}

async function mapConcurrent(items, limit, fn) {
  const results = new Array(items.length);
  let next = 0;
  async function worker() {
    while (next < items.length) {
      const index = next++;
      results[index] = await fn(items[index]);
    }
  }
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, worker));
  return results;
}

// One unreadable DAG (e.g. a broken serialisation) must not hide every other result.
async function getOr(path, fallback, what) {
  try {
    return await getJSON(path);
  } catch (error) {
    console.warn(`[fuzzy search] could not load ${what}:`, error);
    return fallback;
  }
}

async function buildIndex(withDocs) {
  const dags = await fetchDags();
  const perDag = await mapConcurrent(dags, FETCH_CONCURRENCY, async (dag) => {
    const base = `api/v2/dags/${encodeURIComponent(dag.dag_id)}`;
    const [tasks, details] = await Promise.all([
      getOr(`${base}/tasks`, { tasks: [] }, `tasks of ${dag.dag_id}`),
      withDocs ? getOr(`${base}/details`, {}, `details of ${dag.dag_id}`) : {},
    ]);
    return { tasks: tasks.tasks, doc: details.doc_md || "" };
  });

  const items = [];
  dags.forEach((dag, index) => {
    const dagLabel = dag.dag_display_name || dag.dag_id;
    items.push({
      kind: "dag",
      key: `dag:${dag.dag_id}`,
      dagId: dag.dag_id,
      label: dagLabel,
      secondary: dagLabel === dag.dag_id ? "" : dag.dag_id,
      tags: (dag.tags || []).map((tag) => tag.name),
      paused: Boolean(dag.is_paused),
      doc: perDag[index].doc,
      href: `dags/${encodeURIComponent(dag.dag_id)}`,
    });
    for (const task of perDag[index].tasks) {
      items.push({
        kind: "task",
        key: `task:${dag.dag_id}:${task.task_id}`,
        dagId: dag.dag_id,
        dagLabel,
        label: task.task_display_name || task.task_id,
        secondary: task.operator_name || "",
        doc: withDocs ? task.doc_md || "" : "",
        href: `dags/${encodeURIComponent(dag.dag_id)}/tasks/${encodeURIComponent(task.task_id)}`,
      });
    }
  });
  return items.map(prepareItem);
}

function cacheKey(withDocs) {
  return `${CACHE_PREFIX}:docs=${withDocs ? 1 : 0}`;
}

function readCache(withDocs) {
  try {
    const cached = JSON.parse(sessionStorage.getItem(cacheKey(withDocs)) || "null");
    if (cached && Array.isArray(cached.items)) {
      return { builtAt: cached.builtAt, docs: withDocs, items: cached.items.map(prepareItem) };
    }
  } catch {
    // A corrupt cache entry is just a cache miss.
  }
  return null;
}

function writeCache(index) {
  try {
    const items = index.items.map(({ fields, docNorm, ...item }) => item);
    sessionStorage.setItem(cacheKey(index.docs), JSON.stringify({ builtAt: index.builtAt, items }));
  } catch {
    // Storage full or disabled: the in-memory index still works.
  }
}

// The user's own switch, on unless they turned it off.
function docsWanted() {
  if (!DOCS_AVAILABLE) return false;
  try {
    return localStorage.getItem(DOCS_KEY) !== "0";
  } catch {
    return true;
  }
}

function setDocsWanted(on) {
  try {
    localStorage.setItem(DOCS_KEY, on ? "1" : "0");
  } catch {
    // Not remembering is fine.
  }
}

function readRecent() {
  try {
    const recent = JSON.parse(localStorage.getItem(RECENT_KEY) || "[]");
    return Array.isArray(recent) ? recent.filter((key) => typeof key === "string") : [];
  } catch {
    return [];
  }
}

function rememberRecent(key) {
  try {
    const recent = [key, ...readRecent().filter((other) => other !== key)].slice(0, MAX_RECENT);
    localStorage.setItem(RECENT_KEY, JSON.stringify(recent));
  } catch {
    // Not remembering is fine.
  }
}

// ---------------------------------------------------------------------------------------------
// Matching
// ---------------------------------------------------------------------------------------------

// Anything but letters and digits separates words: _ - . / : spaces, punctuation.
const SEPARATOR = /[^\p{L}\p{N}]/u;

// Lower-cases and turns separators into spaces, one character for one, so match positions in the
// normalised text are positions in the original text too.
function normalise(text) {
  let out = "";
  for (let i = 0; i < text.length; i++) {
    const char = text[i];
    out += SEPARATOR.test(char) ? " " : char.toLowerCase().charAt(0) || char;
  }
  return out;
}

function field(name, text, weight) {
  return { name, text, norm: normalise(text), weight };
}

// Searchable fields per item. A task needs at least one query word in a field marked `own`,
// so typing a DAG's name lists the DAG, not every one of its tasks.
function prepareItem(item) {
  const fields =
    item.kind === "dag"
      ? [
          field("label", item.label, 1.0),
          field("secondary", item.secondary, 0.9),
          ...item.tags.map((tag, i) => field(`tag${i}`, tag, 0.6)),
        ]
      : [
          { ...field("label", item.label, 1.0), own: true },
          field("dag", item.dagLabel, 0.5),
          { ...field("secondary", item.secondary, 0.4), own: true },
        ];
  return { ...item, fields, docNorm: normalise(item.doc || "") };
}

const SCORE_CHAR = 1;
const BONUS_FIRST = 8;
const BONUS_WORD_START = 6;
const BONUS_CAMEL = 4;
const BONUS_CONSECUTIVE = 4;
const PENALTY_GAP_OPEN = 1;
const PENALTY_GAP_EXTEND = 0.3;

function charBonus(original, norm, j) {
  if (j === 0) return BONUS_FIRST;
  if (norm[j - 1] === " ") return BONUS_WORD_START;
  const prev = original[j - 1];
  const char = original[j];
  if (char !== char.toLowerCase() && prev === prev.toLowerCase()) return BONUS_CAMEL;
  if (/\d/.test(char) && !/\d/.test(prev)) return BONUS_CAMEL;
  return 0;
}

// Best alignment of `word` as a subsequence of the field, rewarding word starts and runs of
// consecutive characters, punishing gaps. Returns { score, positions } or null.
function matchWord(word, f) {
  const text = f.norm;
  const n = word.length;
  const m = text.length;
  if (n === 0 || n > m) return null;

  // Cheap rejection before the alignment.
  let k = 0;
  for (let j = 0; j < m && k < n; j++) if (text[j] === word[k]) k++;
  if (k < n) return null;

  const NONE = -Infinity;
  const scores = [];
  const parents = [];
  for (let i = 0; i < n; i++) {
    const row = new Float64Array(m).fill(NONE);
    const parent = new Int32Array(m).fill(-1);
    let run = NONE; // best predecessor at least two columns back, gap penalty already applied
    let runFrom = -1;
    for (let j = 0; j < m; j++) {
      if (i > 0 && j >= 2) {
        const candidate = scores[i - 1][j - 2] - PENALTY_GAP_OPEN;
        run -= PENALTY_GAP_EXTEND;
        if (candidate > run) {
          run = candidate;
          runFrom = j - 2;
        }
      }
      if (text[j] !== word[i]) continue;
      const here = SCORE_CHAR + charBonus(f.text, text, j);
      if (i === 0) {
        row[j] = here;
        continue;
      }
      let best = NONE;
      let from = -1;
      if (j >= 1 && scores[i - 1][j - 1] > NONE) {
        best = scores[i - 1][j - 1] + BONUS_CONSECUTIVE;
        from = j - 1;
      }
      if (run > best) {
        best = run;
        from = runFrom;
      }
      if (best > NONE) {
        row[j] = best + here;
        parent[j] = from;
      }
    }
    scores.push(row);
    parents.push(parent);
  }

  let end = -1;
  let score = NONE;
  for (let j = 0; j < m; j++) {
    if (scores[n - 1][j] > score) {
      score = scores[n - 1][j];
      end = j;
    }
  }
  if (end < 0) return null;
  const positions = new Array(n);
  for (let i = n - 1, j = end; i >= 0; i--) {
    positions[i] = j;
    j = parents[i][j];
  }
  return { score, positions };
}

function scoreItem(item, words, contextDagId) {
  let total = 0;
  let ownHit = item.kind === "dag";
  const highlights = {};
  for (const word of words) {
    let best = null;
    for (const f of item.fields) {
      const match = matchWord(word, f);
      if (!match) continue;
      const weighted = match.score * f.weight;
      if (!best || weighted > best.weighted) best = { weighted, field: f, positions: match.positions };
    }
    if (!best) return null;
    total += best.weighted;
    if (best.field.own) ownHit = true;
    const marked = (highlights[best.field.name] ||= new Set());
    for (const position of best.positions) marked.add(position);
  }
  if (!ownHit) return null;
  // A DAG beats its own tasks on an equal match; the whole label matching exactly beats all.
  if (item.kind === "dag") total += 2;
  // The DAG being looked at, and its tasks, come first among equals.
  if (contextDagId && item.dagId === contextDagId) total += 3;
  if (item.fields[0].norm === words.join(" ")) total += 20;
  return { item, score: total, highlights };
}

// In prose nearly any letters turn up somewhere in the right order, so docs match strictly: every
// query word has to start a word of the text. A whole-word occurrence is preferred and scores more.
function matchDoc(item, words) {
  const text = item.docNorm;
  if (!text) return null;
  const positions = new Set();
  let score = 0;
  let first = Infinity;
  for (const word of words) {
    let found = -1;
    let whole = false;
    for (let at = text.indexOf(word); at >= 0 && !whole; at = text.indexOf(word, at + 1)) {
      if (at > 0 && text[at - 1] !== " ") continue;
      const end = at + word.length;
      const isWhole = end === text.length || text[end] === " ";
      if (found < 0 || isWhole) {
        found = at;
        whole = isWhole;
      }
    }
    if (found < 0) return null;
    score += whole ? 2 : 1;
    first = Math.min(first, found);
    for (let i = found; i < found + word.length; i++) positions.add(i);
  }
  return { score, positions, first };
}

// Name matches first; below them, as a section of their own, items found only in their docs.
function search(items, query, contextDagId = null, includeDocs = true) {
  const words = normalise(query).split(" ").filter(Boolean);
  if (words.length === 0) return [];
  const named = [];
  const documented = [];
  for (const item of items) {
    const result = scoreItem(item, words, contextDagId);
    if (result) {
      named.push(result);
      continue;
    }
    const doc = includeDocs ? matchDoc(item, words) : null;
    if (doc) {
      let score = doc.score;
      if (item.kind === "dag") score += 0.5;
      if (contextDagId && item.dagId === contextDagId) score += 1;
      documented.push({ item, score, highlights: {}, doc, section: "doc" });
    }
  }
  const byScore = (a, b) => b.score - a.score || a.item.label.localeCompare(b.item.label);
  named.sort(byScore);
  documented.sort(byScore);
  return [...named.slice(0, MAX_RESULTS), ...documented.slice(0, MAX_DOC_RESULTS)];
}

const SNIPPET_BEFORE = 40;
const SNIPPET_LENGTH = 160;
const MARKDOWN_PUNCTUATION = new Set(["*", "#", "`", ">", "|"]);

// A stretch of the doc around its first hit, with whitespace collapsed and markdown punctuation
// dropped; `marks` are the hit positions in the returned text.
function snippet(doc, positions, first) {
  const start = Math.max(0, first - SNIPPET_BEFORE);
  const end = Math.min(doc.length, start + SNIPPET_LENGTH);
  let text = start > 0 ? "…" : "";
  const marks = new Set();
  let afterSpace = start === 0;
  for (let i = start; i < end; i++) {
    let char = doc[i];
    if (MARKDOWN_PUNCTUATION.has(char)) continue;
    if (/\s/.test(char)) {
      if (afterSpace) continue;
      char = " ";
      afterSpace = true;
    } else {
      afterSpace = false;
    }
    if (positions.has(i)) marks.add(text.length);
    text += char;
  }
  if (end < doc.length) text += "…";
  return { text, marks };
}

// ---------------------------------------------------------------------------------------------
// Navigation
// ---------------------------------------------------------------------------------------------

function currentDagId() {
  const base = new URL(document.baseURI).pathname;
  const path = window.location.pathname.startsWith(base) ? window.location.pathname.slice(base.length) : "";
  const match = /^dags\/([^/]+)/.exec(path);
  return match ? decodeURIComponent(match[1]) : null;
}

// Client-side navigation through React Router's history listener, so the UI keeps its state
// instead of reloading. The state shape mirrors what React Router writes itself, which keeps
// its back/forward bookkeeping intact.
function navigate(href, newTab) {
  const url = new URL(href, document.baseURI);
  if (newTab) {
    window.open(url.href, "_blank", "noopener");
    return;
  }
  const idx = (window.history.state && window.history.state.idx) || 0;
  const state = { usr: null, key: Math.random().toString(36).slice(2, 10), idx: idx + 1 };
  window.history.pushState(state, "", url.href);
  window.dispatchEvent(new PopStateEvent("popstate", { state }));
}

// ---------------------------------------------------------------------------------------------
// Overlay
// ---------------------------------------------------------------------------------------------

const STYLE = `
:host { all: initial; }
.backdrop {
  position: fixed; inset: 0; z-index: 2147483000;
  background: rgba(0, 0, 0, 0.45);
  display: flex; justify-content: center; align-items: flex-start;
  padding-top: 12vh;
  font-family: Inter, ui-sans-serif, system-ui, -apple-system, "Segoe UI", sans-serif;
  font-size: 14px; line-height: 1.4;
  --bg: #ffffff; --fg: #1a202c; --muted: #718096; --border: #e2e8f0;
  --active: #edf2f7; --accent: #017cee; --mark: rgba(1, 124, 238, 0.16);
  --badge-dag: #017cee; --badge-task: #805ad5;
}
.backdrop.dark {
  --bg: #1a202c; --fg: #f7fafc; --muted: #a0aec0; --border: #2d3748;
  --active: #2d3748; --accent: #63b3ed; --mark: rgba(99, 179, 237, 0.22);
  --badge-dag: #4299e1; --badge-task: #9f7aea;
}
.panel {
  width: min(720px, 92vw); max-height: 70vh;
  display: flex; flex-direction: column;
  background: var(--bg); color: var(--fg);
  border: 1px solid var(--border); border-radius: 10px;
  box-shadow: 0 20px 50px rgba(0, 0, 0, 0.3);
  overflow: hidden;
}
.searchrow { display: flex; align-items: center; gap: 8px; padding-right: 12px; border-bottom: 1px solid var(--border); }
input {
  all: unset; box-sizing: border-box; flex: 1; min-width: 0;
  padding: 14px 16px; font-size: 16px; color: var(--fg);
}
.docs {
  all: unset; flex: none; cursor: pointer;
  font-size: 12px; padding: 2px 10px; border-radius: 999px;
  border: 1px solid var(--border); color: var(--muted);
}
.docs[aria-pressed="true"] { border-color: var(--accent); color: var(--accent); background: var(--mark); }
.docs:focus-visible { outline: 2px solid var(--accent); outline-offset: 1px; }
input::placeholder { color: var(--muted); }
.status { padding: 6px 16px; font-size: 12px; color: var(--muted); border-bottom: 1px solid var(--border); }
.status.error { color: #e53e3e; }
ul { list-style: none; margin: 0; padding: 4px 0; overflow-y: auto; }
li {
  display: flex; gap: 10px; align-items: baseline;
  padding: 7px 16px; cursor: pointer;
}
li.active { background: var(--active); }
.badge {
  flex: none; width: 34px; text-align: center;
  font-size: 10px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.04em;
  border-radius: 4px; padding: 1px 0; color: #fff;
}
.badge.dag { background: var(--badge-dag); }
.badge.task { background: var(--badge-task); }
.body { min-width: 0; flex: 1; }
.label { font-weight: 500; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.meta { font-size: 12px; color: var(--muted); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.meta > span + span::before { content: " · "; }
.tag { border: 1px solid var(--border); border-radius: 4px; padding: 0 4px; margin-left: 4px; }
mark { background: var(--mark); color: inherit; border-radius: 2px; font-weight: 600; }
.empty { padding: 14px 16px; color: var(--muted); }
.section {
  padding: 10px 16px 4px; cursor: default;
  font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.04em;
  color: var(--muted); border-top: 1px solid var(--border);
}
.section:first-child { border-top: none; }
.snippet { white-space: normal; display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; }
.footer {
  display: flex; gap: 14px; padding: 6px 16px; font-size: 11px; color: var(--muted);
  border-top: 1px solid var(--border);
}
kbd { font-family: inherit; border: 1px solid var(--border); border-radius: 3px; padding: 0 4px; }
`;

// Follows the UI's language; anything without a translation is English. Airflow's i18next keeps the
// chosen language in localStorage, and sets <html lang> only when the language changes, so after a
// reload <html lang> can still say "en" under a German UI.
const MESSAGES = {
  en: {
    title: "Search DAGs and tasks",
    placeholder: "Search DAGs and tasks…",
    select: "select",
    open: "open",
    newTab: "new tab",
    close: "close",
    counts: (dags, tasks) => `${dags} DAGs, ${tasks} tasks`,
    loading: "loading DAGs and tasks…",
    refreshing: "refreshing…",
    loadFailed: (message) => `Could not load DAGs: ${message}`,
    refreshFailed: (message) => `refresh failed: ${message}`,
    recent: "recently opened",
    noMatch: "No DAG or task matches.",
    inDocs: "Found in documentation",
    docs: "Docs",
    docsTitle: "Search documentation too",
    task: "Task",
    inDag: "in ",
    paused: "paused",
  },
  de: {
    title: "DAGs und Tasks durchsuchen",
    placeholder: "DAGs und Tasks durchsuchen…",
    select: "auswählen",
    open: "öffnen",
    newTab: "neuer Tab",
    close: "schließen",
    counts: (dags, tasks) => `${dags} DAGs, ${tasks} Tasks`,
    loading: "lade DAGs und Tasks…",
    refreshing: "aktualisiere…",
    loadFailed: (message) => `DAGs konnten nicht geladen werden: ${message}`,
    refreshFailed: (message) => `Aktualisieren fehlgeschlagen: ${message}`,
    recent: "zuletzt geöffnet",
    noMatch: "Kein DAG und kein Task passt.",
    inDocs: "In der Doku gefunden",
    docs: "Doku",
    docsTitle: "Auch die Doku durchsuchen",
    task: "Task",
    inDag: "in ",
    paused: "pausiert",
  },
};

function messages() {
  let stored = null;
  try {
    stored = localStorage.getItem("i18nextLng");
  } catch {
    // No storage: fall back to the document's language.
  }
  const lang = (stored || document.documentElement.lang || "en").toLowerCase().split("-")[0];
  return MESSAGES[lang] || MESSAGES.en;
}

const IS_MAC = typeof navigator !== "undefined" && /Mac|iPhone|iPad/.test(navigator.platform || navigator.userAgent);
const MOD = IS_MAC ? "⌘" : "Ctrl";
const ALT = IS_MAC ? "⌥" : "Alt";

function element(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
}

// Text with the matched characters wrapped in <mark>, built from text nodes only.
function highlighted(text, positions) {
  const fragment = document.createDocumentFragment();
  if (!positions || positions.size === 0) {
    fragment.append(text);
    return fragment;
  }
  let i = 0;
  while (i < text.length) {
    const inMark = positions.has(i);
    let j = i;
    while (j < text.length && positions.has(j) === inMark) j++;
    const chunk = text.slice(i, j);
    fragment.append(inMark ? element("mark", "", chunk) : chunk);
    i = j;
  }
  return fragment;
}

function createOverlay() {
  const host = element("div");
  host.setAttribute("data-airflow-fuzzy-search", "");
  const root = host.attachShadow({ mode: "open" });
  root.append(element("style", "", STYLE));

  const backdrop = element("div", "backdrop");
  const panel = element("div", "panel");
  panel.setAttribute("role", "dialog");
  const input = element("input");
  input.type = "text";
  input.spellcheck = false;
  input.autocomplete = "off";
  const searchRow = element("div", "searchrow");
  searchRow.append(input);
  const docsButton = element("button", "docs");
  docsButton.type = "button";
  if (DOCS_AVAILABLE) searchRow.append(docsButton);
  const status = element("div", "status");
  const list = element("ul");
  list.setAttribute("role", "listbox");
  const footer = element("div", "footer");
  panel.append(searchRow, status, list, footer);
  backdrop.append(panel);
  root.append(backdrop);

  const state = { index: readCache(docsWanted()), loading: null, error: null, results: [], active: 0 };

  // Keys typed in the overlay must not reach the UI's own single-key shortcuts (e.g. marking a
  // task successful), which would otherwise see them once they bubble out of the shadow root.
  for (const type of ["keydown", "keyup", "keypress"]) {
    host.addEventListener(type, (event) => event.stopPropagation());
  }

  backdrop.addEventListener("mousedown", (event) => {
    if (event.target === backdrop) close();
  });

  input.addEventListener("input", () => {
    state.active = 0;
    render();
  });

  docsButton.addEventListener("click", () => {
    toggleDocs();
    input.focus();
  });

  input.addEventListener("keydown", (event) => {
    if (event.altKey && event.code === "KeyD" && DOCS_AVAILABLE) {
      event.preventDefault();
      toggleDocs();
    } else if (event.key === "Escape") {
      event.preventDefault();
      close();
    } else if (event.key === "ArrowDown" || (event.ctrlKey && event.key === "n")) {
      event.preventDefault();
      move(1);
    } else if (event.key === "ArrowUp" || (event.ctrlKey && event.key === "p")) {
      event.preventDefault();
      move(-1);
    } else if (event.key === "Enter") {
      event.preventDefault();
      open(state.active, event.metaKey || event.ctrlKey);
    }
  });

  function move(delta) {
    if (state.results.length === 0) return;
    state.active = (state.active + delta + state.results.length) % state.results.length;
    renderActive();
  }

  function open(position, newTab) {
    const result = state.results[position];
    if (!result) return;
    rememberRecent(result.item.key);
    close();
    navigate(result.item.href, newTab);
  }

  // Switching docs off just stops searching them; the index without them is built on its next
  // refresh. Switching them on needs an index that has them, from the cache or freshly loaded.
  function toggleDocs() {
    const on = !docsWanted();
    setDocsWanted(on);
    if (on && !(state.index && state.index.docs)) {
      const cached = readCache(true);
      if (cached) state.index = cached;
    }
    if (needsRefresh()) refresh();
    render();
  }

  function needsRefresh() {
    if (!state.index) return true;
    if (docsWanted() && !state.index.docs) return true;
    return Date.now() - state.index.builtAt > CACHE_FRESH_MS;
  }

  function refresh() {
    if (state.loading) return state.loading;
    const withDocs = docsWanted();
    state.loading = buildIndex(withDocs)
      .then((items) => {
        const index = { builtAt: Date.now(), docs: withDocs, items };
        writeCache(index);
        // Turned on while an index without docs was loading: keep the one we have, load again below.
        if (!withDocs && docsWanted() && state.index && state.index.docs) return;
        state.index = index;
        state.error = null;
      })
      .catch((error) => {
        console.error("[fuzzy search] loading DAGs failed:", error);
        state.error = error;
      })
      .finally(() => {
        state.loading = null;
        if (withDocs !== docsWanted() && docsWanted()) refresh();
        if (host.isConnected) render();
      });
    return state.loading;
  }

  function currentResults() {
    const items = state.index ? state.index.items : [];
    const query = input.value.trim();
    if (query) return search(items, query, currentDagId(), docsWanted());
    const byKey = new Map(items.map((item) => [item.key, item]));
    return readRecent()
      .map((key) => byKey.get(key))
      .filter(Boolean)
      .map((item) => ({ item, score: 0, highlights: {} }));
  }

  function renderStaticTexts(t) {
    panel.setAttribute("aria-label", t.title);
    input.setAttribute("aria-label", t.title);
    input.placeholder = t.placeholder;
    footer.replaceChildren(
      ...[
        ["↑ ↓", t.select],
        ["Enter", t.open],
        [`${MOD} Enter`, t.newTab],
        ["Esc", t.close],
        ...(DOCS_AVAILABLE ? [[`${ALT} D`, t.docs]] : []),
      ].map(([keys, what]) => {
        const hint = element("span");
        hint.append(element("kbd", "", keys), ` ${what}`);
        return hint;
      }),
    );
  }

  function renderDocsButton(t) {
    docsButton.textContent = t.docs;
    docsButton.title = `${t.docsTitle} (${ALT} D)`;
    docsButton.setAttribute("aria-pressed", String(docsWanted()));
  }

  function renderStatus(t) {
    status.classList.toggle("error", Boolean(state.error));
    if (state.error && !state.index) {
      status.textContent = t.loadFailed(state.error.message);
      return;
    }
    const items = state.index ? state.index.items : [];
    const dags = items.filter((item) => item.kind === "dag").length;
    const parts = [];
    if (state.index) parts.push(t.counts(dags, items.length - dags));
    if (state.loading) parts.push(state.index ? t.refreshing : t.loading);
    if (state.error) parts.push(t.refreshFailed(state.error.message));
    if (!input.value.trim() && state.results.length) parts.push(t.recent);
    status.textContent = parts.join(" · ");
  }

  function renderRow(t, result, position) {
    const { item, highlights } = result;
    const row = element("li");
    row.setAttribute("role", "option");
    row.addEventListener("mousemove", () => {
      if (state.active !== position) {
        state.active = position;
        renderActive();
      }
    });
    row.addEventListener("click", (event) => open(position, event.metaKey || event.ctrlKey));

    row.append(element("span", `badge ${item.kind}`, item.kind === "dag" ? "DAG" : t.task));
    const body = element("div", "body");
    const label = element("div", "label");
    label.append(highlighted(item.label, highlights.label));
    const meta = element("div", "meta");
    if (item.kind === "dag") {
      if (item.secondary) {
        const id = element("span");
        id.append(highlighted(item.secondary, highlights.secondary));
        meta.append(id);
      }
      if (item.paused) meta.append(element("span", "", t.paused));
      if (item.tags.length) {
        const tags = element("span");
        item.tags.forEach((tag, i) => {
          const chip = element("span", "tag");
          chip.append(highlighted(tag, highlights[`tag${i}`]));
          tags.append(chip);
        });
        meta.append(tags);
      }
    } else {
      const dag = element("span");
      dag.append(t.inDag, highlighted(item.dagLabel, highlights.dag));
      meta.append(dag);
      if (item.secondary) {
        const operator = element("span");
        operator.append(highlighted(item.secondary, highlights.secondary));
        meta.append(operator);
      }
    }
    body.append(label);
    if (meta.childNodes.length) body.append(meta);
    if (result.section === "doc") {
      const excerpt = snippet(item.doc, result.doc.positions, result.doc.first);
      const doc = element("div", "meta snippet");
      doc.append(highlighted(excerpt.text, excerpt.marks));
      body.append(doc);
    }
    row.append(body);
    return row;
  }

  function renderActive() {
    list.querySelectorAll("li[role=option]").forEach((row, position) => {
      const active = position === state.active;
      row.classList.toggle("active", active);
      row.setAttribute("aria-selected", String(active));
      if (active) row.scrollIntoView({ block: "nearest" });
    });
  }

  function render() {
    state.results = currentResults();
    if (state.active >= state.results.length) state.active = 0;
    const t = messages();
    renderDocsButton(t);
    renderStatus(t);
    const rows = state.results.map((result, position) => renderRow(t, result, position));
    const firstDoc = state.results.findIndex((result) => result.section === "doc");
    if (firstDoc >= 0) rows.splice(firstDoc, 0, element("li", "section", t.inDocs));
    list.replaceChildren(...rows);
    if (state.results.length === 0 && state.index && input.value.trim()) {
      list.append(element("li", "empty", t.noMatch));
    }
    renderActive();
  }

  function show() {
    backdrop.classList.toggle("dark", document.documentElement.classList.contains("dark"));
    renderStaticTexts(messages());
    document.body.append(host);
    input.value = "";
    state.active = 0;
    if (needsRefresh()) refresh();
    render();
    input.focus();
  }

  function close() {
    host.remove();
  }

  return {
    toggle() {
      if (host.isConnected) close();
      else show();
    },
  };
}

// ---------------------------------------------------------------------------------------------
// Wiring
// ---------------------------------------------------------------------------------------------

function install() {
  const overlay = createOverlay();
  // Capture phase on window runs before the UI's hotkey library, which listens on document.
  window.addEventListener(
    "keydown",
    (event) => {
      if (!(event.metaKey || event.ctrlKey) || event.altKey || event.shiftKey) return;
      if (event.key.toLowerCase() !== "k") return;
      event.preventDefault();
      event.stopImmediatePropagation();
      overlay.toggle();
    },
    true,
  );
  return overlay;
}

if (typeof window !== "undefined" && !globalThis[GLOBAL_KEY]) {
  globalThis[GLOBAL_KEY] = install();
}

// The UI mounts this as a component in its base layout; the overlay lives outside React.
globalThis.AirflowPlugin = function FuzzySearchPlugin() {
  return null;
};

// Exported for tests.
export { normalise, matchWord, search, prepareItem, snippet };
