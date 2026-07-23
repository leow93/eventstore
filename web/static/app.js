"use strict";

// Eventstore admin console. A tiny hash-routed SPA over the read-only JSON API:
//   #/streams                     browse streams
//   #/categories                  browse categories
//   #/stream/<name>               read a whole stream
//   #/category/<name>             infinite-scroll a category
const main = document.getElementById("main");

// --- API ----------------------------------------------------------------
async function api(path) {
  const res = await fetch(path, { headers: { Accept: "application/json" } });
  const body = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(body.error || `${res.status} ${res.statusText}`);
  return body;
}

// --- helpers ------------------------------------------------------------
const el = (tag, attrs = {}, ...children) => {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (k === "class") node.className = v;
    else if (k === "html") node.innerHTML = v;
    else if (k.startsWith("on") && typeof v === "function") node.addEventListener(k.slice(2), v);
    else if (v !== null && v !== undefined) node.setAttribute(k, v);
  }
  for (const c of children.flat()) {
    if (c === null || c === undefined) continue;
    node.append(c.nodeType ? c : document.createTextNode(String(c)));
  }
  return node;
};

const fmtNum = (n) => Number(n).toLocaleString();

function fmtTimestamp(ts) {
  const n = Number(ts);
  if (!n) return "—";
  // The store stamps events with UnixNano, but tolerate other units by
  // magnitude so externally-written timestamps still render sensibly.
  let ms;
  if (n >= 1e18) ms = n / 1e6;      // nanoseconds
  else if (n >= 1e15) ms = n / 1e3; // microseconds
  else if (n >= 1e12) ms = n;       // milliseconds
  else ms = n * 1000;               // seconds
  const d = new Date(ms);
  if (isNaN(d.getTime())) return String(ts);
  return d.toISOString().replace("T", " ").replace("Z", " UTC");
}

function setError(msg) {
  main.prepend(el("div", { class: "error" }, msg));
}

// Syntax-highlight a JSON value into a <pre>.
function jsonBlock(value) {
  const text = JSON.stringify(value, null, 2);
  const escaped = text
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
  const html = escaped.replace(
    /("(\\.|[^"\\])*"(\s*:)?|\b(true|false|null)\b|-?\d+(\.\d+)?([eE][+-]?\d+)?)/g,
    (m) => {
      let cls = "json-number";
      if (/^"/.test(m)) cls = /:$/.test(m) ? "json-key" : "json-string";
      else if (/true|false/.test(m)) cls = "json-bool";
      else if (/null/.test(m)) cls = "json-null";
      return `<span class="${cls}">${m}</span>`;
    }
  );
  return el("pre", { class: "json", html });
}

// --- navigation ---------------------------------------------------------
function go(hash) {
  window.location.hash = hash;
}

function setActiveNav(view) {
  document.querySelectorAll(".navbtn").forEach((b) =>
    b.classList.toggle("active", b.dataset.view === view)
  );
}

async function refreshStats() {
  try {
    const s = await api("/api/stats");
    document.getElementById("stat-streams").textContent = fmtNum(s.streams);
    document.getElementById("stat-categories").textContent = fmtNum(s.categories);
    document.getElementById("stat-events").textContent = fmtNum(s.events);
  } catch {
    /* stats are best-effort */
  }
}

// --- event table (shared by stream & category views) --------------------
// Builds a table body that appends rows; each event row can expand to show
// payload and meta. Returns { table, append(events) }.
function eventTable() {
  const tbody = el("tbody");
  const table = el(
    "table",
    {},
    el(
      "thead",
      {},
      el(
        "tr",
        {},
        el("th", { style: "width:28px" }),
        el("th", { class: "num", style: "width:90px" }, "Global"),
        el("th", { style: "width:80px" }, "Pos"),
        el("th", {}, "Stream"),
        el("th", {}, "Type"),
        el("th", { style: "width:210px" }, "Timestamp")
      )
    ),
    tbody
  );

  function append(events) {
    for (const ev of events) {
      const detailRow = el(
        "tr",
        { class: "ev-detail", style: "display:none" },
        el(
          "td",
          { colspan: "6" },
          el(
            "div",
            { class: "ev-detail-inner" },
            el("div", {}, el("div", { class: "kv-label" }, "Payload"), jsonBlock(ev.payload)),
            el("div", {}, el("div", { class: "kv-label" }, "Meta"), jsonBlock(ev.meta))
          )
        )
      );

      const toggle = el("span", { class: "ev-toggle" }, "▸");
      const row = el(
        "tr",
        {
          class: "clickable",
          onclick: () => {
            const open = detailRow.style.display === "none";
            detailRow.style.display = open ? "table-row" : "none";
            toggle.textContent = open ? "▾" : "▸";
          },
        },
        el("td", {}, toggle),
        el("td", { class: "num mono" }, fmtNum(ev.globalPosition)),
        el("td", { class: "num mono" }, fmtNum(ev.position)),
        el(
          "td",
          { class: "mono" },
          el("a", { class: "crumbs", style: "color:var(--accent)", onclick: (e) => { e.stopPropagation(); go(`#/stream/${encodeURIComponent(ev.streamName)}`); } }, ev.streamName)
        ),
        el("td", {}, el("span", { class: "type-badge" }, ev.eventType || "—")),
        el("td", { class: "mono", style: "color:var(--muted)" }, fmtTimestamp(ev.timestamp))
      );

      tbody.append(row, detailRow);
    }
  }

  return { table, append, count: () => tbody.querySelectorAll("tr.clickable").length };
}

// --- views --------------------------------------------------------------
async function viewStreams() {
  setActiveNav("streams");
  const params = new URLSearchParams(window.location.hash.split("?")[1] || "");
  const prefix = params.get("prefix") || "";

  main.replaceChildren(
    el("div", { class: "page-head" }, el("h1", {}, "Streams")),
    el("div", { class: "loading-bar" })
  );

  const search = el("input", {
    type: "search",
    placeholder: "Filter by stream prefix… (e.g. user-)",
    value: prefix,
  });
  let debounce;
  search.addEventListener("input", () => {
    clearTimeout(debounce);
    debounce = setTimeout(() => {
      const q = search.value.trim();
      go(q ? `#/streams?prefix=${encodeURIComponent(q)}` : "#/streams");
    }, 200);
  });

  let data;
  try {
    data = await api(`/api/streams?limit=1000&prefix=${encodeURIComponent(prefix)}`);
  } catch (e) {
    main.replaceChildren(el("div", { class: "page-head" }, el("h1", {}, "Streams")));
    setError(e.message);
    return;
  }

  const rows = data.streams.map((s) =>
    el(
      "tr",
      { class: "clickable", onclick: () => go(`#/stream/${encodeURIComponent(s.name)}`) },
      el("td", { class: "mono" }, s.name),
      el("td", {}, el("span", { class: "pill", onclick: (e) => { e.stopPropagation(); go(`#/category/${encodeURIComponent(s.category)}`); }, style: "cursor:pointer" }, s.category)),
      el("td", { class: "num mono" }, fmtNum(s.count))
    )
  );

  main.replaceChildren(
    el("div", { class: "page-head" }, el("h1", {}, "Streams")),
    el("div", { class: "toolbar" }, search, el("span", { class: "spacer" }), el("span", { class: "count-note" }, `${fmtNum(data.total)} stream${data.total === 1 ? "" : "s"}`)),
    rows.length
      ? el(
          "table",
          {},
          el("thead", {}, el("tr", {}, el("th", {}, "Stream"), el("th", { style: "width:180px" }, "Category"), el("th", { class: "num", style: "width:120px" }, "Events"))),
          el("tbody", {}, ...rows)
        )
      : el("div", { class: "empty" }, prefix ? `No streams matching “${prefix}”.` : "No streams yet.")
  );
  search.focus();
  search.setSelectionRange(prefix.length, prefix.length);
}

async function viewCategories() {
  setActiveNav("categories");
  main.replaceChildren(el("div", { class: "page-head" }, el("h1", {}, "Categories")), el("div", { class: "loading-bar" }));

  let data;
  try {
    data = await api("/api/categories");
  } catch (e) {
    main.replaceChildren(el("div", { class: "page-head" }, el("h1", {}, "Categories")));
    setError(e.message);
    return;
  }

  const rows = data.categories.map((c) =>
    el(
      "tr",
      { class: "clickable", onclick: () => go(`#/category/${encodeURIComponent(c.name)}`) },
      el("td", {}, el("span", { class: "pill" }, c.name)),
      el("td", { class: "num mono" }, fmtNum(c.streamCount)),
      el("td", { class: "num mono" }, fmtNum(c.eventCount))
    )
  );

  main.replaceChildren(
    el("div", { class: "page-head" }, el("h1", {}, "Categories")),
    el("div", { class: "toolbar" }, el("span", { class: "spacer" }), el("span", { class: "count-note" }, `${fmtNum(data.categories.length)} categor${data.categories.length === 1 ? "y" : "ies"}`)),
    rows.length
      ? el(
          "table",
          {},
          el("thead", {}, el("tr", {}, el("th", {}, "Category"), el("th", { class: "num", style: "width:120px" }, "Streams"), el("th", { class: "num", style: "width:120px" }, "Events"))),
          el("tbody", {}, ...rows)
        )
      : el("div", { class: "empty" }, "No categories yet.")
  );
}

async function viewStream(name) {
  setActiveNav("streams");
  let direction = "forwards";

  const head = el(
    "div",
    { class: "page-head" },
    el("div", { class: "crumbs" }, el("a", { onclick: () => go("#/streams") }, "Streams"), " / ", name),
    el("h1", {}, el("span", { class: "mono" }, name))
  );
  const body = el("div", {});
  main.replaceChildren(head, el("div", { class: "loading-bar", id: "lb" }), body);

  async function load() {
    body.replaceChildren();
    const lb = el("div", { class: "loading-bar" });
    body.append(lb);
    let data;
    try {
      data = await api(`/api/stream?name=${encodeURIComponent(name)}&direction=${direction}&limit=1000`);
    } catch (e) {
      body.replaceChildren();
      setError(e.message);
      return;
    }
    const et = eventTable();
    et.append(data.events);

    const dirSelect = el(
      "select",
      { onchange: (e) => { direction = e.target.value; load(); } },
      el("option", { value: "forwards", ...(direction === "forwards" ? { selected: "" } : {}) }, "Oldest first"),
      el("option", { value: "backwards", ...(direction === "backwards" ? { selected: "" } : {}) }, "Newest first")
    );

    body.replaceChildren(
      el(
        "div",
        { class: "toolbar" },
        el("span", { class: "count-note" }, `Category `),
        el("a", { class: "pill", style: "cursor:pointer", onclick: () => go(`#/category/${encodeURIComponent(data.category)}`) }, data.category),
        el("span", { class: "spacer" }),
        dirSelect,
        el("span", { class: "count-note" }, `version ${fmtNum(data.version)}`)
      ),
      data.events.length ? et.table : el("div", { class: "empty" }, "This stream has no events.")
    );
    if (data.version > data.events.length) {
      body.append(el("div", { class: "sentinel" }, `Showing first ${fmtNum(data.events.length)} of ${fmtNum(data.version)} events.`));
    }
  }
  load();
}

async function viewCategory(name) {
  setActiveNav("categories");
  const PAGE = 50;

  const head = el(
    "div",
    { class: "page-head" },
    el("div", { class: "crumbs" }, el("a", { onclick: () => go("#/categories") }, "Categories"), " / ", name),
    el("h1", {}, el("span", { class: "pill" }, name)),
    el("div", { class: "subtitle", id: "cat-sub" })
  );

  const et = eventTable();
  const sentinel = el("div", { class: "sentinel" }, "Loading…");
  main.replaceChildren(head, et.table, sentinel);

  let nextFrom = 1;
  let done = false;
  let loading = false;
  let total = 0;

  async function loadMore() {
    if (loading || done) return;
    loading = true;
    sentinel.textContent = "Loading…";
    try {
      const data = await api(`/api/category?name=${encodeURIComponent(name)}&from=${nextFrom}&limit=${PAGE}`);
      total = data.total;
      et.append(data.events);
      document.getElementById("cat-sub").textContent =
        `${fmtNum(et.count())} of ${fmtNum(total)} events loaded`;
      if (data.next) {
        nextFrom = data.next;
        sentinel.textContent = "Scroll for more…";
      } else {
        done = true;
        sentinel.textContent = et.count() ? `End of category — ${fmtNum(total)} events.` : "This category has no events.";
      }
    } catch (e) {
      sentinel.textContent = "";
      setError(e.message);
      done = true;
    } finally {
      loading = false;
    }
  }

  // Fetch the next page whenever the sentinel scrolls into view.
  const observer = new IntersectionObserver(
    (entries) => {
      if (entries[0].isIntersecting) loadMore();
    },
    { root: main, rootMargin: "200px" }
  );
  observer.observe(sentinel);

  // Kick off the first page (also covers the case where the page is tall enough
  // that the sentinel is already visible).
  await loadMore();
}

// --- router -------------------------------------------------------------
function router() {
  const raw = window.location.hash.slice(1) || "/streams";
  const [path] = raw.split("?");
  const parts = path.split("/").filter(Boolean); // e.g. ["stream", "user-1"]

  main.scrollTop = 0;
  refreshStats();

  if (parts[0] === "stream" && parts[1]) return viewStream(decodeURIComponent(parts.slice(1).join("/")));
  if (parts[0] === "category" && parts[1]) return viewCategory(decodeURIComponent(parts.slice(1).join("/")));
  if (parts[0] === "categories") return viewCategories();
  return viewStreams();
}

document.querySelectorAll(".navbtn").forEach((b) =>
  b.addEventListener("click", () => go(`#/${b.dataset.view}`))
);
document.querySelector(".brand").addEventListener("click", () => go("#/streams"));
window.addEventListener("hashchange", router);
window.addEventListener("DOMContentLoaded", router);
router();
