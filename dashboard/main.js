// PocketBase admin UI extension: adds a "Replication" tab to the admin
// sidebar that opens the cluster dashboard inside the admin UI.
//
// This script is served through PocketBase's experimental UI extension
// API (/_/extensions.js) and runs inside the admin SPA document, where
// window.app.pb (JS client with the live superuser auth) and
// window.app.store.headerLinks (the reactive sidebar nav model) are
// available. Everything is defensive: if the SPA internals change, it
// falls back to a simple floating link, and the standalone dashboard at
// /api/replication/dashboard keeps working regardless.

var DASH_PATH = "/api/replication/dashboard";

function dashURL() {
  try {
    if (window.app && app.pb && app.pb.buildURL) return app.pb.buildURL(DASH_PATH);
  } catch (_) {}
  return DASH_PATH;
}

function currentToken() {
  try {
    if (window.app && app.pb && app.pb.authStore && app.pb.authStore.token) {
      return app.pb.authStore.token;
    }
  } catch (_) {}
  return "";
}

// ---------------------------------------------------------------------
// overlay with the embedded dashboard

var overlay = null;

function closeOverlay() {
  if (overlay) {
    overlay.remove();
    overlay = null;
    document.removeEventListener("keydown", escHandler);
  }
}

function escHandler(ev) {
  if (ev.key === "Escape") closeOverlay();
}

function openOverlay() {
  if (overlay) return;

  overlay = document.createElement("div");
  overlay.id = "pbr-overlay";
  overlay.style.cssText =
    "position:fixed;inset:0;z-index:99998;background:rgba(20,24,31,.55);" +
    "display:flex;align-items:stretch;justify-content:center;padding:28px;";

  var panel = document.createElement("div");
  panel.style.cssText =
    "position:relative;flex:1;max-width:1200px;background:#fff;" +
    "border-radius:10px;overflow:hidden;box-shadow:0 8px 40px rgba(0,0,0,.35);";

  var close = document.createElement("button");
  close.type = "button";
  close.textContent = "✕";
  close.title = "Close (Esc)";
  close.style.cssText =
    "position:absolute;top:10px;right:12px;z-index:2;width:32px;height:32px;" +
    "border:none;border-radius:99px;background:rgba(28,36,48,.08);" +
    "font:16px system-ui,sans-serif;cursor:pointer;color:#1c2430;";
  close.onclick = closeOverlay;

  var frame = document.createElement("iframe");
  frame.src = dashURL();
  frame.style.cssText = "width:100%;height:100%;border:0;display:block;";
  frame.addEventListener("load", function () {
    postToken(frame);
  });

  panel.appendChild(close);
  panel.appendChild(frame);
  overlay.appendChild(panel);
  overlay.addEventListener("click", function (ev) {
    if (ev.target === overlay) closeOverlay();
  });
  document.body.appendChild(overlay);
  document.addEventListener("keydown", escHandler);
}

function postToken(frame) {
  var token = currentToken();
  if (!token || !frame.contentWindow) return;
  try {
    frame.contentWindow.postMessage({ pbrToken: token }, window.location.origin);
  } catch (_) {}
}

// the embedded page asks for the token on startup; answer with the live one
window.addEventListener("message", function (ev) {
  if (ev.origin !== window.location.origin) return;
  if (!ev.data || !ev.data.pbrTokenRequest) return;
  var token = currentToken();
  if (token && ev.source) {
    try { ev.source.postMessage({ pbrToken: token }, window.location.origin); } catch (_) {}
  }
});

// intercept clicks on our nav link so it opens the overlay instead of a tab
document.addEventListener(
  "click",
  function (ev) {
    var a = ev.target && ev.target.closest && ev.target.closest("a");
    if (!a) return;
    var href = a.getAttribute("href") || "";
    if (href.indexOf(DASH_PATH) === -1) return;
    ev.preventDefault();
    ev.stopPropagation();
    openOverlay();
  },
  true
);

// ---------------------------------------------------------------------
// register the sidebar tab (or fall back to a floating link)

function addFloatingLink() {
  if (document.getElementById("pbr-dash-link")) return;
  var link = document.createElement("a");
  link.id = "pbr-dash-link";
  link.href = dashURL();
  link.textContent = "⇄ Replication";
  link.style.cssText =
    "position:fixed;right:14px;bottom:14px;z-index:99999;" +
    "background:#4f6ef7;color:#fff;text-decoration:none;" +
    "padding:8px 14px;border-radius:99px;font:13px system-ui,sans-serif;" +
    "box-shadow:0 2px 10px rgba(0,0,0,.25);opacity:.92;";
  document.body.appendChild(link);
}

function addNavTab() {
  var links = window.app && app.store && app.store.headerLinks;
  if (!Array.isArray(links)) return false;
  if (links.some(function (l) { return l && l.href && l.href.indexOf(DASH_PATH) !== -1; })) {
    return true;
  }
  // reassign (instead of push) so the reactive store re-renders the nav
  app.store.headerLinks = links.concat([{
    href: "../api/replication/dashboard",
    icon: "ri-arrow-left-right-line",
    label: "Replication",
  }]);
  return true;
}

(function init(attempt) {
  if (addNavTab()) return;
  if (attempt >= 50) { // ~10s: SPA internals changed, degrade gracefully
    addFloatingLink();
    return;
  }
  setTimeout(function () { init(attempt + 1); }, 200);
})(0);
