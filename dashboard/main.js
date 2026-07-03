// Experimental PocketBase admin UI extension (opt-in via
// Config.EnableUIExtension). Adds a small floating link to the
// replication dashboard when browsing the superuser UI.
//
// PocketBase's UI extension API is experimental and undocumented; this
// script intentionally does the bare, defensive minimum. The standalone
// dashboard at /api/replication/dashboard always works without it.
(function () {
  if (document.getElementById("pbr-dash-link")) return;

  var link = document.createElement("a");
  link.id = "pbr-dash-link";
  link.href = "/api/replication/dashboard";
  link.target = "_blank";
  link.textContent = "⇄ Replication";
  link.style.cssText =
    "position:fixed;right:14px;bottom:14px;z-index:99999;" +
    "background:#4f6ef7;color:#fff;text-decoration:none;" +
    "padding:8px 14px;border-radius:99px;font:13px system-ui,sans-serif;" +
    "box-shadow:0 2px 10px rgba(0,0,0,.25);opacity:.92";
  link.onmouseenter = function () { link.style.opacity = "1"; };
  link.onmouseleave = function () { link.style.opacity = ".92"; };

  document.addEventListener("DOMContentLoaded", function () {
    document.body.appendChild(link);
  });
  if (document.readyState !== "loading") {
    document.body.appendChild(link);
  }
})();
