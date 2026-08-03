/* Click a mermaid diagram to open it as a full-viewport plate.
 *
 * A wide LR diagram is downscaled by `max-width: 100%` to fit the text column,
 * so its labels land well below body size. This lifts the diagram out of the
 * flow and scales it up to the viewport.
 *
 * Material renders each diagram into a CLOSED shadow root, which shapes the
 * whole implementation:
 *
 *   - the SVG cannot be read, styled, or cloned from here, so the host <div>
 *     is MOVED into the plate and moved back on close (cloneNode would return
 *     an empty div — a shadow root does not clone);
 *   - the SVG's own max-width caps its layout size, so widening the host does
 *     nothing: zooming the host is the only lever that enlarges it;
 *   - a click inside the shadow root is retargeted to the host, so one
 *     delegated listener on the document sees every diagram, whenever mermaid
 *     finishes rendering it.
 */

(function () {
  "use strict";

  var MARGIN = 0.92;    /* leave a hair of the page visible around the plate */
  var MAX_ZOOM = 3;     /* past this, a small diagram just looks coarse */
  var MIN_ZOOM = 1.8;   /* below this, opening the plate wasn't worth the click */

  var plate = null;
  var gap = null;
  var host = null;
  var box = null;   /* the diagram's size as laid out in the text column */

  /* Zoom relative to the in-column size, which is why `box` is captured before
     the move and the host's width is pinned to it: inside the plate the host
     has no definite width of its own and collapses (the SVG it holds is
     `width: 100%`), and a collapsed box measures far narrower than the diagram
     really is, capping the enlargement well short of the viewport. */
  function scaleToFit() {
    if (!host || !box.width || !box.height) return;
    var byHeight = (window.innerHeight * MARGIN) / box.height;
    var fit = Math.min((window.innerWidth * MARGIN) / box.width, byHeight, MAX_ZOOM);
    /* A wide diagram on a narrow screen can't be both whole and legible. Prefer
       legible: enlarge past the viewport and let the plate pan. */
    if (fit < MIN_ZOOM) fit = Math.min(MIN_ZOOM, byHeight, MAX_ZOOM);
    host.style.setProperty("--ms-plate-zoom", Math.max(fit, 1).toFixed(3));
  }

  function open(el) {
    if (plate) return;
    host = el;
    box = el.getBoundingClientRect();

    /* hold the diagram's place so the prose behind the plate doesn't reflow */
    gap = document.createElement("div");
    gap.className = "ms-plate-gap";
    gap.style.height = box.height + "px";
    el.replaceWith(gap);
    el.style.width = box.width + "px";

    plate = document.createElement("div");
    plate.className = "ms-plate";
    plate.appendChild(el);
    document.body.appendChild(plate);

    scaleToFit();
    el.focus({ preventScroll: true });
  }

  function close() {
    if (!plate) return;
    host.style.removeProperty("--ms-plate-zoom");
    host.style.removeProperty("width");
    gap.replaceWith(host);
    plate.remove();
    host.focus({ preventScroll: true });
    plate = gap = host = box = null;
  }

  /* Delegated, so diagrams that mermaid renders later are covered too. */
  document.addEventListener("click", function (event) {
    if (plate) {
      close();
      return;
    }
    var el = event.target.closest ? event.target.closest(".mermaid") : null;
    if (el && el.parentElement) open(el);
  });

  document.addEventListener("keydown", function (event) {
    if (event.key === "Escape" && plate) {
      close();
    } else if ((event.key === "Enter" || event.key === " ") && !plate) {
      var el = document.activeElement;
      if (el && el.classList && el.classList.contains("mermaid")) {
        event.preventDefault();
        open(el);
      }
    }
  });

  window.addEventListener("resize", scaleToFit);

  /* Reachable by keyboard, and announced as something to activate. Mermaid
     renders asynchronously, so watch for hosts appearing instead of running
     once at load. */
  function annotate(root) {
    var nodes = root.querySelectorAll ? root.querySelectorAll(".mermaid") : [];
    for (var i = 0; i < nodes.length; i++) {
      var el = nodes[i];
      if (el.hasAttribute("tabindex")) continue;
      el.setAttribute("tabindex", "0");
      el.setAttribute("role", "button");
      el.setAttribute("aria-label", "Diagram — activate to enlarge");
    }
  }

  new MutationObserver(function (records) {
    for (var i = 0; i < records.length; i++) {
      for (var j = 0; j < records[i].addedNodes.length; j++) {
        var node = records[i].addedNodes[j];
        if (node.nodeType === 1) annotate(node.parentNode || node);
      }
    }
  }).observe(document.body, { childList: true, subtree: true });

  annotate(document);
})();
