(() => {
  "use strict";

  const toolbar = document.getElementById("snapshot-toolbar");
  const disclosure = document.getElementById("snapshot-disclosure");
  const frame = document.querySelector(".snapshot-frame");
  if (!(toolbar instanceof HTMLElement) || !(disclosure instanceof HTMLDetailsElement)) {
    return;
  }

  const compactViewport = window.matchMedia(
    "(max-width: 560px), (max-width: 960px) and (max-height: 560px)",
  );
  let boundDocument = null;
  let lastScrollY = 0;
  let ignoreCollapseUntil = 0;

  function syncDisclosureState() {
    toolbar.classList.toggle("is-collapsed", !disclosure.open);
  }

  disclosure.addEventListener("toggle", () => {
    syncDisclosureState();
    if (disclosure.open) {
      ignoreCollapseUntil = performance.now() + 450;
      try {
        lastScrollY = frame?.contentWindow?.scrollY || 0;
      } catch {
        lastScrollY = 0;
      }
    }
  });
  syncDisclosureState();

  function bindNotebookScroll() {
    if (!(frame instanceof HTMLIFrameElement)) {
      return;
    }
    try {
      const frameDocument = frame.contentDocument;
      const frameWindow = frame.contentWindow;
      if (!frameDocument || !frameWindow || frameDocument === boundDocument) {
        return;
      }
      boundDocument = frameDocument;
      lastScrollY = frameWindow.scrollY;
      frameWindow.addEventListener(
        "scroll",
        () => {
          const scrollY = frameWindow.scrollY;
          const movedDown = scrollY - lastScrollY > 3;
          if (
            compactViewport.matches &&
            disclosure.open &&
            scrollY > 40 &&
            movedDown &&
            performance.now() >= ignoreCollapseUntil
          ) {
            disclosure.open = false;
          }
          lastScrollY = scrollY;
        },
        { passive: true },
      );
    } catch {
      // The hardened notebook frame remains usable if origin access is unavailable.
    }
  }

  if (frame instanceof HTMLIFrameElement) {
    frame.addEventListener("load", bindNotebookScroll);
    bindNotebookScroll();
  }

  compactViewport.addEventListener("change", (event) => {
    if (!event.matches) {
      disclosure.open = true;
    }
  });
})();
