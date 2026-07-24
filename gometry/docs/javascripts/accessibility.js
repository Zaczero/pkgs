function labelProgressBars() {
  for (const progress of document.querySelectorAll(
    '.md-progress[role="progressbar"]:not([aria-label])',
  )) {
    progress.setAttribute("aria-label", "Loading page")
  }
}

labelProgressBars()
document.addEventListener("DOMContentLoaded", labelProgressBars)

if (window.document$) {
  window.document$.subscribe(labelProgressBars)
}

new MutationObserver(labelProgressBars).observe(document.documentElement, {
  childList: true,
  subtree: true,
})
