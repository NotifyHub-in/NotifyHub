const toggle = document.querySelector("[data-mobile-toggle]");
const nav = document.querySelector("[data-nav]");

if (toggle && nav) {
  toggle.addEventListener("click", () => {
    const isOpen = nav.classList.toggle("is-open");
    toggle.setAttribute("aria-expanded", String(isOpen));
  });

  nav.querySelectorAll("a").forEach((link) => {
    link.addEventListener("click", () => {
      if (nav.classList.contains("is-open")) {
        nav.classList.remove("is-open");
        toggle.setAttribute("aria-expanded", "false");
      }
    });
  });
}

const currentPath = window.location.pathname.replace(/\/+$/, "/");
document.querySelectorAll(".nav a").forEach((link) => {
  try {
    const url = new URL(link.getAttribute("href"), window.location.href);
    const linkPath = url.pathname.replace(/\/+$/, url.pathname.endsWith("/") ? "/" : "");
    if (linkPath === currentPath || (currentPath.endsWith("/") && linkPath === currentPath)) {
      link.setAttribute("aria-current", "page");
    }
  } catch {
    // Ignore malformed links.
  }
});
