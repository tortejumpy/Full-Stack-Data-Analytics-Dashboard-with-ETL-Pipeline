/**
 * main.js — Application bootstrap, section routing, and shared UI utilities.
 *
 * Exports: showLoading, hideLoading, showError — imported by all components.
 */
import { initRevenue } from "./components/revenue.js";
import { initCustomers } from "./components/customers.js";
import { initCategories } from "./components/categories.js";
import { initRegions } from "./components/regions.js";
import { api } from "./api.js";

// ── Shared UI helpers (exported for components) ───────────────────────────

export function showLoading(id) {
  const el = document.getElementById(id);
  if (el) el.style.display = "flex";
}

export function hideLoading(id) {
  const el = document.getElementById(id);
  if (el) el.style.display = "none";
}

export function showError(sectionId, message) {
  const section = document.getElementById(sectionId);
  const toast = document.createElement("div");
  toast.className = "error-toast";
  toast.innerHTML = `<span>⚠️ ${message}</span><button onclick="this.parentElement.remove()">✕</button>`;
  section.prepend(toast);
  setTimeout(() => toast.remove(), 6000);
}

// ── Section routing ───────────────────────────────────────────────────────

const SECTIONS = [
  { id: "revenue", label: " Revenue", icon: "", init: initRevenue },
  { id: "customers", label: " Customers", icon: "", init: initCustomers },
  { id: "categories", label: " Categories", icon: "", init: initCategories },
  { id: "regions", label: " Regions", icon: "", init: initRegions },
];

let activeSection = null;
const initialized = new Set();

function activateSection(id) {
  SECTIONS.forEach(s => {
    const el = document.getElementById(`${s.id}-section`);
    const nav = document.getElementById(`nav-${s.id}`);
    if (s.id === id) {
      el?.classList.remove("hidden");
      nav?.classList.add("nav-active");
      if (!initialized.has(id)) {
        initialized.add(id);
        s.init();
      }
    } else {
      el?.classList.add("hidden");
      nav?.classList.remove("nav-active");
    }
  });
  activeSection = id;
}

// ── Health banner ─────────────────────────────────────────────────────────

async function checkHealth() {
  try {
    const h = await api.health();
    const dot = document.getElementById("status-dot");
    const text = document.getElementById("status-text");
    if (h.status === "ok") {
      dot.className = "status-dot status-ok";
      text.textContent = `API Online · v${h.version}`;
    }
  } catch {
    const dot = document.getElementById("status-dot");
    const text = document.getElementById("status-text");
    dot.className = "status-dot status-error";
    text.textContent = "API Offline — start the backend";
  }
}

// ── Bootstrap ─────────────────────────────────────────────────────────────

document.addEventListener("DOMContentLoaded", () => {
  // Build nav items
  const nav = document.getElementById("sidebar-nav");
  SECTIONS.forEach(s => {
    const btn = document.createElement("button");
    btn.id = `nav-${s.id}`;
    btn.className = "nav-btn";
    btn.innerHTML = `<span class="nav-icon">${s.icon}</span><span class="nav-label">${s.label.split(" ").slice(1).join(" ")}</span>`;
    btn.addEventListener("click", () => activateSection(s.id));
    nav.appendChild(btn);
  });

  // Build section containers
  const main = document.getElementById("main-content");
  SECTIONS.forEach(s => {
    const div = document.createElement("section");
    div.id = `${s.id}-section`;
    div.className = "section-container hidden";
    main.appendChild(div);
  });

  checkHealth();
  activateSection("revenue");

  // Refresh cache button
  document.getElementById("btn-refresh")?.addEventListener("click", async () => {
    await fetch("http://localhost:8000/api/cache/refresh", { method: "POST" }).catch(() => { });
    initialized.clear();
    activateSection(activeSection);
  });
});
