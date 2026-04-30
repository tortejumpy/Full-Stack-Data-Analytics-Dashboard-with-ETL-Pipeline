/**
 * customers.js — Top Customers section component.
 *
 * Features: sortable columns, debounced search, churn badge highlighting.
 */
import { api } from "../api.js";
import { showError, showLoading, hideLoading } from "../main.js";

let currentSort = { col: "total_spend", dir: "desc" };
let searchTimeout = null;
let allData = [];

function fmt(n) {
  return n == null ? "—" : "$" + Number(n).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function renderTable(data) {
  const tbody = document.getElementById("customers-tbody");
  if (!data.length) {
    tbody.innerHTML = `<tr><td colspan="7" class="empty-cell">No customers found.</td></tr>`;
    return;
  }
  tbody.innerHTML = data.map((c, i) => `
    <tr class="${c.churned ? "row-churned" : ""}">
      <td class="td-rank">${i + 1}</td>
      <td class="td-name">
        <span class="name-text">${c.name || c.customer_id}</span>
        ${c.churned ? '<span class="badge-churn">Churned</span>' : '<span class="badge-active">Active</span>'}
      </td>
      <td>${c.region || "—"}</td>
      <td class="td-money">${fmt(c.total_spend)}</td>
      <td class="td-center">${c.order_count}</td>
      <td class="td-center">${c.last_order_date || "—"}</td>
      <td class="td-center">${c.customer_id}</td>
    </tr>
  `).join("");
}

async function loadCustomers() {
  showLoading("customers-loader");
  try {
    const search = document.getElementById("cust-search")?.value || "";
    const res = await api.topCustomers({
      sort_by: currentSort.col,
      order:   currentSort.dir,
      search,
      limit: 100,
    });
    allData = res.data || [];
    renderTable(allData);

    const churned = allData.filter(c => c.churned).length;
    document.getElementById("cust-total").textContent  = allData.length;
    document.getElementById("cust-churned").textContent = churned;
    document.getElementById("cust-active").textContent  = allData.length - churned;
  } catch (err) {
    showError("customers-section", err.message);
  } finally {
    hideLoading("customers-loader");
  }
}

function handleSort(col) {
  if (currentSort.col === col) {
    currentSort.dir = currentSort.dir === "asc" ? "desc" : "asc";
  } else {
    currentSort = { col, dir: "desc" };
  }
  document.querySelectorAll(".th-sort").forEach(th => {
    th.classList.remove("sort-asc", "sort-desc");
    if (th.dataset.col === col) th.classList.add(currentSort.dir === "asc" ? "sort-asc" : "sort-desc");
  });
  loadCustomers();
}

export function initCustomers() {
  const section = document.getElementById("customers-section");
  section.innerHTML = `
    <div class="section-header">
      <div>
        <h2 class="section-title">Top Customers</h2>
        <p class="section-subtitle">Sorted by spend · Click column headers to re-sort</p>
      </div>
      <div class="filter-row">
        <input type="search" id="cust-search" class="search-input" placeholder="Search by name…">
      </div>
    </div>

    <div class="stats-row">
      <div class="stat-card">
        <span class="stat-label">Total Customers</span>
        <span class="stat-value" id="cust-total">—</span>
      </div>
      <div class="stat-card stat-card--active">
        <span class="stat-label">Active</span>
        <span class="stat-value" id="cust-active">—</span>
      </div>
      <div class="stat-card stat-card--churn">
        <span class="stat-label">Churned</span>
        <span class="stat-value" id="cust-churned">—</span>
      </div>
    </div>

    <div class="table-card" style="position:relative">
      <div id="customers-loader" class="loader-overlay" style="display:none"><div class="spinner"></div></div>
      <div class="table-scroll">
        <table class="data-table">
          <thead>
            <tr>
              <th>#</th>
              <th class="th-sort" data-col="name">Name / Status</th>
              <th class="th-sort" data-col="region">Region</th>
              <th class="th-sort sort-desc" data-col="total_spend">Total Spend ↕</th>
              <th class="th-sort" data-col="order_count">Orders ↕</th>
              <th class="th-sort" data-col="last_order_date">Last Order ↕</th>
              <th>ID</th>
            </tr>
          </thead>
          <tbody id="customers-tbody">
            <tr><td colspan="7" class="empty-cell">Loading…</td></tr>
          </tbody>
        </table>
      </div>
    </div>
  `;

  // Debounced search
  document.getElementById("cust-search").addEventListener("input", (e) => {
    clearTimeout(searchTimeout);
    searchTimeout = setTimeout(loadCustomers, 350);
  });

  // Column sort listeners
  document.querySelectorAll(".th-sort").forEach(th => {
    th.addEventListener("click", () => handleSort(th.dataset.col));
  });

  loadCustomers();
}
