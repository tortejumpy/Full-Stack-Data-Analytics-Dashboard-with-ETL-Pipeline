/**
 * regions.js — Regional Analysis section (summary cards + table).
 */
import { api } from "../api.js";
import { showError, showLoading, hideLoading } from "../main.js";

const REGION_ICONS = {
  North: "", South: "", East: "", West: "", Unknown: "",
};

function fmt(n) {
  return n == null ? "—" : "$" + Number(n).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

export async function initRegions() {
  const section = document.getElementById("regions-section");
  section.innerHTML = `
    <div class="section-header">
      <div>
        <h2 class="section-title">Regional Analysis</h2>
        <p class="section-subtitle">Customer distribution and revenue by region</p>
      </div>
    </div>
    <div id="regions-loader" class="loader-overlay" style="display:none"><div class="spinner"></div></div>
    <div id="regions-cards" class="region-cards-grid"></div>
    <div class="table-card" style="margin-top:1.5rem">
      <div class="table-scroll">
        <table class="data-table">
          <thead>
            <tr>
              <th>Region</th>
              <th class="td-center">Customers</th>
              <th class="td-center">Orders</th>
              <th class="td-money">Total Revenue</th>
              <th class="td-money">Avg Revenue / Customer</th>
            </tr>
          </thead>
          <tbody id="regions-tbody"></tbody>
        </table>
      </div>
    </div>
  `;

  showLoading("regions-loader");
  try {
    const res = await api.regions();
    const data = res.data || [];

    // Summary cards
    const cardsEl = document.getElementById("regions-cards");
    cardsEl.innerHTML = data.map(r => `
      <div class="region-card">
        <div class="region-icon">${REGION_ICONS[r.region] || "📍"}</div>
        <div class="region-name">${r.region}</div>
        <div class="region-metric">${fmt(r.total_revenue)}</div>
        <div class="region-sub">${r.customer_count} customers · ${r.order_count} orders</div>
      </div>
    `).join("");

    // Table
    const tbody = document.getElementById("regions-tbody");
    tbody.innerHTML = data.map(r => `
      <tr>
        <td><strong>${REGION_ICONS[r.region] || "📍"} ${r.region}</strong></td>
        <td class="td-center">${r.customer_count}</td>
        <td class="td-center">${r.order_count}</td>
        <td class="td-money">${fmt(r.total_revenue)}</td>
        <td class="td-money">${fmt(r.avg_revenue_per_customer)}</td>
      </tr>
    `).join("");
  } catch (err) {
    showError("regions-section", err.message);
  } finally {
    hideLoading("regions-loader");
  }
}
