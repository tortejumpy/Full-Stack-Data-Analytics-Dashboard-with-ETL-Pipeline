/**
 * categories.js — Category Performance section (doughnut + bar chart).
 */
import { api } from "../api.js";
import { createDoughnutChart, createBarChart } from "../charts.js";
import { showError, showLoading, hideLoading } from "../main.js";

let donutInstance = null;
let barInstance   = null;

export async function initCategories() {
  const section = document.getElementById("categories-section");
  section.innerHTML = `
    <div class="section-header">
      <div>
        <h2 class="section-title">Category Performance</h2>
        <p class="section-subtitle">Revenue share and avg order value per category</p>
      </div>
    </div>
    <div id="categories-loader" class="loader-overlay" style="display:none"><div class="spinner"></div></div>
    <div class="dual-chart-grid">
      <div class="chart-card">
        <h3 class="chart-label">Revenue Share</h3>
        <div class="chart-wrap" style="height:300px"><canvas id="cat-donut"></canvas></div>
      </div>
      <div class="chart-card">
        <h3 class="chart-label">Revenue by Category</h3>
        <div class="chart-wrap" style="height:300px"><canvas id="cat-bar"></canvas></div>
      </div>
    </div>
    <div class="table-card" style="margin-top:1.5rem">
      <div class="table-scroll">
        <table class="data-table">
          <thead>
            <tr>
              <th>Category</th>
              <th class="td-money">Total Revenue</th>
              <th class="td-money">Avg Order Value</th>
              <th class="td-center">Orders</th>
            </tr>
          </thead>
          <tbody id="cat-tbody"></tbody>
        </table>
      </div>
    </div>
  `;

  showLoading("categories-loader");
  try {
    const res = await api.categories();
    const data = res.data || [];

    const labels  = data.map(d => d.category);
    const revenue = data.map(d => d.total_revenue);

    donutInstance = createDoughnutChart("cat-donut", labels, revenue, donutInstance);
    barInstance   = createBarChart("cat-bar", labels, revenue, "Revenue ($)", barInstance);

    const tbody = document.getElementById("cat-tbody");
    tbody.innerHTML = data.map(d => `
      <tr>
        <td><strong>${d.category}</strong></td>
        <td class="td-money">$${Number(d.total_revenue).toLocaleString(undefined, {minimumFractionDigits:2})}</td>
        <td class="td-money">$${Number(d.avg_order_value).toLocaleString(undefined, {minimumFractionDigits:2})}</td>
        <td class="td-center">${d.order_count}</td>
      </tr>
    `).join("");
  } catch (err) {
    showError("categories-section", err.message);
  } finally {
    hideLoading("categories-loader");
  }
}
