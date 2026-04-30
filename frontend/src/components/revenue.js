/**
 * revenue.js — Monthly Revenue section component.
 *
 * Renders a line chart with date range pickers that re-fetch data on change.
 */
import { api } from "../api.js";
import { createLineChart } from "../charts.js";
import { showError, showLoading, hideLoading } from "../main.js";

let chartInstance = null;

function getFilterValues() {
  const start = document.getElementById("rev-start")?.value || null;
  const end   = document.getElementById("rev-end")?.value   || null;
  return { start_date: start, end_date: end };
}

async function loadRevenue() {
  const loaderId = "revenue-loader";
  showLoading(loaderId);
  try {
    const params = getFilterValues();
    const res = await api.revenue(params);
    const data = res.data || [];

    if (!data.length) {
      document.getElementById("revenue-chart").parentElement.innerHTML =
        '<p class="empty-state">No revenue data found for the selected range.</p>';
      return;
    }

    const labels = data.map((d) => d.month);
    const values = data.map((d) => d.revenue);
    chartInstance = createLineChart("revenue-chart", labels, values, chartInstance);

    // Summary stats
    const total = values.reduce((a, b) => a + b, 0);
    const avg   = total / values.length;
    document.getElementById("rev-total").textContent = "$" + total.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    document.getElementById("rev-avg").textContent   = "$" + avg.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    document.getElementById("rev-months").textContent = data.length;
  } catch (err) {
    showError("revenue-section", err.message);
  } finally {
    hideLoading(loaderId);
  }
}

export function initRevenue() {
  const section = document.getElementById("revenue-section");
  section.innerHTML = `
    <div class="section-header">
      <div>
        <h2 class="section-title">Monthly Revenue</h2>
        <p class="section-subtitle">Completed orders only · Sorted chronologically</p>
      </div>
      <div class="filter-row">
        <label class="filter-label">From
          <input type="month" id="rev-start" class="filter-input">
        </label>
        <label class="filter-label">To
          <input type="month" id="rev-end" class="filter-input">
        </label>
        <button id="rev-apply" class="btn-primary">Apply</button>
        <button id="rev-reset" class="btn-ghost">Reset</button>
      </div>
    </div>

    <div class="stats-row">
      <div class="stat-card">
        <span class="stat-label">Total Revenue</span>
        <span class="stat-value" id="rev-total">—</span>
      </div>
      <div class="stat-card">
        <span class="stat-label">Avg / Month</span>
        <span class="stat-value" id="rev-avg">—</span>
      </div>
      <div class="stat-card">
        <span class="stat-label">Months Tracked</span>
        <span class="stat-value" id="rev-months">—</span>
      </div>
    </div>

    <div class="chart-card">
      <div id="revenue-loader" class="loader-overlay" style="display:none">
        <div class="spinner"></div>
      </div>
      <div class="chart-wrap" style="height:340px">
        <canvas id="revenue-chart"></canvas>
      </div>
    </div>
  `;

  document.getElementById("rev-apply").addEventListener("click", loadRevenue);
  document.getElementById("rev-reset").addEventListener("click", () => {
    document.getElementById("rev-start").value = "";
    document.getElementById("rev-end").value = "";
    loadRevenue();
  });

  loadRevenue();
}
