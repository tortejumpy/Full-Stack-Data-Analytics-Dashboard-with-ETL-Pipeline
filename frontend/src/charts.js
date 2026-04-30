/**
 * charts.js — Chart.js v4 wrapper utilities.
 *
 * Provides factory functions that encapsulate chart configuration so
 * components stay focused on data logic, not chart boilerplate.
 */

/* global Chart */

const PALETTE = {
  blue:    "rgba(99, 179, 237, 1)",
  blueFill:"rgba(99, 179, 237, 0.15)",
  purple:  "rgba(159, 122, 234, 1)",
  green:   "rgba(72, 199, 142, 1)",
  orange:  "rgba(237, 137, 54, 1)",
  pink:    "rgba(237, 100, 166, 1)",
  teal:    "rgba(56, 189, 189, 1)",
  multi: [
    "rgba(99, 179, 237, 0.85)",
    "rgba(159, 122, 234, 0.85)",
    "rgba(72, 199, 142, 0.85)",
    "rgba(237, 137, 54, 0.85)",
    "rgba(237, 100, 166, 0.85)",
    "rgba(56, 189, 189, 0.85)",
    "rgba(246, 224, 94, 0.85)",
  ],
};

const BASE_FONT = { family: "'Inter', sans-serif", size: 12 };

const GRID_COLOR = "rgba(255,255,255,0.07)";
const TICK_COLOR = "rgba(255,255,255,0.45)";

function baseScales() {
  return {
    x: {
      grid: { color: GRID_COLOR },
      ticks: { color: TICK_COLOR, font: BASE_FONT },
    },
    y: {
      grid: { color: GRID_COLOR },
      ticks: {
        color: TICK_COLOR,
        font: BASE_FONT,
        callback: (v) => "$" + Number(v).toLocaleString(),
      },
    },
  };
}

/**
 * Create or update a line chart on the given canvas.
 * @param {string} canvasId
 * @param {string[]} labels
 * @param {number[]} values
 * @param {Chart|null} existing - Previous chart instance to destroy
 * @returns {Chart}
 */
export function createLineChart(canvasId, labels, values, existing = null) {
  if (existing) existing.destroy();
  const ctx = document.getElementById(canvasId).getContext("2d");
  return new Chart(ctx, {
    type: "line",
    data: {
      labels,
      datasets: [{
        label: "Revenue ($)",
        data: values,
        borderColor: PALETTE.blue,
        backgroundColor: PALETTE.blueFill,
        borderWidth: 2.5,
        pointBackgroundColor: PALETTE.blue,
        pointRadius: 4,
        pointHoverRadius: 7,
        fill: true,
        tension: 0.4,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { intersect: false, mode: "index" },
      plugins: {
        legend: { labels: { color: TICK_COLOR, font: BASE_FONT } },
        tooltip: {
          backgroundColor: "rgba(15,23,42,0.95)",
          titleColor: "#e2e8f0",
          bodyColor: "#94a3b8",
          callbacks: {
            label: (ctx) => ` $${Number(ctx.parsed.y).toLocaleString(undefined, { minimumFractionDigits: 2 })}`,
          },
        },
      },
      scales: baseScales(),
    },
  });
}

/**
 * Create or update a bar chart.
 */
export function createBarChart(canvasId, labels, values, labelText = "Revenue", existing = null) {
  if (existing) existing.destroy();
  const ctx = document.getElementById(canvasId).getContext("2d");
  return new Chart(ctx, {
    type: "bar",
    data: {
      labels,
      datasets: [{
        label: labelText,
        data: values,
        backgroundColor: PALETTE.multi.slice(0, labels.length),
        borderRadius: 6,
        borderSkipped: false,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: "rgba(15,23,42,0.95)",
          titleColor: "#e2e8f0",
          bodyColor: "#94a3b8",
          callbacks: {
            label: (ctx) => ` $${Number(ctx.parsed.y).toLocaleString(undefined, { minimumFractionDigits: 2 })}`,
          },
        },
      },
      scales: baseScales(),
    },
  });
}

/**
 * Create or update a doughnut chart.
 */
export function createDoughnutChart(canvasId, labels, values, existing = null) {
  if (existing) existing.destroy();
  const ctx = document.getElementById(canvasId).getContext("2d");
  return new Chart(ctx, {
    type: "doughnut",
    data: {
      labels,
      datasets: [{
        data: values,
        backgroundColor: PALETTE.multi.slice(0, labels.length),
        borderColor: "rgba(15,23,42,0.6)",
        borderWidth: 2,
        hoverOffset: 8,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      cutout: "65%",
      plugins: {
        legend: {
          position: "right",
          labels: { color: TICK_COLOR, font: BASE_FONT, padding: 14, boxWidth: 14 },
        },
        tooltip: {
          backgroundColor: "rgba(15,23,42,0.95)",
          titleColor: "#e2e8f0",
          bodyColor: "#94a3b8",
          callbacks: {
            label: (ctx) => ` $${Number(ctx.parsed).toLocaleString(undefined, { minimumFractionDigits: 2 })}`,
          },
        },
      },
    },
  });
}
