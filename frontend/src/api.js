/**
 * api.js — Centralised fetch wrapper for all backend calls.
 *
 * Single responsibility: abstract HTTP transport so components never
 * import fetch or handle raw errors directly.
 */

const BASE_URL = "http://localhost:8000/api";
const DEFAULT_TIMEOUT_MS = 10000;

class ApiError extends Error {
  constructor(message, status) {
    super(message);
    this.name = "ApiError";
    this.status = status;
  }
}

/**
 * Fetch with timeout and structured error handling.
 * @param {string} endpoint - API path (e.g. "/revenue")
 * @param {Object} params   - Query parameters object
 * @returns {Promise<Object>} Parsed JSON response body
 */
async function apiFetch(endpoint, params = {}) {
  const url = new URL(BASE_URL + endpoint);
  Object.entries(params).forEach(([k, v]) => {
    if (v !== null && v !== undefined && v !== "") {
      url.searchParams.set(k, v);
    }
  });

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), DEFAULT_TIMEOUT_MS);

  try {
    const res = await fetch(url.toString(), {
      signal: controller.signal,
      headers: { Accept: "application/json" },
    });

    clearTimeout(timer);

    if (!res.ok) {
      const body = await res.json().catch(() => ({}));
      throw new ApiError(body.error || `HTTP ${res.status}`, res.status);
    }

    return await res.json();
  } catch (err) {
    clearTimeout(timer);
    if (err.name === "AbortError") throw new ApiError("Request timed out", 408);
    throw err;
  }
}

export const api = {
  revenue: (params = {}) => apiFetch("/revenue", params),
  topCustomers: (params = {}) => apiFetch("/top-customers", params),
  categories: () => apiFetch("/categories"),
  regions: () => apiFetch("/regions"),
  health: () => apiFetch("/health"),
};
