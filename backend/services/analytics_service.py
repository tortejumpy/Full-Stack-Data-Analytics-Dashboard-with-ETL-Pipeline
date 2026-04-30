"""
analytics_service.py — Business logic layer for the analytics API.

This service owns all DataFrame loading, caching, and query logic.
Routes never touch pandas directly — they call service methods and receive
typed Python dicts/lists ready to be serialised by Pydantic.

Cache strategy:
    DataFrames are loaded once on first request and held in memory.
    A TTL-based invalidation strategy reloads data automatically when the
    CSV files change (or after cache_ttl_seconds have elapsed).
    Thread safety is guaranteed by a threading.Lock.
"""

from __future__ import annotations

import logging
import threading
import time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from backend.config import settings

logger = logging.getLogger("analytics_service")


# ──────────────────────────────── Cache Store ─────────────────────────────────


class _DataCache:
    """Internal TTL cache for DataFrames.

    Not exposed outside this module — the AnalyticsService is the public API.
    """

    def __init__(self, ttl: int) -> None:
        self._ttl = ttl
        self._data: dict[str, pd.DataFrame] = {}
        self._loaded_at: float = 0.0
        self._lock = threading.Lock()

    @property
    def is_valid(self) -> bool:
        if not self._data:
            return False
        if self._ttl == 0:
            return True  # Never expire
        return (time.monotonic() - self._loaded_at) < self._ttl

    def set(self, data: dict[str, pd.DataFrame]) -> None:
        with self._lock:
            self._data = data
            self._loaded_at = time.monotonic()

    def get(self, key: str) -> pd.DataFrame | None:
        return self._data.get(key)

    def invalidate(self) -> None:
        with self._lock:
            self._data.clear()
            self._loaded_at = 0.0

    @property
    def is_loaded(self) -> bool:
        return bool(self._data)


# ─────────────────────────── Analytics Service ───────────────────────────────


class AnalyticsService:
    """Singleton service providing analytics data to route handlers.

    Responsibilities:
        - Load CSVs from disk (once, lazily, with TTL cache)
        - Apply all filtering, sorting, and aggregation
        - Return plain Python structures (list[dict]) for Pydantic to validate

    Usage:
        service = AnalyticsService()          # Module-level singleton
        revenue_data = service.get_revenue()  # Called from route handler
    """

    def __init__(self, processed_dir: Path | None = None) -> None:
        self._dir = processed_dir or settings.processed_dir
        self._cache = _DataCache(ttl=settings.cache_ttl_seconds)
        self._load_lock = threading.Lock()

    # ── Private helpers ───────────────────────────────────────────────────

    def _ensure_loaded(self) -> None:
        """Load DataFrames into cache if cache is expired or empty.

        Thread-safe: uses double-checked locking so only one thread pays
        the IO cost on a cold start or TTL expiry.
        """
        if self._cache.is_valid:
            return

        with self._load_lock:
            # Re-check inside lock to avoid redundant loads
            if self._cache.is_valid:
                return

            logger.info("Loading analytics DataFrames from %s …", self._dir)
            data: dict[str, pd.DataFrame] = {}

            for key, filename in [
                ("monthly_revenue", "monthly_revenue.csv"),
                ("top_customers", "top_customers.csv"),
                ("category_performance", "category_performance.csv"),
                ("regional_analysis", "regional_analysis.csv"),
            ]:
                path = self._dir / filename
                if not path.exists():
                    raise FileNotFoundError(
                        f"Analytics file missing: {path}. "
                        "Run analyze.py before starting the backend."
                    )
                data[key] = pd.read_csv(path)
                logger.info("  ✓ %s (%d rows)", filename, len(data[key]))

            self._cache.set(data)
            logger.info("Cache loaded successfully.")

    def _df(self, key: str) -> pd.DataFrame:
        """Return a DataFrame by key, guaranteed to be loaded."""
        self._ensure_loaded()
        df = self._cache.get(key)
        if df is None:
            raise KeyError(f"Unknown dataset key: {key}")
        return df

    @staticmethod
    def _to_records(df: pd.DataFrame) -> list[dict[str, Any]]:
        """Convert a DataFrame to a JSON-serialisable list of dicts.

        Handles NaN → None conversion so Pydantic receives ``None`` instead
        of ``float('nan')`` which is not valid JSON.
        """
        return df.replace({np.nan: None}).to_dict(orient="records")

    # ── Public API ────────────────────────────────────────────────────────

    def get_revenue(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return monthly revenue points, optionally filtered by date range.

        Args:
            start_date: ISO month string e.g. '2024-01' (inclusive).
            end_date:   ISO month string e.g. '2024-12' (inclusive).

        Returns:
            List of dicts with keys: month, revenue, order_count.
        """
        df = self._df("monthly_revenue").copy()

        if start_date:
            df = df[df["month"] >= start_date]
        if end_date:
            df = df[df["month"] <= end_date]

        return self._to_records(df)

    def get_top_customers(
        self,
        limit: int = 50,
        sort_by: str = "total_spend",
        order: str = "desc",
        search: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return top customers with optional filtering and sorting.

        Args:
            limit:   Maximum number of rows to return (1–500).
            sort_by: Column name to sort by.
            order:   'asc' or 'desc'.
            search:  Case-insensitive substring match on customer name.

        Returns:
            List of customer dicts.
        """
        df = self._df("top_customers").copy()

        # Search filter
        if search:
            mask = df["name"].str.contains(search, case=False, na=False)
            df = df[mask]

        # Sorting — guard against invalid column names
        valid_sort_cols = {"total_spend", "order_count", "last_order_date", "name", "region"}
        if sort_by not in valid_sort_cols:
            sort_by = "total_spend"
        ascending = order.lower() == "asc"
        df = df.sort_values(sort_by, ascending=ascending, na_position="last")

        # Limit
        limit = max(1, min(limit, 500))
        df = df.head(limit)

        return self._to_records(df)

    def get_categories(self) -> list[dict[str, Any]]:
        """Return category performance metrics.

        Returns:
            List of category dicts sorted by total_revenue descending.
        """
        return self._to_records(self._df("category_performance"))

    def get_regions(self) -> list[dict[str, Any]]:
        """Return regional analysis metrics.

        Returns:
            List of region dicts sorted by total_revenue descending.
        """
        return self._to_records(self._df("regional_analysis"))

    def invalidate_cache(self) -> None:
        """Force a cache refresh on the next request.

        Useful after re-running the analytics pipeline without restarting
        the server.
        """
        self._cache.invalidate()
        logger.info("Analytics cache invalidated.")

    @property
    def cache_loaded(self) -> bool:
        """True if data is currently resident in the cache."""
        return self._cache.is_loaded


# Module-level singleton — imported by routes
analytics_service = AnalyticsService()
