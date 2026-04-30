"""
clean_data.py — Production-grade data cleaning pipeline.

Handles customers.csv and orders.csv with full edge-case coverage:
  - Email validation (lowercase, must contain '@' and '.')
  - Deduplication using latest signup_date
  - Multi-format date parsing with graceful NaT fallback
  - Grouped median imputation for missing amounts
  - Status normalization with fallback bucket
  - Cleaning report generation

Run:
    python clean_data.py
"""

import logging
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import numpy as np
import pandas as pd
from dateutil import parser as dateutil_parser

# ─────────────────────────────── Logging setup ──────────────────────────────

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

# Force UTF-8 output on Windows without wrapping the stream
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # Python 3.7+ TextIOWrapper
except AttributeError:
    pass  # In test captures or non-reconfigurable streams, skip silently

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_DIR / "clean_data.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("clean_data")

# ─────────────────────────────── Configuration ──────────────────────────────

DATA_RAW_DIR = Path("data/raw")
DATA_PROCESSED_DIR = Path("data/processed")
DATA_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

# Status normalisation map: raw value → canonical value
STATUS_MAP: dict[str, str] = {
    "completed": "completed",
    "complete": "completed",
    "shipped": "shipped",
    "shipping": "shipped",
    "pending": "pending",
    "in_progress": "pending",
    "cancelled": "cancelled",
    "canceled": "cancelled",
    "refunded": "refunded",
    "return": "refunded",
}
FALLBACK_STATUS = "other"

# ─────────────────────────────────── DTOs ───────────────────────────────────


@dataclass
class CleaningStats:
    """Holds before/after statistics for a single dataset."""

    name: str
    rows_before: int = 0
    rows_after: int = 0
    nulls_before: dict[str, int] = field(default_factory=dict)
    nulls_after: dict[str, int] = field(default_factory=dict)
    duplicates_removed: int = 0
    extra: dict[str, Any] = field(default_factory=dict)

    @property
    def rows_dropped(self) -> int:
        return self.rows_before - self.rows_after


# ─────────────────────────────── Data Loading ────────────────────────────────


def load_data(path: Path, **read_kwargs) -> pd.DataFrame:
    """Load a CSV file from *path* and return a DataFrame.

    Args:
        path: Absolute or relative path to the CSV file.
        **read_kwargs: Extra keyword arguments forwarded to ``pd.read_csv``.

    Returns:
        Raw DataFrame with no transformations applied.

    Raises:
        FileNotFoundError: If the file does not exist.
        ValueError: If the file is empty or cannot be parsed.
    """
    if not path.exists():
        raise FileNotFoundError(f"Data file not found: {path}")

    logger.info("Loading data from %s", path)
    df = pd.read_csv(path, **read_kwargs)

    if df.empty:
        raise ValueError(f"File is empty: {path}")

    logger.info("Loaded %d rows × %d columns from %s", len(df), df.shape[1], path.name)
    return df


# ─────────────────────────────── Email helpers ───────────────────────────────

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def _validate_email(series: pd.Series) -> pd.Series:
    """Return a boolean mask: True where the email is structurally valid.

    Rules:
      - Must be a non-null string
      - Lowercased before check
      - Must match ``local@domain.tld`` pattern (RFC-lite)
    """
    # Lower-case safely; non-strings become NaN, then treated as invalid
    lowered = series.where(series.notna()).str.lower()
    return lowered.str.match(_EMAIL_RE, na=False)


# ───────────────────────────── Date parsing helpers ──────────────────────────


def _parse_dates_robust(series: pd.Series) -> pd.Series:
    """Parse a mixed-format date series to ``datetime64[ns]`` with NaT fallback.

    Tries (in order):
      1. ``pd.to_datetime`` with ``infer_datetime_format`` (vectorised, fast).
      2. ``dateutil.parser.parse`` row-by-row only for rows that failed step 1
         (avoids the overhead of per-row parsing for clean data).

    Returns:
        Series of ``datetime64[ns]``.
    """
    # Step 1 — fast vectorised parse
    parsed = pd.to_datetime(series, errors="coerce", dayfirst=False)

    # Step 2 — fallback for rows that produced NaT
    nat_mask = parsed.isna() & series.notna()
    if nat_mask.any():
        logger.debug("Attempting dateutil fallback on %d unparseable dates.", nat_mask.sum())

        def _try_dateutil(val: Any) -> Optional[pd.Timestamp]:
            try:
                return pd.Timestamp(dateutil_parser.parse(str(val)))
            except (ValueError, OverflowError, TypeError):
                return pd.NaT

        parsed.loc[nat_mask] = series.loc[nat_mask].map(_try_dateutil)

    return parsed


# ─────────────────────────── Customer Cleaning ───────────────────────────────


def clean_customers(df: pd.DataFrame) -> tuple[pd.DataFrame, CleaningStats]:
    """Clean the customers DataFrame.

    Operations (in order):
      1. Strip whitespace from all string columns.
      2. Lowercase email column.
      3. Flag / filter invalid emails.
      4. Parse signup_date robustly.
      5. Deduplicate by customer_id, keeping the latest signup_date.
      6. Fill missing region with "Unknown".

    Args:
        df: Raw customers DataFrame.

    Returns:
        Tuple of (cleaned DataFrame, CleaningStats).
    """
    stats = CleaningStats(
        name="customers",
        rows_before=len(df),
        nulls_before=df.isnull().sum().to_dict(),
    )

    df = df.copy()

    # 1. Strip whitespace from all object columns (handles NaN safely)
    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].apply(lambda col: col.str.strip() if col.notna().any() else col)
    logger.info("[customers] Whitespace stripped from %d string columns.", len(str_cols))

    # 2. Lowercase email
    df["email"] = df["email"].str.lower()

    # 3. Email validation — mark invalid rows
    valid_email_mask = _validate_email(df["email"])
    invalid_count = (~valid_email_mask).sum()
    if invalid_count:
        logger.warning(
            "[customers] %d rows have invalid emails — filtering out: %s",
            invalid_count,
            df.loc[~valid_email_mask, "email"].tolist(),
        )
    df = df[valid_email_mask].copy()
    stats.extra["invalid_emails_removed"] = int(invalid_count)

    # 4. Robust date parsing
    df["signup_date"] = _parse_dates_robust(df["signup_date"])
    nat_dates = df["signup_date"].isna().sum()
    if nat_dates:
        logger.warning("[customers] %d signup_date values could not be parsed → NaT.", nat_dates)

    # 5. Deduplicate: keep latest signup_date per customer_id
    before_dedup = len(df)
    df = (
        df.sort_values("signup_date", ascending=False, na_position="last")
        .drop_duplicates(subset=["customer_id"], keep="first")
        .reset_index(drop=True)
    )
    dupes_removed = before_dedup - len(df)
    stats.duplicates_removed = dupes_removed
    if dupes_removed:
        logger.info("[customers] %d duplicate customer rows removed (kept latest).", dupes_removed)

    # 6. Fill missing region
    missing_region = df["region"].isna().sum()
    df["region"] = df["region"].fillna("Unknown")
    if missing_region:
        logger.info("[customers] %d missing region values filled with 'Unknown'.", missing_region)

    stats.rows_after = len(df)
    stats.nulls_after = df.isnull().sum().to_dict()
    logger.info("[customers] Cleaning complete: %d → %d rows.", stats.rows_before, stats.rows_after)
    return df, stats


# ──────────────────────────── Order Cleaning ─────────────────────────────────


def _normalise_status(series: pd.Series) -> pd.Series:
    """Map raw order status strings to canonical values.

    Lookup is case-insensitive. Values not found in STATUS_MAP fall back to
    FALLBACK_STATUS ('other').

    Args:
        series: Raw status column.

    Returns:
        Series with normalised status values.
    """
    lowered = series.str.lower().str.strip()
    return lowered.map(STATUS_MAP).fillna(FALLBACK_STATUS)


def _impute_amount_by_product(df: pd.DataFrame) -> pd.DataFrame:
    """Fill missing ``amount`` with the median grouped by ``product``.

    For products where ALL rows have null amount (group median is NaN),
    falls back to the global median to avoid leaving NaNs.

    Args:
        df: Orders DataFrame with ``amount`` and ``product`` columns.

    Returns:
        DataFrame with ``amount`` nulls filled.
    """
    global_median = df["amount"].median()
    group_medians = df.groupby("product")["amount"].transform("median")

    # Where group median is NaN (all-null group), use global median
    fill_values = group_medians.fillna(global_median)
    df["amount"] = df["amount"].fillna(fill_values)
    return df


def clean_orders(df: pd.DataFrame) -> tuple[pd.DataFrame, CleaningStats]:
    """Clean the orders DataFrame.

    Operations (in order):
      1. Drop rows where BOTH order_id and customer_id are null.
      2. Robust multi-format date parsing for order_date.
      3. Median imputation for amount, grouped by product.
      4. Status normalisation via mapping + fallback bucket.
      5. Add order_year_month derived column.

    Args:
        df: Raw orders DataFrame.

    Returns:
        Tuple of (cleaned DataFrame, CleaningStats).
    """
    stats = CleaningStats(
        name="orders",
        rows_before=len(df),
        nulls_before=df.isnull().sum().to_dict(),
    )

    df = df.copy()

    # Strip object columns
    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].apply(lambda col: col.str.strip() if col.notna().any() else col)

    # 1. Drop rows unrecoverable due to both IDs being null
    unrecoverable_mask = df["order_id"].isna() & df["customer_id"].isna()
    dropped_unrecoverable = unrecoverable_mask.sum()
    if dropped_unrecoverable:
        logger.warning(
            "[orders] Dropping %d rows with both order_id and customer_id null.",
            dropped_unrecoverable,
        )
    df = df[~unrecoverable_mask].copy()
    stats.extra["unrecoverable_rows_dropped"] = int(dropped_unrecoverable)

    # 2. Robust date parsing
    df["order_date"] = _parse_dates_robust(df["order_date"])
    nat_dates = df["order_date"].isna().sum()
    if nat_dates:
        logger.warning(
            "[orders] %d order_date values could not be parsed → NaT (rows retained).", nat_dates
        )
    stats.extra["unparseable_dates"] = int(nat_dates)

    # 3. Amount imputation (grouped median, fallback global median)
    missing_amount_before = df["amount"].isna().sum()
    df = _impute_amount_by_product(df)
    missing_amount_after = df["amount"].isna().sum()
    logger.info(
        "[orders] Amount imputation: %d nulls filled (%d remaining).",
        missing_amount_before - missing_amount_after,
        missing_amount_after,
    )
    stats.extra["amounts_imputed"] = int(missing_amount_before - missing_amount_after)

    # 4. Status normalisation
    unique_raw = df["status"].dropna().unique().tolist()
    df["status"] = _normalise_status(df["status"].fillna(""))
    logger.info(
        "[orders] Status normalised. Raw unique values: %s → Canonical: %s",
        unique_raw,
        df["status"].unique().tolist(),
    )

    # 5. Derived column: order_year_month (Period for easy time-series grouping)
    df["order_year_month"] = df["order_date"].dt.to_period("M").astype(str)

    stats.rows_after = len(df)
    stats.nulls_after = df.isnull().sum().to_dict()
    logger.info("[orders] Cleaning complete: %d → %d rows.", stats.rows_before, stats.rows_after)
    return df, stats


# ─────────────────────────── Report Generation ───────────────────────────────


def generate_report(*stats_list: CleaningStats) -> dict[str, Any]:
    """Build a structured cleaning report from one or more CleaningStats objects.

    Args:
        *stats_list: Any number of CleaningStats instances.

    Returns:
        Nested dict suitable for JSON serialisation or logging.
    """
    report: dict[str, Any] = {"datasets": {}}

    for stats in stats_list:
        report["datasets"][stats.name] = {
            "rows_before": stats.rows_before,
            "rows_after": stats.rows_after,
            "rows_dropped": stats.rows_dropped,
            "duplicates_removed": stats.duplicates_removed,
            "nulls_before": stats.nulls_before,
            "nulls_after": stats.nulls_after,
            **stats.extra,
        }

    logger.info("─── Cleaning Report ───────────────────────────────────────")
    for ds_name, ds_stats in report["datasets"].items():
        logger.info(
            "[%s] %d → %d rows (dropped %d, dupes %d)",
            ds_name,
            ds_stats["rows_before"],
            ds_stats["rows_after"],
            ds_stats["rows_dropped"],
            ds_stats["duplicates_removed"],
        )
    logger.info("───────────────────────────────────────────────────────────")

    return report


# ──────────────────────────────── Entry Point ────────────────────────────────


def main() -> None:
    """Run the full data cleaning pipeline end-to-end."""
    logger.info("=== Data Cleaning Pipeline --- START ========================")

    # Load
    customers_raw = load_data(DATA_RAW_DIR / "customers.csv")
    orders_raw = load_data(DATA_RAW_DIR / "orders.csv")

    # Clean
    customers_clean, cust_stats = clean_customers(customers_raw)
    orders_clean, ord_stats = clean_orders(orders_raw)

    # Save
    cust_out = DATA_PROCESSED_DIR / "customers_clean.csv"
    ord_out = DATA_PROCESSED_DIR / "orders_clean.csv"
    customers_clean.to_csv(cust_out, index=False)
    orders_clean.to_csv(ord_out, index=False)
    logger.info("Saved cleaned customers → %s", cust_out)
    logger.info("Saved cleaned orders    → %s", ord_out)

    # Report
    generate_report(cust_stats, ord_stats)

    logger.info("=== Data Cleaning Pipeline --- DONE ========================")


if __name__ == "__main__":
    main()
