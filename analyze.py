"""
analyze.py — Business analytics engine.

Reads cleaned data, merges datasets, and computes five analytical outputs:
  1. Monthly revenue (completed orders, time-sorted)
  2. Top customers (by total spend, with churn flag)
  3. Category performance (revenue, avg order value, count)
  4. Regional analysis (customers, orders, revenue, avg/customer)

All paths are config-driven; no paths are hardcoded.

Run:
    python analyze.py
"""

import logging
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import NamedTuple, Optional

import pandas as pd

# ─────────────────────────────── Logging setup ──────────────────────────────

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

# Force UTF-8 output on Windows without wrapping the stream
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except AttributeError:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_DIR / "analyze.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("analyze")

# ──────────────────────────────── Configuration ──────────────────────────────


@dataclass(frozen=True)
class AnalyticsConfig:
    """Config-driven paths and thresholds.

    All values can be overridden via environment variables so this config
    works identically in local, CI, and Docker environments.
    """

    processed_dir: Path = Path(os.getenv("PROCESSED_DIR", "data/processed"))
    output_dir: Path = Path(os.getenv("OUTPUT_DIR", "data/processed"))
    churn_days: int = int(os.getenv("CHURN_DAYS", "90"))

    customers_file: str = "customers_clean.csv"
    orders_file: str = "orders_clean.csv"

    @property
    def customers_path(self) -> Path:
        return self.processed_dir / self.customers_file

    @property
    def orders_path(self) -> Path:
        return self.processed_dir / self.orders_file


CONFIG = AnalyticsConfig()

# ──────────────────────────────── Data Types ─────────────────────────────────


class MergeReport(NamedTuple):
    """Tracks join quality metrics."""

    unmatched_customers: int
    unmatched_orders: int
    total_matched: int


class AnalyticsOutput(NamedTuple):
    """Container for all computed analytical DataFrames."""

    monthly_revenue: pd.DataFrame
    top_customers: pd.DataFrame
    category_performance: pd.DataFrame
    regional_analysis: pd.DataFrame


# ──────────────────────────────── Data Loading ───────────────────────────────


def load_data(config: AnalyticsConfig = CONFIG) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load cleaned customers and orders from the processed directory.

    Args:
        config: AnalyticsConfig instance controlling file paths.

    Returns:
        Tuple of (customers_df, orders_df).

    Raises:
        FileNotFoundError: If either file is missing — run clean_data.py first.
    """
    for path in (config.customers_path, config.orders_path):
        if not path.exists():
            raise FileNotFoundError(
                f"Cleaned data not found: {path}. Run clean_data.py first."
            )

    customers = pd.read_csv(
        config.customers_path,
        parse_dates=["signup_date"],
        dtype={"customer_id": str},
    )
    orders = pd.read_csv(
        config.orders_path,
        parse_dates=["order_date"],
        dtype={"customer_id": str, "order_id": str},
    )

    logger.info(
        "Loaded %d customers, %d orders.", len(customers), len(orders)
    )
    return customers, orders


# ────────────────────────────── Data Merging ─────────────────────────────────


def merge_data(
    customers: pd.DataFrame,
    orders: pd.DataFrame,
) -> tuple[pd.DataFrame, MergeReport]:
    """Left-join orders onto customers, explicitly tracking join quality.

    Design decision: we use an EXPLICIT left join so that orders with no
    matching customer are still retained in the merged frame (they appear with
    NaN customer fields).  A separate inner join is used to measure matched
    rows for the report.

    Args:
        customers: Cleaned customers DataFrame.
        orders: Cleaned orders DataFrame.

    Returns:
        Tuple of (merged DataFrame, MergeReport).
    """
    # Identify customers who placed no orders
    customer_ids_with_orders = set(orders["customer_id"].dropna().unique())
    all_customer_ids = set(customers["customer_id"].unique())
    unmatched_customers = len(all_customer_ids - customer_ids_with_orders)

    # Identify orders referencing non-existent customers
    order_customer_ids = set(orders["customer_id"].dropna().unique())
    unmatched_orders = len(order_customer_ids - all_customer_ids)

    if unmatched_customers:
        logger.warning(
            "%d customer(s) have no matching orders.", unmatched_customers
        )
    if unmatched_orders:
        logger.warning(
            "%d order customer_id(s) do not match any customer record.",
            unmatched_orders,
        )

    # Explicit left join: orders ← customers metadata
    merged = orders.merge(
        customers[["customer_id", "name", "region", "signup_date"]],
        on="customer_id",
        how="left",
        validate="many_to_one",
    )

    total_matched = merged["name"].notna().sum()
    report = MergeReport(
        unmatched_customers=unmatched_customers,
        unmatched_orders=unmatched_orders,
        total_matched=total_matched,
    )

    logger.info(
        "Merge complete: %d rows, %d matched, %d unmatched orders.",
        len(merged),
        total_matched,
        unmatched_orders,
    )
    return merged, report


# ────────────────────────────── Churn Logic ──────────────────────────────────


def _compute_churn_flags(
    merged: pd.DataFrame,
    config: AnalyticsConfig = CONFIG,
) -> pd.Series:
    """Return a boolean Series indexed by customer_id: True = churned.

    Churn definition: no completed order within the last ``churn_days``
    days, where reference date = max order_date in the entire dataset.

    Args:
        merged: Fully merged DataFrame with order_date and status columns.
        config: AnalyticsConfig for churn_days threshold.

    Returns:
        Series with customer_id as index and bool churn flag as value.
    """
    reference_date: pd.Timestamp = merged["order_date"].max()
    cutoff: pd.Timestamp = reference_date - pd.Timedelta(days=config.churn_days)

    logger.info(
        "Churn reference: %s | Cutoff: %s (%d-day window).",
        reference_date.date(),
        cutoff.date(),
        config.churn_days,
    )

    recent_completed = (
        merged[(merged["status"] == "completed") & (merged["order_date"] >= cutoff)][
            "customer_id"
        ]
        .dropna()
        .unique()
    )
    active_set = set(recent_completed)

    # A customer is churned if they have NO recent completed order
    all_customers_in_orders = merged["customer_id"].dropna().unique()
    churn_series = pd.Series(
        [cid not in active_set for cid in all_customers_in_orders],
        index=all_customers_in_orders,
        name="churned",
    )
    logger.info(
        "Churn flags: %d active, %d churned.",
        (~churn_series).sum(),
        churn_series.sum(),
    )
    return churn_series


# ─────────────────────────── Metric Computation ──────────────────────────────


def compute_metrics(
    merged: pd.DataFrame,
    customers: pd.DataFrame,
    config: AnalyticsConfig = CONFIG,
) -> AnalyticsOutput:
    """Compute all five analytical metrics from the merged DataFrame.

    Args:
        merged: Merged orders+customers DataFrame.
        customers: Clean customers DataFrame (for regional customer counts).
        config: AnalyticsConfig instance.

    Returns:
        AnalyticsOutput with four DataFrames.
    """
    completed = merged[merged["status"] == "completed"].copy()
    logger.info(
        "Computing metrics on %d completed orders (of %d total).",
        len(completed),
        len(merged),
    )

    # ── 1. Monthly Revenue ────────────────────────────────────────────────
    monthly_revenue = (
        completed.dropna(subset=["order_date"])
        .assign(month=lambda df: df["order_date"].dt.to_period("M"))
        .groupby("month", as_index=False)
        .agg(revenue=("amount", "sum"), order_count=("order_id", "count"))
        .sort_values("month")
        .assign(month=lambda df: df["month"].astype(str))
    )
    logger.info("Monthly revenue: %d periods computed.", len(monthly_revenue))

    # ── 2. Top Customers ─────────────────────────────────────────────────
    churn_flags = _compute_churn_flags(merged, config)

    top_customers = (
        completed.groupby("customer_id", as_index=False)
        .agg(
            total_spend=("amount", "sum"),
            order_count=("order_id", "count"),
            last_order_date=("order_date", "max"),
        )
        .merge(
            customers[["customer_id", "name", "region"]],
            on="customer_id",
            how="left",
        )
        .sort_values("total_spend", ascending=False)
        .reset_index(drop=True)
    )
    top_customers["churned"] = (
        top_customers["customer_id"].map(churn_flags).fillna(True)
    )
    top_customers["last_order_date"] = top_customers["last_order_date"].dt.strftime(
        "%Y-%m-%d"
    )
    logger.info("Top customers: %d unique buyers.", len(top_customers))

    # ── 3. Category Performance ───────────────────────────────────────────
    category_performance = (
        completed.groupby("category", as_index=False)
        .agg(
            total_revenue=("amount", "sum"),
            avg_order_value=("amount", "mean"),
            order_count=("order_id", "count"),
        )
        .round({"total_revenue": 2, "avg_order_value": 2})
        .sort_values("total_revenue", ascending=False)
        .reset_index(drop=True)
    )
    logger.info(
        "Category performance: %d categories.", len(category_performance)
    )

    # ── 4. Regional Analysis ─────────────────────────────────────────────
    # Customer counts come from the customers table (not just those who ordered)
    customers_per_region = (
        customers.groupby("region", as_index=False)
        .agg(customer_count=("customer_id", "count"))
    )

    orders_revenue_per_region = (
        completed.groupby("name")  # group by customer name for merge compat
        .agg(
            revenue=("amount", "sum"),
            order_count=("order_id", "count"),
        )
        .reset_index()
        .merge(
            customers[["name", "region"]],
            on="name",
            how="left",
        )
        .groupby("region", as_index=False)
        .agg(
            total_revenue=("revenue", "sum"),
            order_count=("order_count", "sum"),
        )
    )

    regional_analysis = (
        customers_per_region.merge(
            orders_revenue_per_region, on="region", how="left"
        )
        .fillna({"total_revenue": 0.0, "order_count": 0})
        .assign(
            avg_revenue_per_customer=lambda df: (
                df["total_revenue"] / df["customer_count"].replace(0, pd.NA)
            ).round(2)
        )
        .round({"total_revenue": 2})
        .sort_values("total_revenue", ascending=False)
        .reset_index(drop=True)
    )
    logger.info(
        "Regional analysis: %d regions.", len(regional_analysis)
    )

    return AnalyticsOutput(
        monthly_revenue=monthly_revenue,
        top_customers=top_customers,
        category_performance=category_performance,
        regional_analysis=regional_analysis,
    )


# ───────────────────────────── Output Saving ─────────────────────────────────


def save_outputs(output: AnalyticsOutput, config: AnalyticsConfig = CONFIG) -> None:
    """Persist all analytical DataFrames as formatted CSVs.

    Args:
        output: AnalyticsOutput containing the four result DataFrames.
        config: AnalyticsConfig for output directory.
    """
    config.output_dir.mkdir(parents=True, exist_ok=True)

    files = {
        "monthly_revenue.csv": output.monthly_revenue,
        "top_customers.csv": output.top_customers,
        "category_performance.csv": output.category_performance,
        "regional_analysis.csv": output.regional_analysis,
    }

    for filename, df in files.items():
        path = config.output_dir / filename
        df.to_csv(path, index=False, float_format="%.2f")
        logger.info("Saved %s (%d rows) → %s", filename, len(df), path)


# ──────────────────────────────── Entry Point ────────────────────────────────


def main() -> None:
    """Run the full analytics pipeline end-to-end."""
    logger.info("=== Analytics Pipeline --- START ===========================")

    customers, orders = load_data(CONFIG)
    merged, merge_report = merge_data(customers, orders)

    logger.info(
        "Merge report: %d unmatched customers, %d unmatched orders, %d matched.",
        merge_report.unmatched_customers,
        merge_report.unmatched_orders,
        merge_report.total_matched,
    )

    output = compute_metrics(merged, customers, CONFIG)
    save_outputs(output, CONFIG)

    logger.info("=== Analytics Pipeline --- DONE =============================")


if __name__ == "__main__":
    main()
