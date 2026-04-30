"""
conftest.py — Shared pytest fixtures for the data cleaning test suite.

Fixtures use intentionally dirty data to validate edge-case handling.
"""
import pandas as pd
import pytest


@pytest.fixture
def customers_df() -> pd.DataFrame:
    """Dirty customers DataFrame with known edge cases."""
    return pd.DataFrame(
        {
            "customer_id": ["C001", "C001", "C002", "C003", "C004", "C005"],
            "name":        ["Alice", "Alice Dup", "Bob", "  Carol  ", "David", "Eve"],
            "email":       [
                "alice@example.com",   # valid
                "alice@example.com",   # valid (duplicate)
                "bob.smith@",          # invalid — no domain
                "carol@domain.com",    # valid
                "DAVID@COMPANY.COM",   # valid but uppercase
                "evedavis.com",        # invalid — no @
            ],
            "signup_date": [
                "2023-01-15",
                "2023-03-20",   # later date → should be kept for C001
                "2022-11-05",
                "15/06/2023",
                "invalid-date",  # → NaT
                "2023-04-10",
            ],
            "region": ["North", "North", "South", None, "West", None],
        }
    )


@pytest.fixture
def orders_df() -> pd.DataFrame:
    """Dirty orders DataFrame with mixed date formats, nulls, bad statuses."""
    return pd.DataFrame(
        {
            "order_id":   ["O001", "O002", "O003", "O004", "O005", None],
            "customer_id":["C001", "C002", "C003", "C004", None,   None],
            "product":    ["Laptop", "Mouse", "Laptop", "Mouse", "Desk", "Desk"],
            "category":   ["Electronics"] * 4 + ["Furniture"] * 2,
            "amount":     [1299.99, 29.99, None, None, 499.99, None],
            "order_date": [
                "2024-01-10",       # ISO
                "10/02/2024",       # DD/MM/YYYY
                "Feb 15 2024",      # Natural language
                "15-Mar-2024",      # DD-Mon-YYYY
                "not-a-date",       # → NaT
                "2024-05-01",       # valid
            ],
            "status": ["completed", "COMPLETED", "Shipped", "cancelled", "PENDING", "refunded"],
        }
    )
