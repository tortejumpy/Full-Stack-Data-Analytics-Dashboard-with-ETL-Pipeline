"""
test_clean_data.py — Unit tests for the data cleaning pipeline.

Each test is named for a single responsibility, uses deterministic fixture
data, and asserts exact outcomes — not just "it didn't crash".
"""
import pandas as pd
import pytest

from clean_data import (
    clean_customers,
    clean_orders,
    _validate_email,
    _parse_dates_robust,
    _normalise_status,
    _impute_amount_by_product,
    STATUS_MAP,
    FALLBACK_STATUS,
)


# ─────────────────────────────────────────────────────────────────────────────
# 1. Email Validation
# ─────────────────────────────────────────────────────────────────────────────


class TestEmailValidation:
    """Tests for _validate_email vectorised function."""

    def test_valid_emails_pass(self):
        """Standard well-formed emails must be accepted."""
        series = pd.Series(["alice@example.com", "bob@domain.co.uk", "x@y.z"])
        result = _validate_email(series)
        assert result.all(), "All valid emails should pass"

    def test_missing_at_sign_fails(self):
        """Email without '@' must be flagged as invalid."""
        series = pd.Series(["evedavis.com", "nodomain"])
        result = _validate_email(series)
        assert not result.any(), "Emails missing '@' should all fail"

    def test_missing_dot_in_domain_fails(self):
        """Email with no '.' after '@' must be flagged."""
        series = pd.Series(["bob@nodot", "alice@localhost"])
        result = _validate_email(series)
        assert not result.any()

    def test_incomplete_domain_fails(self):
        """'user@' with nothing after must fail."""
        series = pd.Series(["bob.smith@"])
        result = _validate_email(series)
        assert not result.any()

    def test_null_values_treated_as_invalid(self):
        """NaN entries must return False, not raise."""
        series = pd.Series([None, float("nan"), "valid@email.com"])
        result = _validate_email(series)
        assert result.sum() == 1, "Only the valid email should pass"

    def test_case_insensitive(self):
        """Uppercase emails should be lowercased and accepted."""
        series = pd.Series(["ALICE@EXAMPLE.COM"])
        result = _validate_email(series)
        assert result.iloc[0]


# ─────────────────────────────────────────────────────────────────────────────
# 2. Customer Deduplication
# ─────────────────────────────────────────────────────────────────────────────


class TestCustomerDeduplication:
    """Tests for deduplication logic in clean_customers."""

    def test_latest_signup_date_kept(self, customers_df):
        """When a customer_id appears twice, the row with the later
        signup_date must be retained."""
        clean, stats = clean_customers(customers_df)

        # C001 has dates 2023-01-15 and 2023-03-20 — latest should win
        c001_rows = clean[clean["customer_id"] == "C001"]
        assert len(c001_rows) == 1
        assert pd.to_datetime(c001_rows.iloc[0]["signup_date"]).month == 3

    def test_duplicate_count_reported(self, customers_df):
        """CleaningStats must correctly count removed duplicates."""
        _, stats = clean_customers(customers_df)
        assert stats.duplicates_removed >= 1

    def test_no_duplicate_customer_ids_in_output(self, customers_df):
        """Output DataFrame must have unique customer_id values."""
        clean, _ = clean_customers(customers_df)
        assert clean["customer_id"].is_unique


# ─────────────────────────────────────────────────────────────────────────────
# 3. Date Parsing
# ─────────────────────────────────────────────────────────────────────────────


class TestDateParsing:
    """Tests for _parse_dates_robust multi-format parsing."""

    def test_iso_format_parsed(self):
        series = pd.Series(["2024-01-15"])
        result = _parse_dates_robust(series)
        assert result.iloc[0] == pd.Timestamp("2024-01-15")

    def test_dmy_slash_format_parsed(self):
        series = pd.Series(["15/06/2023"])
        result = _parse_dates_robust(series)
        assert result.iloc[0].year == 2023
        assert result.notna().all()

    def test_natural_language_date_parsed(self):
        series = pd.Series(["Feb 15 2024", "Mar 10 2025"])
        result = _parse_dates_robust(series)
        assert result.notna().all()
        assert result.iloc[0].month == 2

    def test_unparseable_becomes_nat(self):
        series = pd.Series(["not-a-date", "nodatehere", "invalid"])
        result = _parse_dates_robust(series)
        assert result.isna().all()

    def test_null_stays_nat(self):
        series = pd.Series([None])
        result = _parse_dates_robust(series)
        assert pd.isna(result.iloc[0])

    def test_mixed_formats_in_one_column(self):
        """Real-world scenario: one column with multiple format styles."""
        series = pd.Series([
            "2024-01-10",
            "10/02/2024",
            "Feb 15 2024",
            "15-Mar-2024",
        ])
        result = _parse_dates_robust(series)
        assert result.notna().sum() == 4, "All parseable dates should succeed"


# ─────────────────────────────────────────────────────────────────────────────
# 4. Median Imputation
# ─────────────────────────────────────────────────────────────────────────────


class TestMedianImputation:
    """Tests for _impute_amount_by_product grouped median filling."""

    def test_group_median_fills_null(self, orders_df):
        """Null amount in a group with known values must be filled with
        the group median — not the global median."""
        # Laptops: [1299.99, None] → median = 1299.99
        # Mice:    [29.99, None]   → median = 29.99
        result = _impute_amount_by_product(orders_df.copy())
        laptops = result[result["product"] == "Laptop"]["amount"]
        mice    = result[result["product"] == "Mouse"]["amount"]
        assert laptops.notna().all()
        assert mice.notna().all()
        # The imputed value should equal the only other value in the group
        assert laptops.iloc[1] == pytest.approx(1299.99)
        assert mice.iloc[1]    == pytest.approx(29.99)

    def test_global_fallback_for_all_null_group(self):
        """A group where ALL rows have null amount should fall back to
        the global median, not remain NaN."""
        df = pd.DataFrame({
            "product": ["Ghost", "Ghost", "Real"],
            "amount":  [None, None, 100.0],
        })
        result = _impute_amount_by_product(df)
        assert result["amount"].notna().all()
        # Global median is 100.0; ghost rows should be filled with 100.0
        ghost_vals = result[result["product"] == "Ghost"]["amount"]
        assert all(abs(v - 100.0) < 1e-6 for v in ghost_vals), f"Expected 100.0, got {ghost_vals.tolist()}"

    def test_non_null_rows_unchanged(self, orders_df):
        """Values that were already present must not be mutated."""
        result = _impute_amount_by_product(orders_df.copy())
        assert result.loc[0, "amount"] == pytest.approx(1299.99)
        assert result.loc[1, "amount"] == pytest.approx(29.99)


# ─────────────────────────────────────────────────────────────────────────────
# 5. Status Normalisation
# ─────────────────────────────────────────────────────────────────────────────


class TestStatusNormalisation:
    """Tests for _normalise_status mapping and fallback logic."""

    def test_known_statuses_mapped_correctly(self):
        series = pd.Series(["completed", "COMPLETED", "Shipped", "cancelled"])
        result = _normalise_status(series)
        assert result.iloc[0] == "completed"
        assert result.iloc[1] == "completed"
        assert result.iloc[2] == "shipped"
        assert result.iloc[3] == "cancelled"

    def test_unknown_status_becomes_fallback(self):
        series = pd.Series(["mystery", "???", "whatever"])
        result = _normalise_status(series)
        assert (result == FALLBACK_STATUS).all()

    def test_case_insensitive_mapping(self):
        series = pd.Series(["PENDING", "Pending", "pEnDiNg"])
        result = _normalise_status(series)
        assert (result == "pending").all()

    def test_all_canonical_values_in_map(self):
        """Every value in STATUS_MAP must map to a known canonical value."""
        canonical = {"completed", "shipped", "pending", "cancelled", "refunded"}
        for raw, mapped in STATUS_MAP.items():
            assert mapped in canonical, f"'{raw}' maps to unknown value '{mapped}'"

    def test_clean_orders_status_output(self, orders_df):
        """After clean_orders, status column must contain only canonical values."""
        clean, _ = clean_orders(orders_df)
        canonical = {"completed", "shipped", "pending", "cancelled", "refunded", FALLBACK_STATUS}
        unexpected = set(clean["status"].unique()) - canonical
        assert not unexpected, f"Unexpected status values: {unexpected}"
