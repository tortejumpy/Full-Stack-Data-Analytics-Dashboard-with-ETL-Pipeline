"""
schemas.py — Pydantic v2 response models for all API endpoints.

Using strict typing ensures serialisation errors surface at the boundary
rather than silently corrupting client data.
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field, ConfigDict


# Base 


class BaseResponse(BaseModel):
    """Common envelope for all list responses."""

    model_config = ConfigDict(populate_by_name=True)

    success: bool = True
    count: int = Field(description="Number of items in the payload.")


#  Revenue 


class RevenuePoint(BaseModel):
    """A single month-revenue data point."""

    model_config = ConfigDict(populate_by_name=True)

    month: str = Field(description="ISO year-month string e.g. '2024-01'.")
    revenue: float = Field(ge=0, description="Total completed order revenue.")
    order_count: int = Field(ge=0, description="Number of completed orders.")


class RevenueResponse(BaseResponse):
    data: list[RevenuePoint]


# Customers 

class CustomerRow(BaseModel):
    """Top customer record with spend and churn status."""

    model_config = ConfigDict(populate_by_name=True)

    customer_id: str
    name: Optional[str] = None
    region: Optional[str] = None
    total_spend: float = Field(ge=0)
    order_count: int = Field(ge=0)
    last_order_date: Optional[str] = None
    churned: bool


class CustomerResponse(BaseResponse):
    data: list[CustomerRow]


#  Categories 


class CategoryRow(BaseModel):
    """Category-level aggregated performance metrics."""

    model_config = ConfigDict(populate_by_name=True)

    category: str
    total_revenue: float = Field(ge=0)
    avg_order_value: float = Field(ge=0)
    order_count: int = Field(ge=0)


class CategoryResponse(BaseResponse):
    data: list[CategoryRow]


#Regions 


class RegionRow(BaseModel):
    """Regional aggregated metrics."""

    model_config = ConfigDict(populate_by_name=True)

    region: str
    customer_count: int = Field(ge=0)
    order_count: float = Field(ge=0)  # float because pandas fills with 0.0
    total_revenue: float = Field(ge=0)
    avg_revenue_per_customer: Optional[float] = None


class RegionResponse(BaseResponse):
    data: list[RegionRow]


# Health 


class HealthResponse(BaseModel):
    """Lightweight liveness check response."""

    status: str = "ok"
    version: str
    cache_loaded: bool


# ─────────────────────────── Error ───────────────────────────────────────────


class ErrorResponse(BaseModel):
    """Standardised error envelope."""

    success: bool = False
    error: str
    detail: Optional[str] = None
