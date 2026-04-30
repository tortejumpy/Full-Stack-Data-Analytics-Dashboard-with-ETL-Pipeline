"""
analytics.py — API route definitions for the analytics platform.

All routes delegate business logic entirely to AnalyticsService.
Routes are responsible ONLY for:
  - Parsing and validating query parameters
  - Calling the service layer
  - Returning typed responses

This clean separation means routes are trivially testable and the service
layer can be reused outside HTTP contexts (e.g. CLI scripts, scheduled jobs).
"""

from __future__ import annotations

import logging
from typing import Annotated, Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

from backend.config import settings
from backend.models.schemas import (
    CategoryResponse,
    CustomerResponse,
    HealthResponse,
    RegionResponse,
    RevenueResponse,
)
from backend.services.analytics_service import analytics_service
from backend.utils.response import error_response, success_response

logger = logging.getLogger("routes.analytics")

router = APIRouter()


# Health


@router.get(
    "/health",
    response_model=HealthResponse,
    summary="Liveness check",
    tags=["Health"],
)
async def health_check() -> HealthResponse:
    """Return the service liveness status and cache state.

    Used by load balancers and uptime monitors.
    """
    return HealthResponse(
        status="ok",
        version=settings.app_version,
        cache_loaded=analytics_service.cache_loaded,
    )


#  Revenue 


@router.get(
    "/revenue",
    response_model=RevenueResponse,
    summary="Monthly revenue trend",
    tags=["Analytics"],
)
async def get_revenue(
    start_date: Annotated[
        Optional[str],
        Query(
            description="Filter from this month (inclusive). Format: YYYY-MM",
            pattern=r"^\d{4}-\d{2}$",
            examples=["2024-01"],
        ),
    ] = None,
    end_date: Annotated[
        Optional[str],
        Query(
            description="Filter up to this month (inclusive). Format: YYYY-MM",
            pattern=r"^\d{4}-\d{2}$",
            examples=["2024-12"],
        ),
    ] = None,
) -> JSONResponse:
    """Return monthly revenue for completed orders.

    Optionally filter by ``start_date`` and ``end_date`` (both inclusive).
    Results are sorted chronologically.
    """
    try:
        data = analytics_service.get_revenue(
            start_date=start_date,
            end_date=end_date,
        )
        return JSONResponse(content=success_response(data))
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Error in /revenue: %s", exc)
        return error_response("Failed to retrieve revenue data.", str(exc))


# Top Customers 


@router.get(
    "/top-customers",
    response_model=CustomerResponse,
    summary="Top customers by spend",
    tags=["Analytics"],
)
async def get_top_customers(
    limit: Annotated[
        int,
        Query(ge=1, le=500, description="Maximum rows to return."),
    ] = 50,
    sort_by: Annotated[
        str,
        Query(
            description="Column to sort by.",
            pattern=r"^(total_spend|order_count|last_order_date|name|region)$",
        ),
    ] = "total_spend",
    order: Annotated[
        str,
        Query(description="Sort direction.", pattern=r"^(asc|desc)$"),
    ] = "desc",
    search: Annotated[
        Optional[str],
        Query(description="Case-insensitive substring search on customer name."),
    ] = None,
) -> JSONResponse:
    """Return top customers sorted by spend with churn flags.

    Supports:
      - ``limit``: rows returned (default 50, max 500)
      - ``sort_by``: column name (total_spend, order_count, etc.)
      - ``order``: asc / desc
      - ``search``: filter by customer name substring
    """
    try:
        data = analytics_service.get_top_customers(
            limit=limit,
            sort_by=sort_by,
            order=order,
            search=search,
        )
        return JSONResponse(content=success_response(data))
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Error in /top-customers: %s", exc)
        return error_response("Failed to retrieve customer data.", str(exc))


# Categories 

@router.get(
    "/categories",
    response_model=CategoryResponse,
    summary="Category performance metrics",
    tags=["Analytics"],
)
async def get_categories() -> JSONResponse:
    """Return aggregated performance metrics per product category.

    Metrics: total revenue, average order value, order count.
    Sorted by total revenue descending.
    """
    try:
        data = analytics_service.get_categories()
        return JSONResponse(content=success_response(data))
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Error in /categories: %s", exc)
        return error_response("Failed to retrieve category data.", str(exc))


# Regions 

@router.get(
    "/regions",
    response_model=RegionResponse,
    summary="Regional analytics summary",
    tags=["Analytics"],
)
async def get_regions() -> JSONResponse:
    """Return regional breakdown: customer count, orders, revenue, avg/customer.

    Sorted by total revenue descending.
    """
    try:
        data = analytics_service.get_regions()
        return JSONResponse(content=success_response(data))
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Error in /regions: %s", exc)
        return error_response("Failed to retrieve regional data.", str(exc))


# ─Cache Management 


@router.post(
    "/cache/refresh",
    summary="Force cache refresh",
    tags=["Admin"],
)
async def refresh_cache() -> dict:
    """Invalidate the in-memory DataFrame cache.

    Useful after re-running the analytics pipeline without restarting the
    server.  The next request to any data endpoint will reload from disk.
    """
    analytics_service.invalidate_cache()
    return {"success": True, "message": "Cache invalidated. Data will reload on next request."}
