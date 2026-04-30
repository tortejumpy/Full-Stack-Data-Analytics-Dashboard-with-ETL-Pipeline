"""
response.py — Standardised JSON response helpers.

Centralising response construction ensures every endpoint returns a
consistent envelope, making it trivial to add fields (e.g., request_id,
pagination metadata) project-wide without touching individual routes.
"""

from __future__ import annotations

from typing import Any

from fastapi import Request
from fastapi.responses import JSONResponse


def success_response(
    data: list[Any],
    status_code: int = 200,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a successful API response envelope.

    Args:
        data: Serialised payload (list of dicts/models).
        status_code: HTTP status code (default 200).
        extra: Optional extra keys to merge into the envelope.

    Returns:
        Dict suitable for FastAPI to return as JSON.
    """
    envelope: dict[str, Any] = {
        "success": True,
        "count": len(data),
        "data": data,
    }
    if extra:
        envelope.update(extra)
    return envelope


def error_response(
    error: str,
    detail: str | None = None,
    status_code: int = 500,
) -> JSONResponse:
    """Build a structured error JSONResponse.

    Args:
        error: Short, human-readable error message.
        detail: Optional extended detail string (stack trace, hint, etc.).
        status_code: HTTP status code.

    Returns:
        FastAPI JSONResponse with the error envelope.
    """
    body: dict[str, Any] = {"success": False, "error": error}
    if detail:
        body["detail"] = detail
    return JSONResponse(status_code=status_code, content=body)


async def not_found_handler(request: Request, exc: Exception) -> JSONResponse:
    """Global 404 handler returning structured JSON instead of HTML."""
    return error_response(
        error="Resource not found.",
        detail=str(request.url),
        status_code=404,
    )


async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Global 500 handler — prevents leaking stack traces in production."""
    return error_response(
        error="An unexpected server error occurred.",
        detail=type(exc).__name__,
        status_code=500,
    )
