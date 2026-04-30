"""
main.py — FastAPI application factory.

Responsibilities:
  - Configure CORS
  - Mount all routers under the API prefix
  - Register global exception handlers
  - Lifespan context: warm the cache on startup

Design choice: using lifespan (not deprecated @app.on_event) as per
FastAPI ≥0.93 best practices.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from backend.config import settings
from backend.routes.analytics import router as analytics_router
from backend.services.analytics_service import analytics_service
from backend.utils.response import unhandled_exception_handler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("main")


#Lifespan 


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Application lifespan: warm cache on startup, clean up on shutdown."""
    logger.info("═══ Startup: warming analytics cache …")
    try:
        # Pre-load DataFrames so first request is instant
        analytics_service._ensure_loaded()  # noqa: SLF001 — intentional warm-up
        logger.info("═══ Startup: cache warm ✓")
    except FileNotFoundError as exc:
        logger.warning(
            "═══ Startup: analytics files not found — %s. "
            "Run analyze.py first, then hit /api/cache/refresh.",
            exc,
        )
    yield
    logger.info("═══ Shutdown: analytics service cleaned up.")


#App Factory 
def create_app() -> FastAPI:
    """Create and configure the FastAPI application instance.

    Returns:
        Configured FastAPI app with middleware, routers, and handlers.
    """
    app = FastAPI(
        title=settings.app_title,
        version=settings.app_version,
        docs_url="/docs",
        redoc_url="/redoc",
        lifespan=lifespan,
    )

    #  CORS
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    #  Routers 
    app.include_router(analytics_router, prefix=settings.api_prefix)

    # Global Exception Handlers
    app.add_exception_handler(Exception, unhandled_exception_handler)

    @app.exception_handler(404)
    async def custom_404(request: Request, exc: Exception) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content={
                "success": False,
                "error": "Endpoint not found.",
                "detail": str(request.url),
            },
        )

    #  Root redirect to docs 
    @app.get("/", include_in_schema=False)
    async def root() -> JSONResponse:
        return JSONResponse(
            content={
                "message": f"{settings.app_title} v{settings.app_version}",
                "docs": "/docs",
                "health": f"{settings.api_prefix}/health",
            }
        )

    return app


# Module-level app instance used by uvicorn
app = create_app()
