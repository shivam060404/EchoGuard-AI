"""FastAPI application and the central registry for API routes."""

from fastapi import FastAPI

from .config import ApiSettings
from .routes.health import router as health_router


def create_app(settings: ApiSettings | None = None) -> FastAPI:
    app = FastAPI(title="EchoGuard AI", version="0.1.0")
    app.state.settings = settings or ApiSettings.from_env()
    app.include_router(health_router)
    return app


app = create_app()

