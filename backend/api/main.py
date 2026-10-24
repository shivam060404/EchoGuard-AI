"""FastAPI application and the central registry for API routes."""

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from .config import ApiSettings
from .routes.health import router as health_router
from .routes.console import router as console_router
from .routes.demo import router as demo_router
from backend.workflow.store import Conflict


def create_app(settings: ApiSettings | None = None) -> FastAPI:
    app = FastAPI(title="EchoGuard AI", version="0.1.0")
    app.state.settings = settings or ApiSettings.from_env()
    app.include_router(health_router)
    app.include_router(console_router)
    app.include_router(demo_router)

    @app.exception_handler(Conflict)
    async def conflict(_request, exc):
        return JSONResponse(status_code=409, content={"detail": str(exc)})

    @app.exception_handler(LookupError)
    async def missing(_request, _exc):
        return JSONResponse(status_code=404, content={"detail": "Resource not found in tenant"})

    @app.exception_handler(PermissionError)
    async def forbidden(_request, _exc):
        return JSONResponse(status_code=403, content={"detail": "Role does not permit this operation"})
    return app


app = create_app()

