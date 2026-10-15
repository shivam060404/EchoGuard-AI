"""Process and infrastructure health checks."""

from __future__ import annotations

import socket
from concurrent.futures import ThreadPoolExecutor

from fastapi import APIRouter, Request, Response

from ..config import ApiSettings


router = APIRouter(prefix="/health", tags=["health"])


def _tcp_reachable(host: str, port: int, timeout: float = 0.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


@router.get("/live")
def live() -> dict[str, str]:
    return {"status": "ok", "service": "api"}


@router.get("/ready")
def ready(request: Request, response: Response) -> dict[str, object]:
    settings: ApiSettings = request.app.state.settings
    endpoints = [
        ("kafka", settings.kafka_host, settings.kafka_port),
        ("mongo", settings.mongo_host, settings.mongo_port),
    ]
    with ThreadPoolExecutor(max_workers=2) as pool:
        checks = list(pool.map(lambda item: _tcp_reachable(item[1], item[2]), endpoints))
    dependencies = dict(zip((item[0] for item in endpoints), checks, strict=True))
    healthy = all(checks)
    if not healthy:
        response.status_code = 503
    return {"status": "ready" if healthy else "degraded", "dependencies": dependencies}
