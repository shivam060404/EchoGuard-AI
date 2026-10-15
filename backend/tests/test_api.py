from fastapi.testclient import TestClient
import pytest

from backend.api.main import app, create_app


client = TestClient(app)


def test_liveness() -> None:
    response = client.get("/health/live")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "service": "api"}


def test_readiness_reports_missing_dependencies(monkeypatch) -> None:
    monkeypatch.setattr("backend.api.routes.health._tcp_reachable", lambda *_args, **_kwargs: False)
    response = client.get("/health/ready")
    assert response.status_code == 503
    assert response.json() == {
        "status": "degraded",
        "dependencies": {"kafka": False, "mongo": False},
    }


def test_readiness_reports_available_dependencies(monkeypatch) -> None:
    monkeypatch.setattr("backend.api.routes.health._tcp_reachable", lambda *_args, **_kwargs: True)
    response = client.get("/health/ready")
    assert response.status_code == 200
    assert response.json()["status"] == "ready"


def test_api_settings_are_loaded_once_and_used_by_readiness(monkeypatch) -> None:
    monkeypatch.setenv("ECHOGUARD_KAFKA_HOST", "broker.internal")
    monkeypatch.setenv("ECHOGUARD_KAFKA_PORT", "19092")
    monkeypatch.setenv("ECHOGUARD_MONGO_HOST", "database.internal")
    monkeypatch.setenv("ECHOGUARD_MONGO_PORT", "27018")
    calls = []
    monkeypatch.setattr("backend.api.routes.health._tcp_reachable",
                        lambda host, port: calls.append((host, port)) or True)
    response = TestClient(create_app()).get("/health/ready")
    assert response.status_code == 200
    assert set(calls) == {("broker.internal", 19092), ("database.internal", 27018)}


@pytest.mark.parametrize("name,value", [
    ("ECHOGUARD_KAFKA_HOST", " "),
    ("ECHOGUARD_KAFKA_PORT", "not-a-port"),
    ("ECHOGUARD_MONGO_PORT", "65536"),
])
def test_invalid_api_settings_fail_at_startup(monkeypatch, name: str, value: str) -> None:
    monkeypatch.setenv(name, value)
    with pytest.raises(ValueError, match=name):
        create_app()

