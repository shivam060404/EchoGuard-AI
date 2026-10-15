"""Tenant and role are derived exclusively from server-configured token records."""
import hashlib
import hmac
import json
import os
from datetime import datetime, timezone

from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from backend.workflow.service import Principal

bearer = HTTPBearer(auto_error=False)


def authenticate(token: str, registry: str | None = None) -> Principal:
    try:
        records = json.loads(registry if registry is not None else os.getenv("ECHOGUARD_AUTH_JSON", "{}"))
        hashed = hashlib.sha256(token.encode()).hexdigest()
        for candidate, record in records.items():
            if hmac.compare_digest(candidate, hashed):
                expires = datetime.fromisoformat(record["expires_at"])
                if expires.tzinfo is None or expires <= datetime.now(timezone.utc):
                    break
                if record["role"] not in {"operator", "reviewer"}:
                    break
                if not record["tenant_id"] or not record["actor_id"]:
                    break
                return Principal(record["tenant_id"], record["actor_id"], record["role"])
    except (ValueError, TypeError, KeyError, AttributeError):
        raise HTTPException(503, "Authentication configuration unavailable") from None
    raise HTTPException(401, "Invalid or expired access token", headers={"WWW-Authenticate": "Bearer"})


def principal(credentials: HTTPAuthorizationCredentials | None = Depends(bearer)) -> Principal:
    if credentials is None:
        raise HTTPException(401, "Access token required", headers={"WWW-Authenticate": "Bearer"})
    return authenticate(credentials.credentials)
