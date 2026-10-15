"""Versioned local domain-feature vectors for the small offline demo corpus."""

from __future__ import annotations

import hashlib
import math
import re


EMBEDDING_VERSION = "domain-hash-1.0.0"
DIMENSIONS = 256
TOKEN = re.compile(r"[a-z0-9]+(?:-[a-z0-9]+)?")
CONCEPTS = {
    "money_taken": {"charged", "charge", "debited", "deducted", "billed", "captured"},
    "payment": {"payment", "payments", "checkout", "card", "subscription", "renewal"},
    "failure": {"failed", "failure", "failing", "rejected", "declined", "error", "unsuccessful"},
    "authentication": {"login", "log-in", "signin", "sign-in", "authentication", "auth", "access"},
    "credential": {"password", "credential", "credentials", "session", "token"},
    "refund": {"refund", "reverse", "reversal", "reimburse", "reimbursement"},
}


def tokens(text: str) -> list[str]:
    return TOKEN.findall(text.casefold())[:512]


def embed(text: str) -> list[float]:
    """Deterministic concept-vector baseline; no network/model download required."""
    weights: dict[str, float] = {}
    for word in tokens(text):
        weights[f"token:{word}"] = weights.get(f"token:{word}", 0.0) + 1.0
        for concept, members in CONCEPTS.items():
            if word in members:
                weights[f"concept:{concept}"] = weights.get(f"concept:{concept}", 0.0) + 2.0
    vector = [0.0] * DIMENSIONS
    for feature, weight in weights.items():
        digest = hashlib.sha256(feature.encode("utf-8")).digest()
        index = int.from_bytes(digest[:4], "big") % DIMENSIONS
        sign = 1 if digest[4] & 1 else -1
        vector[index] += sign * (1.0 + math.log(weight))
    norm = math.sqrt(sum(value * value for value in vector))
    return [value / norm for value in vector] if norm else vector


def similarity(left: list[float], right: list[float]) -> float:
    if len(left) != DIMENSIONS or len(right) != DIMENSIONS:
        raise ValueError("Embedding dimensions do not match")
    return sum(a * b for a, b in zip(left, right, strict=True))
