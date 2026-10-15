"""Immutable approved-document versions and citation identity."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone

from backend.knowledge.embedding import EMBEDDING_VERSION, embed


def utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.utcoffset() is None or parsed.utcoffset().total_seconds() != 0:
        raise ValueError("Knowledge timestamps must be UTC")
    return parsed.astimezone(timezone.utc)


def build_document(source: dict) -> tuple[dict, list[dict]]:
    required = {"document_id", "version", "tenant_id", "product", "title", "source",
                "status", "effective_from", "effective_until", "error_codes", "sections"}
    if set(source) != required:
        raise ValueError("Knowledge document fields differ from the approved manifest")
    if source["status"] not in {"approved", "draft", "revoked"}:
        raise ValueError("Invalid approval status")
    for field in ("document_id", "version", "tenant_id", "product", "title", "source"):
        if not isinstance(source[field], str) or not source[field].strip():
            raise ValueError(f"{field} must be nonempty")
    start = utc(source["effective_from"])
    end = utc(source["effective_until"]) if source["effective_until"] else None
    if end is not None and end <= start:
        raise ValueError("Knowledge effective interval must be positive")
    if not isinstance(source["error_codes"], list) or not all(
        isinstance(code, str) and code for code in source["error_codes"]
    ):
        raise ValueError("Invalid error codes")
    if not isinstance(source["sections"], list) or not source["sections"]:
        raise ValueError("A document needs at least one section")
    key = f"{source['tenant_id']}:{source['document_id']}:{source['version']}"
    digest = hashlib.sha256(json.dumps(source, sort_keys=True, separators=(",", ":"),
                                      ensure_ascii=False).encode("utf-8")).hexdigest()
    document = {
        "_id": key, "document_id": source["document_id"], "version": source["version"],
        "tenant_id": source["tenant_id"], "product": source["product"],
        "title": source["title"], "source": source["source"],
        "status": source["status"], "effective_from": start,
        "effective_until": end, "error_codes": source["error_codes"],
        "sha256": digest, "embedding_version": EMBEDDING_VERSION,
    }
    chunks = []
    seen_sections = set()
    for section in source["sections"]:
        if set(section) != {"section_id", "heading", "text"}:
            raise ValueError("Invalid section fields")
        section_id = section["section_id"]
        if not isinstance(section_id, str) or not section_id or section_id in seen_sections:
            raise ValueError("Duplicate or empty section ID")
        seen_sections.add(section_id)
        if not isinstance(section["heading"], str) or not section["heading"].strip():
            raise ValueError("Section heading must be nonempty")
        text = section["text"]
        if not isinstance(text, str) or not 20 <= len(text) <= 3000:
            raise ValueError("Section text must be 20–3000 characters")
        chunk_id = hashlib.sha256(
            f"{key}\x1f{section_id}\x1f{section['heading']}\x1f{text}".encode("utf-8")
        ).hexdigest()[:32]
        chunks.append({
            "_id": f"chunk-{chunk_id}", "document_key": key,
            "document_id": source["document_id"], "version": source["version"],
            "tenant_id": source["tenant_id"], "product": source["product"],
            "section_id": section_id, "heading": section["heading"], "text": text,
            "error_codes": source["error_codes"], "embedding_version": EMBEDDING_VERSION,
            "vector": embed(f"{source['title']} {section['heading']} {text}"),
        })
    document["chunk_ids"] = [chunk["_id"] for chunk in chunks]
    return document, chunks
