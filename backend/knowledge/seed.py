"""Load immutable, synthetic knowledge versions into MongoDB."""

from __future__ import annotations

import json
import os
import argparse
from pathlib import Path

from backend.knowledge.model import build_document


SEED_PATH = Path(__file__).with_name("seed_documents.json")


def load_sources(path: Path = SEED_PATH) -> list[dict]:
    sources = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(sources, list) or not sources:
        raise ValueError("Knowledge seed must be a nonempty list")
    return sources


def sources_for_tenant(tenant: str, sources: list[dict] | None = None) -> list[dict]:
    if not tenant or len(tenant) > 128:
        raise ValueError('Tenant must contain 1–128 characters')
    return [dict(source, tenant_id=tenant) if source['tenant_id'] == 'demo-tenant'
            else source for source in (sources if sources is not None else load_sources())]


def seed(database, sources: list[dict]) -> dict[str, int]:
    database.knowledge_documents.create_index(
        [("tenant_id", 1), ("product", 1), ("status", 1), ("effective_from", 1)]
    )
    database.knowledge_chunks.create_index(
        [("text", "text"), ("heading", "text"), ("error_codes", "text")],
        name="knowledge_text_v1",
    )
    database.knowledge_chunks.create_index(
        [("tenant_id", 1), ("product", 1), ("document_key", 1)]
    )
    documents = 0
    chunks_count = 0
    for source in sources:
        document, chunks = build_document(source)
        existing = database.knowledge_documents.find_one({"_id": document["_id"]})
        if existing and existing["sha256"] != document["sha256"]:
            raise ValueError(f"Immutable document version changed: {document['_id']}")
        if existing and (existing.get("embedding_version") != document["embedding_version"]
                         or existing.get("chunk_ids") != document["chunk_ids"]):
            raise ValueError(f"Knowledge index version changed: {document['_id']}")
        database.knowledge_documents.update_one(
            {"_id": document["_id"]}, {"$setOnInsert": document}, upsert=True
        )
        for chunk in chunks:
            previous = database.knowledge_chunks.find_one({"_id": chunk["_id"]})
            if previous and any(previous.get(field) != chunk[field]
                                for field in ("document_key", "section_id", "heading", "text",
                                              "embedding_version", "vector")):
                raise ValueError(f"Immutable knowledge chunk changed: {chunk['_id']}")
            database.knowledge_chunks.update_one(
                {"_id": chunk["_id"]}, {"$setOnInsert": chunk}, upsert=True
            )
        documents += 1
        chunks_count += len(chunks)
    return {"documents": documents, "chunks": chunks_count}


def main() -> None:
    from pymongo import MongoClient

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--tenant', default='demo-tenant')
    args = parser.parse_args()
    if not args.tenant or len(args.tenant) > 128:
        parser.error('--tenant must contain 1–128 characters')
    sources = sources_for_tenant(args.tenant)
    uri = os.getenv("MONGO_URI", "mongodb://mongo:27017")
    with MongoClient(uri, serverSelectionTimeoutMS=5000, tz_aware=True) as client:
        counts = seed(client.echoguard, sources)
    print(json.dumps({"status": "seeded", **counts}, sort_keys=True))


if __name__ == "__main__":
    main()
