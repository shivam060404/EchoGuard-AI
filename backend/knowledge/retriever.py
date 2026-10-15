"""Tenant-scoped hybrid text/vector retrieval with approved-version citations."""

from __future__ import annotations

import re
import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol

from backend.knowledge.embedding import DIMENSIONS, EMBEDDING_VERSION, embed, similarity, tokens


CODE = re.compile(r"\b[A-Z]{2,8}-\d{2,5}\b")
MAX_CANDIDATES = 1000


@dataclass(frozen=True)
class RetrievalHit:
    citation: str
    document_id: str
    version: str
    chunk_id: str
    title: str
    source: str
    heading: str
    text: str
    score: float
    embedding_version: str


class Retriever(Protocol):
    def retrieve(self, *, tenant_id: str, product: str, query: str,
                 as_of: datetime, top_k: int = 5) -> list[RetrievalHit]: ...


def rank_chunks(query: str, chunks: list[dict], lexical_ids: list[str],
                top_k: int) -> list[tuple[dict, float]]:
    query_vector = embed(query)
    code = CODE.search(query.upper())
    exact_code = code.group() if code else None
    vector_ranking = sorted(
        chunks,
        key=lambda chunk: (-similarity(query_vector, chunk["vector"]), chunk["_id"]),
    )
    vector_positions = {chunk["_id"]: position for position, chunk in enumerate(vector_ranking, 1)}
    lexical_positions = {chunk_id: position for position, chunk_id in enumerate(lexical_ids, 1)}
    scored = []
    for chunk in chunks:
        if chunk.get("embedding_version") != EMBEDDING_VERSION or len(chunk["vector"]) != DIMENSIONS:
            raise ValueError("Unrecognized knowledge embedding version or dimension")
        score = 1.0 / (60 + vector_positions[chunk["_id"]])
        if chunk["_id"] in lexical_positions:
            score += 1.0 / (60 + lexical_positions[chunk["_id"]])
        if exact_code and exact_code in chunk["error_codes"]:
            score += 1.0 / 30.0
        scored.append((chunk, score))
    return sorted(scored, key=lambda item: (-item[1], item[0]["_id"]))[:top_k]


class MongoHybridRetriever:
    def __init__(self, database):
        self.db = database

    def retrieve(self, *, tenant_id: str, product: str, query: str,
                 as_of: datetime, top_k: int = 5) -> list[RetrievalHit]:
        if not tenant_id or not product or not query.strip():
            raise ValueError("Tenant, product, and query are required")
        if not tokens(query):
            raise ValueError("Query needs searchable terms")
        if as_of.tzinfo is None or as_of.utcoffset().total_seconds() != 0:
            raise ValueError("as_of must be UTC")
        if not 1 <= top_k <= 10:
            raise ValueError("top_k must be between 1 and 10")
        at = as_of.astimezone(timezone.utc)
        approved = list(self.db.knowledge_documents.find({
            "tenant_id": tenant_id, "product": product, "status": "approved",
            "effective_from": {"$lte": at},
            "$or": [{"effective_until": None}, {"effective_until": {"$gt": at}}],
            "embedding_version": EMBEDDING_VERSION,
        }).limit(MAX_CANDIDATES + 1))
        if len(approved) > MAX_CANDIDATES:
            raise RuntimeError("Local knowledge corpus exceeds safe scan bound")
        if not approved:
            return []
        registry = {document["_id"]: document for document in approved}
        scope = {"tenant_id": tenant_id, "product": product,
                 "document_key": {"$in": list(registry)}}
        chunks = list(self.db.knowledge_chunks.find(scope).limit(MAX_CANDIDATES + 1))
        if len(chunks) > MAX_CANDIDATES:
            raise RuntimeError("Local knowledge chunks exceed safe scan bound")
        verified_chunks = []
        for chunk in chunks:
            if chunk["_id"] not in registry[chunk["document_key"]]["chunk_ids"]:
                continue
            expected_id = hashlib.sha256(
                (f"{chunk['document_key']}\x1f{chunk['section_id']}\x1f"
                 f"{chunk['heading']}\x1f{chunk['text']}").encode("utf-8")
            ).hexdigest()[:32]
            if chunk["_id"] != f"chunk-{expected_id}":
                raise ValueError("Knowledge chunk content does not match its immutable ID")
            expected_vector = embed(
                f"{registry[chunk['document_key']]['title']} {chunk['heading']} {chunk['text']}"
            )
            if chunk["vector"] != expected_vector:
                raise ValueError("Knowledge chunk embedding does not match its versioned content")
            verified_chunks.append(chunk)
        chunks = verified_chunks
        if not chunks:
            return []
        search = " ".join(tokens(query)[:32]).replace("-", " ")
        text_hits = []
        if search:
            text_hits = list(self.db.knowledge_chunks.find(
                {**scope, "$text": {"$search": search}},
                {"_id": 1, "text_score": {"$meta": "textScore"}},
            ).limit(MAX_CANDIDATES))
        text_hits.sort(key=lambda hit: (-hit.get("text_score", 0), hit["_id"]))
        ranked = rank_chunks(query, chunks, [hit["_id"] for hit in text_hits], top_k)
        results = []
        for chunk, score in ranked:
            document = registry[chunk["document_key"]]
            results.append(RetrievalHit(
                citation=(f"knowledge://{tenant_id}/{document['document_id']}"
                          f"/{document['version']}/{chunk['_id']}"),
                document_id=document["document_id"], version=document["version"],
                chunk_id=chunk["_id"], title=document["title"], source=document["source"],
                heading=chunk["heading"], text=chunk["text"], score=score,
                embedding_version=EMBEDDING_VERSION,
            ))
        return results
