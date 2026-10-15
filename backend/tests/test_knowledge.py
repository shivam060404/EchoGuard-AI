"""Knowledge identity, offline retrieval, and exclusion invariants."""

from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from backend.knowledge.embedding import DIMENSIONS, embed, similarity
from backend.knowledge.model import build_document
from backend.knowledge.retriever import rank_chunks


KNOWLEDGE = Path(__file__).parents[1] / "knowledge"


def sources() -> list[dict]:
    return json.loads((KNOWLEDGE / "seed_documents.json").read_text())


def test_document_and_chunk_identity_is_replay_stable() -> None:
    document, chunks = build_document(sources()[0])
    assert (document, chunks) == build_document(copy.deepcopy(sources()[0]))
    assert document["chunk_ids"] == [chunk["_id"] for chunk in chunks]
    assert all(chunk["document_key"] == document["_id"] for chunk in chunks)


def test_versioned_embedding_is_normalized_and_paraphrase_related() -> None:
    query = embed("My card was debited but renewal did not complete")
    payment = embed("Payment failed after the customer was charged")
    identity = embed("Sign in rejected even with the correct password")
    assert len(query) == DIMENSIONS
    assert similarity(query, payment) > similarity(query, identity)


def test_ranked_seed_queries_recall_relevant_approved_document() -> None:
    catalog = [build_document(source) for source in sources()]
    cases = json.loads((KNOWLEDGE / "eval_cases.json").read_text())
    for case in cases:
        chunks = [chunk for document, document_chunks in catalog
                  if document["tenant_id"] == case["tenant_id"]
                  and document["product"] == case["product"]
                  and document["status"] == "approved"
                  and document["effective_until"] is None
                  for chunk in document_chunks]
        result = rank_chunks(case["query"], chunks, [], 5)
        assert case["expected_document_id"] in [chunk["document_id"] for chunk, _ in result]


def test_seed_rejects_duplicate_sections_and_bad_effective_range() -> None:
    source = sources()[0]
    bad_sections = copy.deepcopy(source)
    bad_sections["sections"].append(copy.deepcopy(bad_sections["sections"][0]))
    with pytest.raises(ValueError, match="section ID"):
        build_document(bad_sections)
    bad_time = {**source, "effective_until": "2025-01-01T00:00:00Z"}
    with pytest.raises(ValueError, match="effective interval"):
        build_document(bad_time)


def test_effective_dates_accept_bson_naive_utc_and_exclude_expiry_boundary():
    from datetime import datetime, timezone, timedelta
    from backend.knowledge.model import active_at
    at = datetime(2026, 10, 16, tzinfo=timezone.utc)
    document = {"status": "approved", "effective_from": at.replace(tzinfo=None),
                "effective_until": (at + timedelta(days=1)).replace(tzinfo=None)}
    assert active_at(document, at)
    assert not active_at(document, at + timedelta(days=1))
    assert not active_at({**document, "status": "revoked"}, at)
    with pytest.raises(ValueError):
        active_at(document, at.replace(tzinfo=None))
