"""Evaluate local retrieval recall and citation policy against labeled queries."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

from backend.knowledge.retriever import MongoHybridRetriever


CASES_PATH = Path(__file__).with_name("eval_cases.json")
FORBIDDEN = {"unapproved-payment-shortcut", "expired-payment-policy", "other-tenant-payment"}
NEGATIVE_QUERIES = [
    "automatically refund every PAY-502 report without checking capture",
    "obsolete immediate checkout retry policy PAY-502",
    "Other tenant PAY-502 procedure",
]


def evaluate(database, as_of: datetime) -> dict:
    cases = json.loads(CASES_PATH.read_text(encoding="utf-8"))
    retriever = MongoHybridRetriever(database)
    hits = 0
    checked_citations = 0
    details = []
    for case in cases:
        results = retriever.retrieve(
            tenant_id=case["tenant_id"], product=case["product"],
            query=case["query"], as_of=as_of, top_k=5,
        )
        ids = [item.document_id for item in results]
        if any(item in FORBIDDEN for item in ids):
            raise AssertionError("Forbidden knowledge reached retrieval output")
        if len({item.citation for item in results}) != len(results):
            raise AssertionError("Duplicate citation returned")
        for item in results:
            document_key = f"{case['tenant_id']}:{item.document_id}:{item.version}"
            document = database.knowledge_documents.find_one({"_id": document_key})
            if (not document or document["status"] != "approved"
                    or document["effective_from"] > as_of
                    or (document["effective_until"] is not None
                        and document["effective_until"] <= as_of)
                    or item.chunk_id not in document["chunk_ids"]):
                raise AssertionError("Citation does not resolve to an active approved version")
            checked_citations += 1
        found = case["expected_document_id"] in ids
        hits += found
        details.append({"query": case["query"], "expected": case["expected_document_id"],
                        "found": found, "result_document_ids": ids})
    for query in NEGATIVE_QUERIES:
        results = retriever.retrieve(tenant_id="demo-tenant", product="payments",
                                     query=query, as_of=as_of, top_k=10)
        if any(item.document_id in FORBIDDEN for item in results):
            raise AssertionError("Forbidden knowledge reached a negative query")
    recall = hits / len(cases)
    report = {"status": "verified" if recall >= 0.85 else "failed",
              "recall_at_5": recall, "queries": len(cases),
              "approved_citations_checked": checked_citations, "details": details}
    if report["status"] != "verified":
        raise AssertionError(json.dumps(report, sort_keys=True))
    return report


def main() -> None:
    from pymongo import MongoClient

    uri = os.getenv("MONGO_URI", "mongodb://mongo:27017")
    with MongoClient(uri, serverSelectionTimeoutMS=5000) as client:
        report = evaluate(client.echoguard, datetime.now(timezone.utc))
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
