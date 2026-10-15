"""Check the seeded Phase 3 case and cluster projection in MongoDB."""

from __future__ import annotations

import argparse
import json
import os
import time


def verify(mongo_uri: str, timeout: float) -> dict:
    from pymongo import MongoClient

    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
    db = client.echoguard
    deadline = time.monotonic() + timeout
    try:
        while time.monotonic() < deadline:
            cases = db.cases.count_documents({"tenant_id": "demo-tenant"})
            clusters = list(db.clusters.find({"tenant_id": "demo-tenant"},
                                             {"_id": 1, "product": 1, "error_code": 1,
                                              "peak_count": 1, "signal_ids": 1}))
            by_code = {item["error_code"]: item for item in clusters}
            incidents = db.incidents.count_documents({"tenant_id": "demo-tenant"})
            if (cases == 250 and len(clusters) == 2
                    and incidents == 2
                    and by_code.get("PAY-502", {}).get("peak_count", 0) >= 150
                    and by_code.get("AUTH-401", {}).get("peak_count", 0) >= 40
                    and db.audit_events.count_documents({"tenant_id": "demo-tenant"}) == 254):
                return {"status": "verified", "cases": cases,
                        "incidents": incidents,
                        "clusters": {code: by_code[code]["_id"] for code in sorted(by_code)}}
            time.sleep(2)
    finally:
        client.close()
    raise TimeoutError(f"Phase 3 projection incomplete: cases={cases}, clusters={len(clusters)}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mongo-uri", default=os.getenv("MONGO_URI", "mongodb://mongo:27017"))
    parser.add_argument("--timeout", type=float, default=300)
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    print(json.dumps(verify(args.mongo_uri, args.timeout), sort_keys=True))


if __name__ == "__main__":
    main()
