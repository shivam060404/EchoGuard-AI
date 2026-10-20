"""Idempotent MongoDB projection of cases, signals, clusters, and audit events."""

from __future__ import annotations

from datetime import datetime, timezone

from backend.worker.correlation import case_record, signal_record


class CaseStore:
    def __init__(self, database):
        self.db = database

    def ensure_indexes(self) -> None:
        self.db.cases.create_index([("tenant_id", 1), ("event_id", 1)], unique=True)
        self.db.cases.create_index([("tenant_id", 1), ("event_time", -1)])
        self.db.cases.create_index([("tenant_id", 1), ("product", 1), ("error_code", 1), ("event_time", 1)])
        self.db.incidents.create_index([("tenant_id", 1), ("_id", 1)])
        self.db.signals.create_index([("tenant_id", 1), ("cluster_id", 1)])
        self.db.clusters.create_index([("tenant_id", 1), ("status", 1), ("last_seen", -1)])
        self.db.incidents.create_index([("tenant_id", 1), ("status", 1), ("last_seen", -1)])
        self.db.audit_events.create_index([("tenant_id", 1), ("subject_id", 1)])

    def write_case(self, event: dict) -> str:
        case = case_record(event)
        self.db.cases.update_one({"_id": case["_id"]}, {"$setOnInsert": case}, upsert=True)
        self.db.audit_events.update_one(
            {"_id": f"audit-{case['_id']}-created"},
            {"$setOnInsert": {
                "_id": f"audit-{case['_id']}-created", "tenant_id": case["tenant_id"],
                "subject_id": case["_id"], "kind": "case_created",
                "event_time": case["event_time"],
            }}, upsert=True,
        )
        return case["_id"]

    def write_signal(self, event: dict) -> str:
        signal = signal_record(event)
        cluster_id = signal["cluster_id"]
        # Store the signal first. A retry repairs a partial cluster/audit projection.
        self.db.signals.update_one(
            {"_id": signal["_id"]},
            {"$setOnInsert": {key: value for key, value in signal.items()
                              if key not in {"current_count", "distinct_customers", "sample_event_ids"}},
             "$max": {"current_count": signal["current_count"],
                      "distinct_customers": signal["distinct_customers"]},
             "$addToSet": {"sample_event_ids": {"$each": signal["sample_event_ids"]}}},
            upsert=True,
        )
        self.db.clusters.update_one(
            {"_id": cluster_id},
            {"$setOnInsert": {
                "_id": cluster_id, "tenant_id": signal["tenant_id"],
                "product": signal["product"], "error_code": signal["error_code"],
                "status": "candidate", "created_at": datetime.now(timezone.utc),
                "version": 1,
            },
             "$addToSet": {"signal_ids": signal["_id"]},
             "$max": {"last_seen": signal["window_end"],
                      "peak_count": signal["current_count"]}},
            upsert=True,
        )
        self.db.audit_events.update_one(
            {"_id": f"audit-{cluster_id}-created"},
            {"$setOnInsert": {
                "_id": f"audit-{cluster_id}-created", "tenant_id": signal["tenant_id"],
                "subject_id": cluster_id, "kind": "cluster_created",
                "event_time": signal["window_start"],
            }}, upsert=True,
        )
        incident_id = f"incident-{cluster_id.removeprefix('cluster-')}"
        self.db.incidents.update_one(
            {"_id": incident_id},
            {"$setOnInsert": {
                "_id": incident_id, "tenant_id": signal["tenant_id"],
                "cluster_id": cluster_id, "product": signal["product"],
                "error_code": signal["error_code"], "status": "candidate",
                "version": 1, "created_at": datetime.now(timezone.utc),
            },
             "$addToSet": {"signal_ids": signal["_id"]},
             "$max": {"last_seen": signal["window_end"],
                      "peak_count": signal["current_count"]}},
            upsert=True,
        )
        self.db.audit_events.update_one(
            {"_id": f"audit-{incident_id}-created"},
            {"$setOnInsert": {
                "_id": f"audit-{incident_id}-created", "tenant_id": signal["tenant_id"],
                "subject_id": incident_id, "kind": "incident_created",
                "event_time": signal["window_start"],
            }}, upsert=True,
        )
        self.reopen_incident(signal["tenant_id"], incident_id, signal["_id"], signal["window_end"])
        return cluster_id

    def project_lifecycle_audit(self, tenant_id: str, incident_id: str) -> None:
        """Repair the queryable ledger from the authoritative atomic incident journal."""
        incident = self.db.incidents.find_one({"_id": incident_id, "tenant_id": tenant_id})
        for event in (incident or {}).get("lifecycle_audit", []):
            self.db.audit_events.update_one({"_id": event["_id"]},
                                            {"$setOnInsert": event}, upsert=True)

    def reopen_incident(self, tenant_id: str, incident_id: str, signal_id: str,
                        window_end: datetime) -> bool:
        event = {"_id": f"audit-{incident_id}-reopened-{signal_id}",
                 "tenant_id": tenant_id, "subject_id": incident_id,
                 "kind": "incident_reopened", "event_time": window_end}
        result = self.db.incidents.update_one(
            {"_id": incident_id, "tenant_id": tenant_id, "status": "resolved",
             "resolved_at": {"$lt": window_end}},
            {"$set": {"status": "reopened", "reopened_by_signal_id": signal_id},
             "$inc": {"version": 1}, "$push": {"lifecycle_audit": event}},
        )
        # Always project, including when replay observes an already-applied transition.
        self.project_lifecycle_audit(tenant_id, incident_id)
        return bool(result.modified_count)

    def resolve_incident(self, tenant_id: str, incident_id: str,
                         expected_version: int, resolved_at: datetime) -> bool:
        """Compare-and-set close with an atomic journal and replayable ledger."""
        if resolved_at.tzinfo is None or resolved_at.utcoffset().total_seconds() != 0:
            raise ValueError("resolved_at must be UTC")
        event = {"_id": f"audit-{incident_id}-resolved-v{expected_version}",
                 "tenant_id": tenant_id, "subject_id": incident_id,
                 "kind": "incident_resolved", "event_time": resolved_at}
        result = self.db.incidents.update_one(
            {"_id": incident_id, "tenant_id": tenant_id,
             "version": expected_version, "status": {"$in": ["candidate", "reopened"]}},
            {"$set": {"status": "resolved", "resolved_at": resolved_at},
             "$inc": {"version": 1}, "$push": {"lifecycle_audit": event}},
        )
        self.project_lifecycle_audit(tenant_id, incident_id)
        return bool(result.modified_count)
