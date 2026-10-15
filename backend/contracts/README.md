# Event contracts

The schemas in `schemas/` are Draft 2020-12 JSON Schema. Topic and schema major versions travel together; `schema_version` is `1.0.0` for the first fixtures. Events have stable IDs and UTC timestamps. Unknown fields are rejected so incompatible producers fail visibly. Phase 1 validates producer payloads before publishing; Phase 2 will validate independently on stream consumption.

Compatibility policy: additive optional fields may increase the minor version. Removing a field, changing its type/meaning, or adding a required field needs a new major topic and a migration. Keep positive fixtures for old versions so `python3 -m backend.contracts.check_contracts` can check that a newer schema still accepts old events within one major version.

Partition keys: raw and enriched events use `tenant_id:customer_id`; signals use `tenant_id:signal_key`. A producer must preserve `event_id` across retries. Invalid ingest payloads are stored in `ingest.dlq.v1` with a source record ID, safe reason code, SHA-256 digest, and base64 payload for controlled redrive. The local DLQ contains synthetic data only.

