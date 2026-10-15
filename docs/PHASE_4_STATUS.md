# Phase 4 status — knowledge and retrieval

## Built

- Immutable versioned synthetic runbooks and historical incident records with approval status, tenant, product, validity interval, source, and stable chunk IDs.
- MongoDB document/chunk registry and text index; bounded local domain-feature vectors with application-side reciprocal-rank fusion.
- Query-time approval and effective-date filtering, tenant isolation, exact error-code boost, and citation IDs.
- Five labeled retrieval queries, negative draft/expired/cross-tenant checks, Docker demo, and CI integration gate.

## Verified in this workspace

Pure knowledge tests, Python compilation, shell syntax, and Compose configuration pass. A pure vector-only check retrieves the expected approved document for all five labeled queries. The workspace cannot access the Docker socket, so the MongoDB text-index integration has not been live-verified here.

## Live gate

Run `backend/knowledge/demo.sh` in the Docker-enabled terminal. The evaluation must report `status: verified`, recall@5 of at least 0.85, and approved citations checked. The seed and evaluation are repeatable. This small synthetic baseline does not establish retrieval quality on real support data.

On 16 October 2026, the citation evaluator was corrected to handle both naive UTC BSON dates and UTC-aware dates. A regression test covers expiry boundaries. The Phase 5 Docker demo also executes this retrieval gate; the user supplied its successful live result on 16 October 2026: recall@5 = 1.0 on the five synthetic queries, alongside successful Phase 5 restart and approval verification.
