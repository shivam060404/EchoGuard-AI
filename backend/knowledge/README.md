# Phase 4: approved knowledge and retrieval

Run `backend/knowledge/demo.sh` from the repository root. It starts MongoDB, builds the isolated knowledge tool image, seeds six synthetic document versions, and evaluates five labeled queries. The command can be repeated: the same document and chunk versions are upserted, while a changed version digest is rejected.

Documents have explicit tenant, product, approval status, validity dates, source URI, version, and section IDs. Each section becomes a stable chunk with a content-derived ID and a versioned 256-dimensional vector. A MongoDB text index supplies lexical candidates. The retriever fuses text and local vector ranks and boosts exact error codes. Returned citations include tenant, document ID, version, and chunk ID. The document registry is checked at query time, so draft, revoked, expired, and other-tenant material is excluded even if chunks remain stored.

The vector is a deterministic domain-feature baseline that recognizes a small set of payment and authentication paraphrases. It is offline and has no external model dependency, but it is not a general semantic embedding model. The evaluation reports recall@5 on the seeded synthetic queries and checks that every returned citation resolves to an active approved version. A broader held-out corpus and a production embedding adapter are later gates.

To inspect the result manually:

~~~sh
docker compose --profile knowledge run --rm knowledge-tool python -m backend.knowledge.evaluate
~~~
