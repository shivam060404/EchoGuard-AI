# Frontend

Next.js App Router application, isolated from the backend package and image. It now includes the authenticated incident review console, approval controls, simulated action view, and outcome display. Browser-driven end-to-end tests and the complete monitored outcome flow remain open; see the [implementation audit](../docs/IMPLEMENTATION_AUDIT.md).

The interface focuses on incident status, supporting evidence, review decisions, and observed report trends. It shows Kafka and MongoDB connection checks, prioritizes incidents needing review, and keeps response controls aligned with the backend's authorized workflow. The current backend uses `MockPlanningProvider` locally and does not require an external LLM API key.

```sh
npm ci
npm run dev
npm run typecheck
npm run build
```

Set `INTERNAL_API_URL` to the server-side API URL when it differs from `http://localhost:8000`. Compose supplies `http://api:8000`.

For the complete local scenario, run `python -m backend.demo.local start`
from the repository root and open the URL it prints. The demo-only traffic
controls appear after signing in to that isolated stack. They are hidden on
the ordinary stack. See [the walkthrough](../backend/demo/README.md).

To update an existing isolated workspace after frontend or API changes, run
`python -m backend.demo.local refresh <project-name>` from the repository root.
This rebuilds API and web with the workspace's saved ports and credentials.

