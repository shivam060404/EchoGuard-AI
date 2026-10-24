# Local incident scenario

From the repository root, with Docker Desktop running and the backend virtual
environment installed:

```sh
python -m backend.demo.local start
```

The launcher creates a new `echoguard-local-*` Compose project with its own
Kafka, MongoDB, and Spark checkpoint volumes. It allocates free loopback ports,
creates a unique tenant and one-day local credentials, seeds approved runbooks,
publishes the synthetic payment/login scenario, and waits for **250 cases, two
incidents, and the expected spike counts**. It leaves the stack running and
prints the web URL and credential-file path. Existing EchoGuard containers and
volumes are untouched.

Open the printed web URL and sign in with the `reviewer` token from the printed
credentials file. The dashboard supports this sequence:

If login says **Invalid or expired access token**, use the token value from
`data/local_demo/<printed-project>/credentials.json` with that same project's
printed web URL. The older `data/local-auth.json` belongs to the regular stack.
Paste only the value of `tokens.reviewer`, without surrounding quotes. Check
both the API and Next.js session without displaying the token:

```sh
python -m backend.demo.local check-auth echoguard-local-XXXXXXXXXX
```

If either check fails, restore the saved token registry to this project's API
and web containers without changing Kafka, MongoDB, or scenario data:

```sh
python -m backend.demo.local repair-auth echoguard-local-XXXXXXXXXX
```

After changing the API or dashboard code, rebuild just those two services
while preserving this workspace's data, ports, and credentials:

```sh
python -m backend.demo.local refresh echoguard-local-XXXXXXXXXX
```

1. Select `PAY-502`. Show the cross-channel evidence, signal counts, and
   detected incident.
2. Start an investigation. Review the possible cause, severity rationale,
   supporting guidance, and proposed response. Change the allowed action type
   if desired, then approve the proposal.
3. Click **Generate report sample**. This sends 12 new synthetic reports
   through Kafka → Spark → MongoDB and waits until the cases are projected.
   Click **Run approved action preview** promptly afterward.
4. Wait for the console's 60-second observation window. Click **Generate
   follow-up activity**. This sends three unrelated normal events through the same
   pipeline so the outcome check has proof that traffic continued. The console
   should show a **decreasing report volume**: reports for PAY-502
   occurred before the simulated action and none occurred in the after window.
   This is a demo observation, **not evidence that a ticket repaired payments**.
5. Review the result and click **Mark resolved**. Show the lifecycle
   audit. The incident is never closed automatically.

The console disables a new action until the recent complaint sample is ready.
If more than about 55 seconds pass before execution, click **Refresh sample**
again. If the observation remains `insufficient_data`, inspect the
case-writer and stream-job logs; do not present a missing sample as recovery.

For an automated acceptance check on a **disposable** demo project, run:

```sh
python -m backend.demo.verify echoguard-local-XXXXXXXXXX
```

This checks the rendered Next.js page, live update stream, session proxy,
incident detail contract, HTTP review, synthetic Kafka traffic, case
projection, one-minute outcome, and human closure sequence.
It uses PAY-502 when eligible, then AUTH-401 if PAY-502 is closed or already
has an authorized action for its current revision, and closes the selected
incident. If neither seeded revision is unused, start another isolated project
before repeating the check or the live walkthrough. An authorized action
cannot be replaced by a second workflow on the same incident revision.
This is an HTTP acceptance check, not a browser click test.

To remove only the isolated demo containers and volumes, use the exact project
name printed by the launcher:

```sh
python -m backend.demo.local stop echoguard-local-XXXXXXXXXX
```

The credential and run metadata files remain under the ignored
`data/local_demo/<project>/` directory. The generated scenario and receipt
remain under ignored `data/scenarios/`. Start again for a completely new demo.

Previously created `echoguard-faculty-*` workspaces remain supported by
`check-auth`, `repair-auth`, and `stop`; new sessions use the neutral
`echoguard-local-*` name.

The local `/demo/*` API routes are disabled unless `ECHOGUARD_LOCAL_DEMO=true`.
They require a valid tenant credential; traffic publication requires the
reviewer role and an idempotency key. Actions remain local simulations.
