"use client";
import { FormEvent, useCallback, useEffect, useRef, useState } from "react";

type Actor = { tenant_id: string; actor_id: string; role: string };
type Incident = { _id?: string; incident_id?: string; product: string; error_code: string | null; peak_count: number; status: string; version: number };
type Step = { citation: string; quote: string };
type State = { run_id: string; status: string; reason?: string; proposal_hash?: string; expires_at?: string;
  incident: Incident; proposal?: { hypothesis: string; confidence: number; action: string; steps: Step[] };
  knowledge?: { citation: string; title: string; version: string }[];
  usage?: { model: string; input_tokens: number; output_tokens: number; cost_usd: number } };
type Action = { status: string; result?: { reference: string; simulated: boolean }; completed_at?: string };
type Audit = { status: string; actor?: string; decision?: { actor_id: string; decision: string }; reason?: string };
type Detail = { incident: Incident; incident_hash: string; evidence: { event_id: string; normalized_text: string }[];
  workflow: { _id: string; view?: State; audit: Audit[]; action?: Action } | null };
type Outcome = { status: string; reason: string; before_count?: number; after_count?: number };

async function api<T>(path: string, body?: object, key?: string): Promise<T> {
  const result = await fetch(`/api/proxy/${path}`, { cache: "no-store", method: body ? "POST" : "GET",
    headers: { "Content-Type": "application/json", ...(key ? { "Idempotency-Key": key } : {}) },
    body: body ? JSON.stringify(body) : undefined });
  const data = await result.json();
  if (!result.ok) throw new Error(typeof data.detail === "string" ? data.detail : "The request could not be completed");
  return data;
}

export default function Console() {
  const [actor, setActor] = useState<Actor | null>(null);
  const [token, setToken] = useState("");
  const [incidents, setIncidents] = useState<Incident[]>([]);
  const [cursor, setCursor] = useState<string | null>(null);
  const [selected, setSelected] = useState("");
  const [detail, setDetail] = useState<Detail | null>(null);
  const [outcome, setOutcome] = useState<Outcome | null>(null);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const [connected, setConnected] = useState(false);
  const [updated, setUpdated] = useState("");
  const selectedRef = useRef("");
  const visibleCount = useRef(50);
  const refreshSequence = useRef(0);
  const requestKeys = useRef<Record<string, string>>({});

  const refresh = useCallback(async () => {
    const sequence = ++refreshSequence.current;
    const list = await api<{ items: Incident[]; next_cursor: string | null }>("incidents");
    while (list.next_cursor && list.items.length < visibleCount.current) {
      const page = await api<{items: Incident[]; next_cursor: string | null}>(`incidents?after=${encodeURIComponent(list.next_cursor)}`);
      if (sequence !== refreshSequence.current) return;
      list.items.push(...page.items); list.next_cursor = page.next_cursor;
    }
    if (sequence !== refreshSequence.current) return;
    setIncidents(list.items); setCursor(list.next_cursor);
    const id = selectedRef.current || list.items[0]?._id || "";
    selectedRef.current = id; setSelected(id);
    if (id) {
      const value = await api<Detail>(`incidents/${id}`);
      if (sequence !== refreshSequence.current || id !== selectedRef.current) return;
      setDetail(value);
      if (value.workflow?.action) {
        const trend = await api<Outcome>(`workflows/${value.workflow._id}/outcome`);
        if (sequence === refreshSequence.current) setOutcome(trend);
      } else setOutcome(null);
    } else { setDetail(null); setOutcome(null); }
    setUpdated(new Date().toLocaleTimeString());
  }, []);

  useEffect(() => { api<Actor>("me").then(setActor).catch(() => {}); }, []);
  useEffect(() => {
    if (!actor) return;
    refresh().catch(e => setError(e.message));
    const stream = new EventSource("/api/proxy/events");
    stream.onopen = () => setConnected(true);
    stream.onerror = () => setConnected(false);
    stream.addEventListener("refresh", () => { refresh().catch(e => setError(e.message)); });
    return () => { stream.close(); setConnected(false); refreshSequence.current++; };
  }, [actor, refresh]);

  async function login(event: FormEvent) {
    event.preventDefault(); setBusy(true); setError("");
    try {
      const response = await fetch("/api/session", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ token }) });
      const body = await response.json();
      if (!response.ok) throw new Error(body.detail || "Sign in failed");
      setActor(body); setToken("");
    } catch (e) { setError((e as Error).message); } finally { setBusy(false); }
  }

  async function mutate(kind: "start" | "approve" | "reject" | "execute" | "resolve" | "edit") {
    if (!detail || busy) return;
    const run = detail.workflow?.view;
    setBusy(true); setError("");
    const identity = `${selected}:${run?.run_id ?? "new"}:${kind}`;
    const key = requestKeys.current[identity] ?? crypto.randomUUID();
    requestKeys.current[identity] = key;
    try {
      if (kind === "start") {
        await api(`incidents/${selected}/workflows`, { expected_version: detail.incident.version }, key);
      } else if (kind === "resolve") {
        await api(`incidents/${selected}/resolve`, { expected_version: detail.incident.version, incident_hash: detail.incident_hash }, key);
      } else if (run) {
        await api(`workflows/${run.run_id}/${kind === "execute" ? "action" : kind === "edit" ? "proposal" : "decision"}`, {
          expected_version: run.incident.version, proposal_hash: run.proposal_hash,
          ...(kind === "edit" ? { action: run.proposal?.action === "create_ticket" ? "post_internal_notification" : "create_ticket", step_indices: run.proposal?.steps.map((_, index) => index) } : kind === "execute" ? {} : { decision: kind }),
        }, key);
      }
      delete requestKeys.current[identity];
      await refresh();
    } catch (e) { setError((e as Error).message); } finally { setBusy(false); }
  }

  const run = detail?.workflow?.view;
  const action = detail?.workflow?.action;
  const reviewer = actor?.role === "reviewer";
  return <main className="shell console-shell">
    <header className="topbar"><div className="brand"><span className="brand-mark">E</span>EchoGuard <strong>AI</strong></div>
      <div className="toolbar"><span className="phase">Simulated actions</span>{actor && <button className="quiet" onClick={async () => {
        const response = await fetch("/api/session", { method: "DELETE" }).catch(() => null);
        if (!response?.ok) { setError("Sign out failed. Please retry."); return; }
        visibleCount.current = 50; setActor(null); setDetail(null); setIncidents([]);
        selectedRef.current = ""; setSelected(""); setOutcome(null); setError(""); requestKeys.current = {};
      }}>Sign out</button>}</div></header>
    {!actor ? <section className="panel login"><p className="eyebrow">REVIEW CONSOLE</p><h1>See the evidence.<br/>Coordinate the response.</h1>
      <p>Sign in with your operator or reviewer access token.</p><form onSubmit={login}>
        <label htmlFor="token">Access token</label><input id="token" type="password" autoComplete="off" required value={token} onChange={e => setToken(e.target.value)} />
        <button disabled={busy || !token}>{busy ? "Signing in…" : "Open console"}</button></form>
      {error && <p role="alert" className="error">{error}</p>}</section> : <>
      <section className="console-heading"><div><p className="eyebrow">INCIDENT OPERATIONS</p><h1>Review & respond</h1><p>{actor.tenant_id} · {actor.actor_id} · {actor.role}</p></div>
        <div><span className={`status ${connected ? "status-ready" : "status-degraded"}`}>{connected ? "Live updates" : "Reconnecting"}</span><p className="small">Updated {updated || "—"}</p></div></section>
      {error && <div role="alert" className="error">{error} <button className="quiet" onClick={() => refresh().then(() => setError("")).catch(e => setError(e.message))}>Refresh</button></div>}
      <div className="workspace"><aside className="panel incident-list"><div className="panel-heading"><h2>Incidents</h2><span>{incidents.length}</span></div>
        {!incidents.length && <p>No incidents yet. New candidates will appear here.</p>}
        {incidents.map(item => <button key={item._id} className={`incident-card ${selected === item._id ? "selected" : ""}`} onClick={() => {
          if (busy) return; selectedRef.current = item._id!; setSelected(item._id!); setDetail(null);
          refresh().catch(e => setError(e.message));
        }}><span className="row"><strong>{item.error_code || item.product}</strong><span className="badge">{item.status}</span></span>
          <span>{item.product}</span><span className="small">{item.peak_count} peak reports</span></button>)}{cursor && <button className="quiet" onClick={async () => {
          try { const page = await api<{items: Incident[]; next_cursor: string | null}>(`incidents?after=${encodeURIComponent(cursor)}`);
            visibleCount.current += page.items.length;
            setIncidents(previous => [...previous, ...page.items.filter(item => !previous.some(old => old._id === item._id))]); setCursor(page.next_cursor);
          } catch (e) { setError((e as Error).message); }
        }}>Load more incidents</button>}</aside>
        <section className="detail">{detail ? <>
          <section className="panel"><div className="panel-heading"><div><p className="eyebrow">{detail.incident.product}</p><h2>{detail.incident.error_code || "Shared issue"}</h2></div><span className="badge">{detail.incident.status}</span></div>
            <p>{detail.incident.peak_count} reports at peak · incident version {detail.incident.version}</p>
            <div className="toolbar"><button disabled={busy || detail.incident.status === "resolved"} onClick={() => mutate("start")}>{run ? "Start fresh investigation" : "Investigate incident"}</button>
            {reviewer && action?.status === "succeeded" && detail.incident.status !== "resolved" && <button className="quiet" disabled={busy} onClick={() => mutate("resolve")}>Mark resolved after review</button>}</div></section>
          {run && <section className="panel"><div className="panel-heading"><h2>Recommended response</h2><span className="badge">{run.status.replaceAll("_", " ")}</span></div>
            {run.reason && <p className="error">Review required: {run.reason.replaceAll("_", " ")}</p>}
            {run.proposal?.steps && <><p>{run.proposal.hypothesis}</p><p className="small">Mock provider confidence {Math.round(run.proposal.confidence * 100)}% · Root cause unconfirmed</p>
              <ol className="steps">{run.proposal.steps.map((step, index) => <li key={`${step.citation}-${index}`}><p>{step.quote}</p><span className="citation">{run.knowledge?.find(k => k.citation === step.citation)?.title || "Approved source"} · {step.citation}</span></li>)}</ol>
              <p>Proposed action: <strong>{run.proposal.action.replaceAll("_", " ")} (simulated)</strong></p>
              <details><summary>Review identity</summary><p className="citation">Proposal {run.proposal_hash}</p><p className="small">Expires {run.expires_at && new Date(run.expires_at).toLocaleString()}</p></details>
              {reviewer && run.status === "awaiting_approval" && <div className="toolbar"><button className="quiet" disabled={busy} onClick={() => mutate("edit")}>Change to {run.proposal.action === "create_ticket" ? "internal notification" : "incident ticket"}</button><button disabled={busy} onClick={() => mutate("approve")}>Approve this proposal</button><button className="quiet" disabled={busy} onClick={() => mutate("reject")}>Reject</button></div>}
              {!reviewer && run.status === "awaiting_approval" && <p className="small">A reviewer must approve or reject this proposal.</p>}
              {reviewer && run.status === "approved" && action?.status !== "succeeded" && <button disabled={busy} onClick={() => mutate("execute")}>{action?.status === "pending" ? "Retry simulated action" : "Execute approved simulation"}</button>}
            </>}
            {action && <div className="action-result"><strong>Action {action.status}</strong><p>{action.result?.reference || "Pending; safe to retry"}</p></div>}
            {outcome && <div className="outcome"><h3>Outcome: {outcome.status.replaceAll("_", " ")}</h3><p>{outcome.reason}</p>{outcome.before_count !== undefined && <p>{outcome.before_count} reports before → {outcome.after_count} after · five-minute windows</p>}</div>}
          </section>}
          <section className="panel"><h2>Supporting conversations</h2><p className="small">Evidence from this incident’s signals · {detail.evidence.length} shown</p>{detail.evidence.map(item => <blockquote key={item.event_id}><p>{item.normalized_text}</p><cite>{item.event_id}</cite></blockquote>)}</section>
          {detail.workflow && <section className="panel"><h2>Audit history</h2><ul className="audit">{detail.workflow.audit.map((entry, index) => <li key={index}><strong>{entry.status.replaceAll("_", " ")}</strong><span>{entry.decision?.actor_id || entry.actor || "Workflow"}</span>{entry.reason && <span>{entry.reason}</span>}</li>)}</ul></section>}
        </> : <section className="panel"><p>{selected ? "Loading incident…" : "Select an incident to review its evidence."}</p></section>}</section></div>
    </>}
    <footer>EchoGuard AI · Human review before action · Synthetic local environment</footer>
  </main>;
}
