"use client";
import { FormEvent, useCallback, useEffect, useRef, useState } from "react";
import { api, ApiError, orderIncidents, platformHealth } from "@/lib/console-client";
import type { Actor, DemoStatus, Detail, Health, Incident, Outcome, RunOutcome, TrafficStatus } from "@/lib/console-client";

const statusNames: Record<string, string> = {
  awaiting_approval: "Awaiting approval", needs_review: "Needs review",
  approved: "Approved", rejected: "Rejected", succeeded: "Completed",
  insufficient_data: "Awaiting more activity", improving: "Decreasing",
  worsening: "Increasing", unchanged: "Stable",
};
function display(value: string) { return statusNames[value] ?? value.replaceAll("_", " "); }
function actionLabel(value: string) {
  return value === "create_ticket" ? "Create incident ticket" :
    value === "post_internal_notification" ? "Notify internal team" : display(value);
}
function time(value?: string) { return value ? new Date(value).toLocaleString() : "—"; }
function shortTime(value?: string) { return value ? new Date(value).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }) : "—"; }

export default function Console() {
  const [actor, setActor] = useState<Actor | null>(null);
  const [token, setToken] = useState("");
  const [incidents, setIncidents] = useState<Incident[]>([]);
  const [cursor, setCursor] = useState<string | null>(null);
  const [selected, setSelected] = useState("");
  const [detail, setDetail] = useState<Detail | null>(null);
  const [outcome, setOutcome] = useState<RunOutcome | null>(null);
  // undefined means the capability check has not completed; null means disabled.
  const [demo, setDemo] = useState<DemoStatus | null | undefined>(undefined);
  const [health, setHealth] = useState<Health | null>(null);
  const [traffic, setTraffic] = useState<Record<string, { baseline?: TrafficStatus; recovery?: TrafficStatus }>>({});
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const [pendingLabel, setPendingLabel] = useState("");
  const [connected, setConnected] = useState(false);
  const [updated, setUpdated] = useState("");
  const [loaded, setLoaded] = useState(false);
  const [query, setQuery] = useState("");
  const [filter, setFilter] = useState<"all" | "open" | "resolved">("all");
  const selectedRef = useRef("");
  const visibleCount = useRef(50);
  const refreshSequence = useRef(0);
  const activeRefresh = useRef(0);
  const sessionEpoch = useRef(0);
  const requestKeys = useRef<Record<string, string>>({});
  const handleFailure = useCallback((cause: unknown) => {
    if (cause instanceof ApiError && cause.status === 401) {
      refreshSequence.current++;
      activeRefresh.current = 0;
      sessionEpoch.current++;
      selectedRef.current = "";
      visibleCount.current = 50;
      requestKeys.current = {};
      setActor(null); setDemo(undefined); setHealth(null); setLoaded(false); setUpdated(""); setIncidents([]); setSelected("");
      setDetail(null); setOutcome(null); setTraffic({});
      setError("Your session has expired. Sign in again with a token from this workspace.");
    } else setError(cause instanceof Error ? cause.message : "Request failed. Please retry.");
  }, []);

  const refresh = useCallback(async (force = false) => {
    // A slow request must not be invalidated by every five-second update.
    if (activeRefresh.current && !force) return;
    const sequence = ++refreshSequence.current;
    activeRefresh.current = sequence;
    try {
    const [mode, list, readiness] = await Promise.all([
      api<DemoStatus>("demo/status").catch(error => {
        if (error instanceof ApiError && error.status === 404) return null;
        throw error;
      }),
      api<{ items: Incident[]; next_cursor: string | null }>("incidents"),
      platformHealth(),
    ]);
    if (sequence !== refreshSequence.current) return;
    setDemo(mode); setHealth(readiness);
    while (list.next_cursor && list.items.length < visibleCount.current) {
      const page = await api<{items: Incident[]; next_cursor: string | null}>(`incidents?after=${encodeURIComponent(list.next_cursor)}`);
      if (sequence !== refreshSequence.current) return;
      list.items.push(...page.items); list.next_cursor = page.next_cursor;
    }
    if (sequence !== refreshSequence.current) return;
    setIncidents(list.items); setCursor(list.next_cursor);
    const id = selectedRef.current || orderIncidents(list.items)[0]?._id || "";
    selectedRef.current = id; setSelected(id);
    if (id) {
      const value = await api<Detail>(`incidents/${id}`);
      if (sequence !== refreshSequence.current || id !== selectedRef.current) return;
      setDetail(value);
      const runId = value.workflow?._id;
      const [progress, trend] = await Promise.all([
        mode ? api<{ baseline?: TrafficStatus; recovery?: TrafficStatus }>(`demo/traffic?incident_id=${encodeURIComponent(id)}`) : Promise.resolve(null),
        value.workflow?.action && runId ? api<Outcome>(`workflows/${runId}/outcome`) : Promise.resolve(null),
      ]);
      if (sequence !== refreshSequence.current || id !== selectedRef.current) return;
      if (progress) setTraffic(previous => ({ ...previous, [id]: progress }));
      if (trend && runId) setOutcome({ runId, value: trend });
      else setOutcome(null);
    } else { setDetail(null); setOutcome(null); }
    setUpdated(new Date().toLocaleTimeString()); setLoaded(true);
    } finally {
      if (activeRefresh.current === sequence) activeRefresh.current = 0;
    }
  }, []);

  useEffect(() => {
    const epoch = sessionEpoch.current;
    api<Actor>("me").then(value => {
      if (sessionEpoch.current === epoch) setActor(value);
    }).catch(() => {});
  }, []);
  useEffect(() => {
    if (!actor) return;
    refresh().catch(handleFailure);
    const stream = new EventSource("/api/proxy/events");
    stream.onopen = () => setConnected(true);
    stream.onerror = () => { setConnected(false); refresh().catch(handleFailure); };
    stream.addEventListener("refresh", () => { refresh().catch(handleFailure); });
    return () => { stream.close(); setConnected(false); refreshSequence.current++; };
  }, [actor, refresh, handleFailure]);

  async function login(event: FormEvent) {
    event.preventDefault(); setBusy(true); setPendingLabel("Signing in"); setError("");
    try {
      const supplied = token.trim().replace(/^["']|["']$/g, "");
      const response = await fetch("/api/session", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ token: supplied }) });
      const body = await response.json();
      if (!response.ok) throw new Error(response.status === 401
        ? "Token not recognized for this workspace. Use the access token paired with this console URL, and paste only its value."
        : body.detail || "Sign in failed");
      refreshSequence.current++;
      activeRefresh.current = 0;
      sessionEpoch.current++;
      selectedRef.current = "";
      visibleCount.current = 50;
      requestKeys.current = {};
      setDemo(undefined); setHealth(null); setLoaded(false); setUpdated(""); setIncidents([]); setSelected("");
      setDetail(null); setOutcome(null); setTraffic({});
      setActor(body); setToken("");
    } catch (e) { handleFailure(e); } finally { setBusy(false); setPendingLabel(""); }
  }

  async function mutate(kind: "start" | "approve" | "reject" | "execute" | "resolve" | "edit") {
    if (!detail || busy) return;
    const run = detail.workflow?.view;
    setBusy(true); setPendingLabel({
      start: "Opening investigation", approve: "Recording approval",
      reject: "Recording decision", execute: "Running approved response",
      resolve: "Closing incident", edit: "Updating proposal",
    }[kind]); setError("");
    const identity = kind === "start" ? `${selected}:${detail.incident.version}:start`
      : kind === "resolve" ? `${selected}:${detail.incident_hash}:resolve`
        : `${run?.run_id}:${run?.proposal_hash}:${kind}`;
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
      await refresh(true);
    } catch (e) { handleFailure(e); } finally { setBusy(false); setPendingLabel(""); }
  }

  async function sendDemoTraffic(kind: "baseline" | "recovery") {
    if (!selected || busy || !demo) return;
    setBusy(true); setPendingLabel(kind === "baseline" ? "Preparing recent reports" : "Preparing follow-up activity"); setError("");
    const identity = `${selected}:${detail?.workflow?.view?.run_id ?? "none"}:traffic:${kind}:${kind === "recovery" ? detail?.workflow?.action?.completed_at ?? "pending" : ""}`;
    const key = requestKeys.current[identity] ?? crypto.randomUUID();
    requestKeys.current[identity] = key;
    try {
      for (let attempt = 0; attempt < 5; attempt++) {
        const result = await api<TrafficStatus>("demo/traffic", { incident_id: selected, kind }, key);
        setTraffic(previous => ({ ...previous, [selected]: { ...previous[selected], [kind]: result } }));
        if (result.status === "ready") { delete requestKeys.current[identity]; await refresh(true); return; }
      }
      setError("The sample is still processing. Select the same control again to check its progress.");
    } catch (e) { handleFailure(e); } finally { setBusy(false); setPendingLabel(""); }
  }

  const run = detail?.workflow?.view;
  const action = detail?.workflow?.action;
  const currentOutcome = outcome && outcome.runId === detail?.workflow?._id ? outcome.value : null;
  const reviewer = actor?.role === "reviewer";
  const baseline = traffic[selected]?.baseline;
  const recovery = traffic[selected]?.recovery;
  const recoveryForAction = Boolean(recovery && action?.completed_at &&
    Date.parse(recovery.started_at) >= Date.parse(action.completed_at));
  const baselineFresh = demo === null || Boolean(demo && baseline?.status === "ready" &&
    run?.created_at && Date.parse(baseline.started_at) >= Date.parse(run.created_at) &&
    Date.now() - new Date(baseline.started_at).getTime() < (demo.outcome_window_seconds - 5) * 1000);
  const approvalExpired = Boolean(run?.expires_at && Date.parse(run.expires_at) <= Date.now());
  const restartAvailable = !detail?.action_authorized && (!run || ["needs_review", "rejected"].includes(run.status) ||
    (approvalExpired && action?.status !== "pending" && action?.status !== "succeeded"));
  const filtered = orderIncidents(incidents.filter(item => (filter === "all" || (filter === "resolved" ? item.status === "resolved" : item.status !== "resolved")) &&
    `${item.error_code ?? ""} ${item.product}`.toLowerCase().includes(query.toLowerCase())));
  const openCount = incidents.filter(item => item.status !== "resolved").length;
  const maxSignal = Math.max(1, ...(detail?.signals ?? []).map(signal => signal.current_count));
  const stage = detail?.incident.status === "resolved" ? 5 : action?.status === "succeeded" ? 4 : run?.status === "approved" ? 3 : run?.status === "awaiting_approval" ? 2 : run ? 1 : 0;
  const stages = ["Detected", "Investigating", "Review", "Approved", "Monitoring", "Resolved"];
  const resolvedCount = incidents.length - openCount;
  const largestSpike = Math.max(0, ...incidents.map(item => item.peak_count));
  const nextStep = detail?.incident.status === "resolved"
    ? { title: "Incident resolved", description: "The response and its review history are available below." }
    : action?.status === "succeeded" && currentOutcome?.status === "improving"
      ? { title: "Ready for final review", description: "Report volume has decreased. Review the evidence before closing the incident." }
      : action?.status === "succeeded"
        ? { title: "Monitor the outcome", description: "Wait for the observation window and compare new reports with the earlier activity." }
        : detail?.action_authorized
          ? { title: "Response already authorized", description: "This incident has a recorded response. Review its status before making another decision." }
        : approvalExpired && action?.status !== "pending"
          ? { title: "Approval expired", description: "Start a new investigation to review current evidence before taking action." }
        : run?.status === "approved"
          ? { title: "Approved action ready", description: demo === undefined ? "Checking workspace status before the action can continue." : demo && !baselineFresh ? "Prepare a recent report sample, then run the approved action preview." : "The approved action is ready for a reviewer to run." }
          : run?.status === "awaiting_approval"
            ? { title: "Decision needed", description: "Review the proposed response and its supporting sources, then approve or reject it." }
            : run
              ? { title: "Review the investigation", description: "Check the available evidence and start a new review if needed." }
              : { title: "Investigation needed", description: "Open an investigation to connect the reports with relevant guidance." };
  return <main className="shell console-shell">
    <header className="topbar"><div className="brand"><span className="brand-mark" aria-hidden="true">E<span>.</span></span><span>EchoGuard <strong>AI</strong></span></div>
      <div className="topbar-right"><span className="topbar-label">OPERATIONS DASHBOARD</span><span className="topbar-divider" />
        {actor ? <><span className="role-pill">{display(actor.role)}</span><button className="quiet topbar-button" onClick={async () => {
          const response = await fetch("/api/session", { method: "DELETE" }).catch(() => null);
          if (!response?.ok) { setError("Sign out failed. Please retry."); return; }
          visibleCount.current = 50; refreshSequence.current++; activeRefresh.current = 0;
          sessionEpoch.current++;
          setActor(null); setDetail(null); setIncidents([]); setDemo(undefined); setHealth(null); setLoaded(false); setUpdated(""); setTraffic({});
          selectedRef.current = ""; setSelected(""); setOutcome(null); setError(""); requestKeys.current = {};
        }}>Sign out <span aria-hidden="true">↗</span></button></> : <span className="role-pill">SECURE CONSOLE</span>}</div></header>
    {!actor ? <div className="login-layout"><section className="login-story"><div className="story-orbit" aria-hidden="true"><span className="orbit-core">E</span><i /><i /><i /></div>
        <p className="eyebrow">INCIDENT OPERATIONS</p><h1>See the issue.<br/><em>Lead the response.</em></h1>
        <p className="lead">A clear view of emerging customer issues, supporting reports, response decisions, and outcomes.</p>
        <div className="story-flow"><span>01 <strong>Detect</strong></span><b>→</b><span>02 <strong>Review</strong></span><b>→</b><span>03 <strong>Respond</strong></span><b>→</b><span>04 <strong>Monitor</strong></span></div>
      </section><section className="panel login"><span className="login-icon" aria-hidden="true">↗</span><p className="eyebrow">WORKSPACE ACCESS</p><h2>Welcome to the console</h2>
        <p className="small">Sign in to review incidents and coordinate the next response.</p>
        <form onSubmit={login}><label htmlFor="token">Access token</label><input id="token" type="password" autoComplete="off" placeholder="Paste token value" required value={token} onChange={e => setToken(e.target.value)} />
          <button disabled={busy || !token}>{busy ? "Signing in…" : <>Enter workspace <span aria-hidden="true">→</span></>}</button></form>
        <details className="login-hint"><summary>Where do I find my access token?</summary><p>Use the token from the credentials file created with this workspace. It must match the console URL you opened.</p></details>
        {error && <p role="alert" className="error">{error}</p>}</section></div> : <>
      <section className="console-heading"><div><p className="eyebrow">OVERVIEW</p><h1>Incident dashboard<span className="heading-period">.</span></h1>
        <p>Monitor customer issues and make the next response decision.</p></div><div className="connection"><span className={`status ${connected ? "status-ready" : "status-degraded"}`}><span className="status-dot" />{connected ? "Console connected" : updated ? "Reconnecting" : "Connecting"}</span><span className="updated">Updated {updated || "—"}</span></div></section>
      <section className="operations-strip" aria-label="System status"><div><span className="eyebrow">SYSTEM STATUS</span><strong>{health?.status === "ready" ? "Services available" : health?.status === "degraded" ? "Service connection needs attention" : "Checking service connections"}</strong></div>
        <span className={`service-state ${health?.dependencies.kafka ? "available" : ""}`}><i aria-hidden="true" />Kafka {health ? health.dependencies.kafka ? "ready" : "unavailable" : "checking"}</span>
        <span className={`service-state ${health?.dependencies.mongo ? "available" : ""}`}><i aria-hidden="true" />MongoDB {health ? health.dependencies.mongo ? "ready" : "unavailable" : "checking"}</span></section>
      <section className="metrics" aria-label="Workspace metrics"><div className="metric metric-highlight"><span>OPEN INCIDENTS</span><strong>{loaded ? `${openCount}${cursor ? "+" : ""}` : "—"}</strong><small>{cursor ? "Among loaded records" : "Require attention"}</small></div>
        <div className="metric"><span>{demo ? "SUPPORT REPORTS" : "TOTAL INCIDENTS"}</span><strong>{loaded ? `${demo ? demo.cases : incidents.length}${!demo && cursor ? "+" : ""}` : "—"}</strong><small>{demo ? "Processed in this workspace" : cursor ? "Showing loaded records" : "In this workspace"}</small></div>
        <div className="metric"><span>LARGEST SPIKE</span><strong>{loaded ? largestSpike : "—"}</strong><small>{cursor ? "Among loaded incidents" : "Reports in one incident"}</small></div>
        <div className="metric"><span>RESOLVED</span><strong>{loaded ? resolvedCount : "—"}</strong><small>{cursor ? "Among loaded records" : "Completed reviews"}</small></div></section>
      {busy && pendingLabel && <div role="status" className="progress"><span className="progress-indicator" aria-hidden="true" />{pendingLabel}…</div>}
      {error && <div role="alert" className="error global-error"><span>{error}</span><button className="quiet" onClick={() => refresh(true).then(() => setError("")).catch(handleFailure)}>Retry refresh</button></div>}
      <div className="workspace"><aside className="panel incident-list"><div className="panel-heading"><div><p className="eyebrow">INCIDENTS</p><h2>All issues <span className="count">{incidents.length}{cursor ? "+" : ""}</span></h2></div></div>
        <label className="search-label" htmlFor="incident-search">Search incidents</label><input id="incident-search" className="search" type="search" placeholder="Search product or error code" value={query} onChange={event => setQuery(event.target.value)} />
        <div className="filters" role="group" aria-label="Incident status filter">{(["all", "open", "resolved"] as const).map(value => <button key={value} className={filter === value ? "active" : ""} aria-pressed={filter === value} onClick={() => setFilter(value)}>{value === "all" ? "All" : value === "open" ? "Open" : "Resolved"}</button>)}</div>
        <div className="incident-items">{filtered.length === 0 && <p className="empty-note">{!loaded ? "Loading incident queue…" : incidents.length ? "No incidents match these filters." : "New incidents will appear here."}</p>}
          {filtered.map(item => <button key={item._id} className={`incident-card ${selected === item._id ? "selected" : ""}`} aria-pressed={selected === item._id} onClick={() => {
            if (busy) return; selectedRef.current = item._id!; setSelected(item._id!); setDetail(null); setOutcome(null);
            refresh(true).catch(handleFailure);
          }}><span className="incident-card-top"><strong>{item.error_code || "Shared issue"}</strong><span className={`badge ${item.status === "resolved" ? "badge-resolved" : "badge-open"}`}>{display(item.status)}</span></span>
            <span className="incident-product">{item.product}</span>{item.action_authorized && item.status !== "resolved" && <span className="incident-card-note">Response authorized · Monitoring</span>}<span className="incident-card-bottom"><span><b>{item.peak_count}</b> peak reports</span><span aria-hidden="true">↗</span></span></button>)}</div>
        {cursor && <button className="quiet load-more" onClick={async () => {
          try { const page = await api<{items: Incident[]; next_cursor: string | null}>(`incidents?after=${encodeURIComponent(cursor)}`);
            visibleCount.current += page.items.length;
            setIncidents(previous => [...previous, ...page.items.filter(item => !previous.some(old => old._id === item._id))]); setCursor(page.next_cursor);
          } catch (e) { handleFailure(e); }
        }}>Load more incidents ↓</button>}</aside>
        <section className="detail">{detail ? <>
          <section className="panel incident-hero"><div className="hero-title"><div><p className="eyebrow">{detail.incident.product.toUpperCase()} / INCIDENT DETAIL</p><h2>{detail.incident.error_code || "Shared issue"}</h2><p>Customer reports last seen {time(detail.incident.last_seen)}</p></div><span className={`badge hero-badge ${detail.incident.status === "resolved" ? "badge-resolved" : "badge-open"}`}>{display(detail.incident.status)}</span></div>
            <div className="incident-numbers"><div><strong>{detail.incident.peak_count}</strong><span>Peak reports</span></div><div><strong>{detail.signals.length}</strong><span>Report windows</span></div><div><strong>{detail.evidence.length}</strong><span>Conversations shown</span></div></div>
            <div className="process" aria-label="Incident workflow progress">{stages.map((label, index) => <div key={label} className={`process-step ${index <= stage ? "complete" : ""} ${index === stage ? "current" : ""}`}><span className="process-index">{index <= stage ? "✓" : String(index + 1).padStart(2, "0")}</span><span>{label}</span></div>)}</div>
            <div className="hero-actions">{restartAvailable && detail.incident.status !== "resolved" && <button disabled={busy} onClick={() => mutate("start")}>{run ? "Restart investigation" : "Investigate incident"} <span aria-hidden="true">→</span></button>}
              {reviewer && action?.status === "succeeded" && detail.incident.status !== "resolved" && <button className="quiet" disabled={busy || demo === undefined || (demo !== null && currentOutcome?.status !== "improving")} onClick={() => mutate("resolve")}>Mark resolved</button>}</div>
            {detail.action_authorized && !action && <p className="review-note">A response was previously authorized for this incident. Its record is being reconciled; do not start another response.</p>}
            {run && restartAvailable && detail.incident.status !== "resolved" && <p className="small hero-note">Review current source reports before starting again.</p>}
          </section>
          <div className="detail-grid"><section className="panel next-step-panel"><p className="eyebrow">NEXT STEP</p><h2>{nextStep.title}</h2><p>{nextStep.description}</p><span className="next-step-stage">{stages[stage]}</span></section>
            <section className="panel signal-panel"><div className="panel-heading"><div><p className="eyebrow">CUSTOMER ACTIVITY</p><h2>Report trend</h2></div><span className="section-count">{detail.signals.length} windows</span></div>
            {detail.signals.length ? detail.signals.map(signal => <div className="signal-row" key={signal.signal_id}><div className="signal-row-header"><strong>{signal.current_count} reports</strong><span>{shortTime(signal.window_start)}–{shortTime(signal.window_end)}</span></div><div className="signal-track"><span style={{ width: `${Math.max(5, Math.round(signal.current_count / maxSignal * 100))}%` }} /></div><small>{signal.distinct_customers} distinct customers</small></div>) : <p className="empty-note">No signal windows are available yet.</p>}</section>
          </div>
          {run && <section className="panel response-panel"><div className="panel-heading"><div><p className="eyebrow">RESPONSE</p><h2>Recommended next steps</h2></div><span className={`badge ${run.status === "approved" ? "badge-resolved" : "badge-open"}`}>{display(run.status)}</span></div>
            {run.triage && <div className="triage-strip"><span>SEVERITY <strong>{run.triage.severity.toUpperCase()}</strong></span><p>{run.triage.rationale}</p></div>}
            {run.reason && <p className="error">Review required: {display(run.reason)}</p>}
            {run.proposal?.steps && <><div className="hypothesis"><span>POSSIBLE CAUSE</span><p>{run.proposal.hypothesis}</p><small>Working assessment · Confirm against live service evidence before treating it as root cause.</small></div>
              <div className="response-columns"><div><h3>Recommended checks</h3><ol className="steps">{run.proposal.steps.map((step, index) => <li key={`${step.citation}-${index}`}><span className="step-index">{String(index + 1).padStart(2, "0")}</span><div><p>{step.quote}</p><span className="citation" title={step.citation}>{run.knowledge?.find(k => k.citation === step.citation)?.title || "Approved guidance"}</span></div></li>)}</ol></div>
                <div className="proposed-action"><span>PROPOSED RESPONSE</span><strong>{actionLabel(run.proposal.action)}</strong><p>A reviewer must approve this response before it can be recorded.</p><details><summary>Approval record</summary><p className="citation">Reference: {run.proposal_hash}</p><p className="small">Expires {time(run.expires_at)}</p></details></div></div>
              {reviewer && run.status === "awaiting_approval" && !approvalExpired && <div className="decision-bar"><button className="quiet" disabled={busy} onClick={() => mutate("edit")}>Switch to {run.proposal.action === "create_ticket" ? "internal notification" : "incident ticket"}</button><button disabled={busy} onClick={() => mutate("approve")}>Approve proposal <span aria-hidden="true">→</span></button><button className="text-button" disabled={busy} onClick={() => mutate("reject")}>Reject</button></div>}
              {!reviewer && run.status === "awaiting_approval" && !approvalExpired && <p className="review-note">Waiting for a reviewer to approve or reject this proposal.</p>}
              {approvalExpired && action?.status !== "pending" && action?.status !== "succeeded" && <p className="review-note">This approval has expired. Restart the investigation to continue.</p>}
              {reviewer && run.status === "approved" && action?.status !== "succeeded" && (!approvalExpired || action?.status === "pending") && <div className="decision-bar"><button disabled={busy || (detail.action_authorized && !action) || (action?.status !== "pending" && !baselineFresh)} onClick={() => mutate("execute")}>{action?.status === "pending" ? "Retry action preview" : "Run approved action preview"} <span aria-hidden="true">→</span></button>{demo && !baselineFresh && action?.status !== "pending" && <span className="small">Generate a recent report sample first.</span>}</div>}
            </>}
            {action && <div className="action-result"><span>RESPONSE STATUS</span><strong>{display(action.status)}</strong><p>{action.result?.reference || "Processing. You can retry the same action safely."}</p></div>}
            {currentOutcome && <div className={`outcome ${currentOutcome.status === "improving" ? "outcome-good" : ""}`}><span>REPORT VOLUME</span><h3>{display(currentOutcome.status)}</h3><p>Based on customer reports before and after the response. This trend alone does not establish the cause of any change.</p>{currentOutcome.before_count !== undefined && <strong>{currentOutcome.before_count} before <span aria-hidden="true">→</span> {currentOutcome.after_count} after <small>· {currentOutcome.window_seconds ?? 300}-second comparison</small></strong>}</div>}
          </section>}
          {demo && reviewer && (run?.status === "approved" || action?.status === "succeeded") && detail.incident.status !== "resolved" && <section className="panel scenario-panel"><div className="panel-heading"><div><p className="eyebrow">OUTCOME CHECK</p><h2>Compare activity before and after</h2></div><span className="simulation-tag">PREVIEW WORKSPACE</span></div>
            <p>Use the sample controls to compare report volume around the approved response.</p>
            <div className="scenario-grid"><div className="scenario-step"><span className="scenario-number">01</span><div><h3>Recent reports</h3><p>Generate a fresh complaint sample before running the action preview.</p>
              <button className="quiet" disabled={busy || detail.action_authorized} onClick={() => sendDemoTraffic("baseline")}>{baseline?.status === "ready" ? "Refresh sample" : "Generate report sample"}</button>
              <span className={`scenario-state ${baseline?.status === "ready" && baselineFresh ? "ready" : ""}`}>{baseline ? `${baseline.projected_cases}/${baseline.expected_cases} available · ${baselineFresh ? "Ready" : "Refresh before action"}` : "No recent sample"}</span></div></div>
              <div className="scenario-step"><span className="scenario-number">02</span><div><h3>Follow-up activity</h3><p>After the observation window, generate follow-up activity to complete the comparison.</p>
                {action?.status === "succeeded" && <button className="quiet" disabled={busy || (recoveryForAction && recovery?.status === "ready") || (currentOutcome?.window_end ? Date.now() < new Date(currentOutcome.window_end).getTime() : true)} onClick={() => sendDemoTraffic("recovery")}>Generate follow-up activity</button>}
                <span className={`scenario-state ${recoveryForAction && recovery?.status === "ready" ? "ready" : ""}`}>{recoveryForAction && recovery ? `${recovery.projected_cases}/${recovery.expected_cases} available · ${recovery.status}` : "Not started"}</span></div></div></div>
            {action?.status === "succeeded" && currentOutcome?.window_end && <p className="small observation-note">Comparison window ends {time(currentOutcome.window_end)}. A reviewer can resolve the incident after a decreasing trend is recorded.</p>}</section>}
          <section className="panel evidence-panel"><div className="panel-heading"><div><p className="eyebrow">SOURCE REPORTS</p><h2>Supporting conversations</h2></div><span className="section-count">{detail.evidence.length} shown</span></div><p className="small">Examples linked to this incident.</p>
            {detail.evidence.length ? <div className="evidence-list">{detail.evidence.map(item => <blockquote key={item.event_id}><p>“{item.normalized_text}”</p><cite><span>{item.channel || "support"}</span><span>{time(item.event_time)}</span></cite></blockquote>)}</div> : <p className="empty-note">No supporting conversations are available.</p>}</section>
          <div className="detail-grid history-grid">{detail.workflow && <section className="panel"><div className="panel-heading"><div><p className="eyebrow">REVIEW RECORD</p><h2>Decision history</h2></div></div><ul className="audit">{detail.workflow.audit.map((entry, index) => <li key={index}><span className="audit-mark" /><div><strong>{display(entry.status)}</strong><span>{entry.decision?.actor_id || entry.actor || "Workflow"}</span>{entry.reason && <p>{entry.reason}</p>}</div></li>)}</ul></section>}
            {detail.lifecycle.length > 0 && <section className="panel"><div className="panel-heading"><div><p className="eyebrow">TIMELINE</p><h2>Incident activity</h2></div></div><ul className="audit">{detail.lifecycle.map((entry, index) => <li key={`${entry.kind}-${index}`}><span className="audit-mark" /><div><strong>{display(entry.kind)}</strong><span>{time(entry.event_time)}</span>{entry.actor && <p>{entry.actor}</p>}</div></li>)}</ul></section>}</div>
        </> : <section className="panel empty-detail"><span aria-hidden="true">⌁</span><h2>{selected ? "Loading incident…" : "Choose an incident"}</h2><p>{selected ? "Retrieving the latest evidence and workflow state." : "Select a signal from the queue to review the evidence and next action."}</p></section>}</section></div>
    </>}
    <footer><span>EchoGuard AI</span><span>Preview workspace · No external customer systems are changed</span></footer>
  </main>;
}
