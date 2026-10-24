"use client";

export type Actor = { tenant_id: string; actor_id: string; role: string };
export type Incident = { _id?: string; incident_id?: string; product: string; error_code: string | null;
  peak_count: number; status: string; version: number; last_seen?: string; action_authorized?: boolean };
export type Step = { citation: string; quote: string };
export type State = { run_id: string; status: string; reason?: string; proposal_hash?: string;
  created_at?: string; expires_at?: string; incident: Incident;
  proposal?: { hypothesis: string; confidence: number; action: string; steps: Step[] };
  triage?: { severity: string; rationale: string };
  knowledge?: { citation: string; title: string; version: string }[];
  usage?: { model: string; input_tokens: number; output_tokens: number; cost_usd: number } };
export type Action = { status: string; result?: { reference: string; simulated: boolean }; completed_at?: string };
export type Audit = { status: string; actor?: string; decision?: { actor_id: string; decision: string }; reason?: string };
export type Detail = { incident: Incident; incident_hash: string;
  evidence: { event_id: string; normalized_text: string; channel?: string; event_time?: string }[];
  signals: { signal_id: string; window_start: string; window_end: string;
    current_count: number; distinct_customers: number }[];
  lifecycle: { kind: string; event_time: string; actor?: string }[];
  action_authorized: boolean;
  workflow: { _id: string; view?: State; audit: Audit[]; action?: Action } | null };
export type Outcome = { status: string; reason: string; before_count?: number; after_count?: number;
  window_seconds?: number; window_end?: string };
export type DemoStatus = { mode: "local"; tenant_id: string; outcome_window_seconds: number;
  cases: number; incidents: number; knowledge_documents: number };
export type Health = { status: "ready" | "degraded"; dependencies: { kafka: boolean; mongo: boolean } };
export type TrafficStatus = { status: "ready" | "processing"; kind: "baseline" | "recovery";
  projected_cases: number; expected_cases: number; started_at: string };
export type RunOutcome = { runId: string; value: Outcome };

export class ApiError extends Error {
  constructor(message: string, readonly status: number) { super(message); }
}

function incidentPriority(item: Incident) {
  return item.status === "resolved" ? 2 : item.action_authorized ? 1 : 0;
}

export function orderIncidents(items: Incident[]) {
  return [...items].sort((left, right) => incidentPriority(left) - incidentPriority(right) ||
    (Date.parse(right.last_seen ?? "") || 0) - (Date.parse(left.last_seen ?? "") || 0) ||
    (left._id ?? "").localeCompare(right._id ?? ""));
}

export async function api<T>(path: string, body?: object, key?: string): Promise<T> {
  let result: Response;
  try {
    result = await fetch(`/api/proxy/${path}`, { cache: "no-store", method: body ? "POST" : "GET",
      headers: { "Content-Type": "application/json", ...(key ? { "Idempotency-Key": key } : {}) },
      body: body ? JSON.stringify(body) : undefined });
  } catch { throw new ApiError("Connection interrupted. Retry the same operation.", 503); }
  let data: unknown;
  try { data = await result.json(); }
  catch { throw new ApiError("The service returned an invalid response. Please retry.", 502); }
  if (!result.ok) {
    const message = data && typeof data === "object" && "detail" in data && typeof data.detail === "string"
      ? data.detail : "The request could not be completed";
    throw new ApiError(message, result.status);
  }
  return data as T;
}

export async function platformHealth(): Promise<Health | null> {
  try {
    const response = await fetch("/api/proxy/health/ready", { cache: "no-store" });
    const body: unknown = await response.json();
    if (!body || typeof body !== "object" || !("dependencies" in body) || !("status" in body)) return null;
    const value = body as Health;
    return (value.status === "ready" || value.status === "degraded") &&
      typeof value.dependencies?.kafka === "boolean" && typeof value.dependencies?.mongo === "boolean" ? value : null;
  } catch { return null; }
}
