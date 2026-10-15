import { NextRequest, NextResponse } from "next/server";
import { accessToken, apiBase, sameOrigin } from "@/lib/session";

export const dynamic = "force-dynamic";
async function proxy(request: NextRequest, context: { params: Promise<{ path: string[] }> }) {
  const path = (await context.params).path.join("/");
  const read = /^(me|events|incidents|incidents\/[a-zA-Z0-9_-]+|workflows\/[a-zA-Z0-9_-]+(?:\/outcome)?)$/;
  const write = /^(incidents\/[a-zA-Z0-9_-]+\/(workflows|resolve)|workflows\/[a-zA-Z0-9_-]+\/(decision|action|proposal))$/;
  if (!(request.method === "GET" ? read : write).test(path))
    return NextResponse.json({ detail: "Unknown resource" }, { status: 404 });
  if (request.method !== "GET" && !sameOrigin(request))
    return NextResponse.json({ detail: "Origin rejected" }, { status: 403 });
  const token = await accessToken();
  if (!token) return NextResponse.json({ detail: "Sign in required" }, { status: 401 });
  const body = request.method === "GET" ? undefined : await request.text();
  if (body && body.length > 8192) return NextResponse.json({ detail: "Request too large" }, { status: 413 });
  try {
    const headers: Record<string, string> = { Authorization: `Bearer ${token}`, "Content-Type": "application/json" };
    const key = request.headers.get("idempotency-key");
    if (key) headers["Idempotency-Key"] = key;
    const result = await fetch(`${apiBase}/${path}${request.nextUrl.search}`, {
      method: request.method, headers, body, cache: "no-store",
      signal: AbortSignal.any([request.signal, AbortSignal.timeout(path === "events" ? 45000 : 60000)]),
    });
    return new Response(result.body, { status: result.status, headers: {
      "Content-Type": result.headers.get("content-type") ?? "application/json",
      "Cache-Control": "no-store", "X-Accel-Buffering": "no",
    } });
  } catch { return NextResponse.json({ detail: "API unavailable. Retry the same operation." }, { status: 503 }); }
}
export const GET = proxy;
export const POST = proxy;
