import { cookies } from "next/headers";
import { NextRequest, NextResponse } from "next/server";
import { apiBase, cookieName, sameOrigin } from "@/lib/session";

export async function POST(request: NextRequest) {
  if (!sameOrigin(request)) return NextResponse.json({ detail: "Origin rejected" }, { status: 403 });
  if (Number(request.headers.get("content-length") ?? 0) > 8192)
    return NextResponse.json({ detail: "Request too large" }, { status: 413 });
  let token: unknown;
  try { token = (await request.json()).token; } catch { return NextResponse.json({ detail: "Invalid request" }, { status: 400 }); }
  if (typeof token !== "string" || token.length < 32 || token.length > 512)
    return NextResponse.json({ detail: "Invalid access token" }, { status: 401 });
  try {
    const upstream = await fetch(`${apiBase}/me`, { headers: { Authorization: `Bearer ${token}` },
      cache: "no-store", signal: AbortSignal.timeout(10000) });
    const body = await upstream.json();
    if (!upstream.ok) return NextResponse.json(body, { status: upstream.status });
    (await cookies()).set(cookieName, token, { httpOnly: true, sameSite: "strict", path: "/",
      secure: (process.env.CONSOLE_ORIGIN ?? "http://localhost:3000").startsWith("https:"), maxAge: 3600 });
    return NextResponse.json(body, { headers: { "Cache-Control": "no-store" } });
  } catch { return NextResponse.json({ detail: "API unavailable" }, { status: 503 }); }
}

export async function DELETE(request: NextRequest) {
  if (!sameOrigin(request)) return NextResponse.json({ detail: "Origin rejected" }, { status: 403 });
  (await cookies()).delete(cookieName);
  return NextResponse.json({ status: "signed_out" });
}
