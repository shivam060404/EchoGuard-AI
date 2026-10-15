import { cookies } from "next/headers";
import { NextRequest } from "next/server";

export const apiBase = process.env.INTERNAL_API_URL ?? "http://localhost:8000";
export const cookieName = "echoguard_session";
export async function accessToken() {
  return (await cookies()).get(cookieName)?.value;
}
export function sameOrigin(request: NextRequest) {
  const origin = request.headers.get("origin");
  // Explicit configured public origin avoids trusting a proxy-supplied hostname.
  const allowed = process.env.CONSOLE_ORIGIN ?? "http://localhost:3000";
  return origin === allowed;
}
