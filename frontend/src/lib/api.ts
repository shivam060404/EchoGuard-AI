export type DependencyHealth = {
  status: "ready" | "degraded";
  dependencies: Record<string, boolean>;
};

const apiBase = process.env.INTERNAL_API_URL ?? "http://localhost:8000";

export async function getPlatformHealth(): Promise<DependencyHealth | null> {
  try {
    const response = await fetch(`${apiBase}/health/ready`, {
      cache: "no-store",
      signal: AbortSignal.timeout(3000),
    });
    const body: unknown = await response.json();
    if (typeof body !== "object" || body === null || !("status" in body) || !("dependencies" in body)) {
      return null;
    }
    if (
      (body.status !== "ready" && body.status !== "degraded") ||
      typeof body.dependencies !== "object" ||
      body.dependencies === null ||
      Array.isArray(body.dependencies)
    ) {
      return null;
    }
    const dependencies = body.dependencies as Record<string, unknown>;
    if (typeof dependencies.kafka !== "boolean" || typeof dependencies.mongo !== "boolean") {
      return null;
    }
    return {status: body.status, dependencies: {kafka: dependencies.kafka, mongo: dependencies.mongo}};
  } catch {
    return null;
  }
}
