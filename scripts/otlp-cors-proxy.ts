/**
 * @filedesc Browser-facing OTLP HTTP shim that handles local CORS preflights before forwarding to Fluent Bit.
 */
const externalPort = Number.parseInt(Bun.env.AUTOK_LOGAL_OTLP_EXTERNAL_PORT ?? "4318", 10);
const internalPort = Number.parseInt(Bun.env.AUTOK_LOGAL_OTLP_INTERNAL_PORT ?? "4319", 10);
const allowedOrigins = new Set(
  (Bun.env.AUTOK_LOGAL_OTLP_CORS_ORIGINS ?? "https://localhost:3000,http://localhost:3000,https://127.0.0.1:3000,http://127.0.0.1:3000")
    .split(",")
    .map((origin) => origin.trim())
    .filter(Boolean),
);

/** Builds the CORS response policy for trusted local Auto-K browser origins. */
function corsHeaders(origin: string | null): HeadersInit {
  const headers: Record<string, string> = {
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Access-Control-Allow-Headers": "content-type, traceparent, tracestate, baggage",
    "Access-Control-Max-Age": "600",
    Vary: "Origin",
  };

  if (origin && allowedOrigins.has(origin)) {
    headers["Access-Control-Allow-Origin"] = origin;
  }

  return headers;
}

/** Removes browser-only hop headers before forwarding the OTLP request to Fluent Bit. */
function cleanForwardHeaders(request: Request): Headers {
  const headers = new Headers(request.headers);
  headers.delete("host");
  headers.delete("origin");
  headers.delete("referer");
  headers.set("connection", "close");
  return headers;
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

Bun.serve({
  hostname: "127.0.0.1",
  port: externalPort,
  async fetch(request) {
    const origin = request.headers.get("origin");
    if (request.method === "OPTIONS") {
      return new Response(null, {
        status: 204,
        headers: corsHeaders(origin),
      });
    }

    const upstreamUrl = new URL(request.url);
    upstreamUrl.protocol = "http:";
    upstreamUrl.hostname = "127.0.0.1";
    upstreamUrl.port = String(internalPort);

    let upstreamResponse: Response;
    try {
      upstreamResponse = await fetch(upstreamUrl, {
        method: request.method,
        headers: cleanForwardHeaders(request),
        body: request.body,
      });
    } catch (error) {
      console.warn(`OTLP proxy upstream ${request.method} ${upstreamUrl.pathname} failed: ${errorMessage(error)}`);
      return new Response("OTLP upstream unavailable\n", {
        status: 502,
        headers: corsHeaders(origin),
      });
    }

    const headers = new Headers(upstreamResponse.headers);
    for (const [key, value] of Object.entries(corsHeaders(origin))) {
      headers.set(key, value);
    }

    return new Response(upstreamResponse.body, {
      status: upstreamResponse.status,
      statusText: upstreamResponse.statusText,
      headers,
    });
  },
});

console.log(`OTLP CORS proxy listening on http://127.0.0.1:${externalPort} and forwarding to http://127.0.0.1:${internalPort}`);
