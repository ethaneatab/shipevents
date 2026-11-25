import { PermanentError } from "./retry.ts";
import type { Status } from "../events/types.ts";

export interface EtaRequest {
  shipmentId: string;
  status: Status;
  location?: string | undefined;
  traceId?: string | undefined;
}

export interface EtaResponse {
  etaAt: string;
  source: string;
}

/** One attempt against the ETA service. 5xx, 429, network errors and timeouts are retryable; other 4xx are not. */
export async function fetchEta(baseUrl: string, req: EtaRequest, signal: AbortSignal): Promise<EtaResponse> {
  const res = await fetch(`${baseUrl}/v1/eta`, {
    method: "POST",
    headers: { "content-type": "application/json", ...(req.traceId ? { "x-trace-id": req.traceId } : {}) },
    body: JSON.stringify(req),
    signal,
  });
  if (res.ok) return (await res.json()) as EtaResponse;
  const text = (await res.text()).slice(0, 200);
  if (res.status >= 400 && res.status < 500 && res.status !== 429) throw new PermanentError(`eta ${res.status}: ${text}`, res.status);
  throw new Error(`eta ${res.status}: ${text}`);
}
