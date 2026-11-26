// Records what the processor actually did with each message, keyed by its Kafka position,
// so the /learn page can light stages from real state instead of a front-end animation.
export type StepStatus = "ok" | "fail" | "skip" | "retry" | "info";

export interface Step {
  stage: string;
  status: StepStatus;
  at: string;
  summary: string;
  data?: unknown;
}

const MAX_TRACES = 2000;
const traces = new Map<string, Step[]>();

export const posKey = (partition: number, offset: string | number) => `p${partition}-o${offset}`;

export function recordStep(key: string, stage: string, status: StepStatus, summary: string, data?: unknown): void {
  let list = traces.get(key);
  if (!list) {
    list = [];
    traces.set(key, list);
    if (traces.size > MAX_TRACES) traces.delete(traces.keys().next().value!);
  }
  list.push({ stage, status, at: new Date().toISOString(), summary, data });
}

export function getSteps(key: string): Step[] {
  return traces.get(key) ?? [];
}

/** Teaching slow-motion: only for messages that carry x-learn-pace-ms (the /learn sample button sets it). */
export async function pace(headers: Record<string, string>): Promise<void> {
  const ms = Math.min(1500, Math.max(0, Number(headers["x-learn-pace-ms"] ?? 0)));
  if (ms > 0) await new Promise((r) => setTimeout(r, ms));
}
