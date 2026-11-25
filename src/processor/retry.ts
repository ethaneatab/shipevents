export interface RetryPolicy {
  maxAttempts: number;
  baseDelayMs: number;
  maxDelayMs: number;
  /** Per attempt. A hung dependency must not hold the partition forever. */
  timeoutMs: number;
  jitter: boolean;
}

export function policyFromEnv(env = process.env): RetryPolicy {
  return {
    maxAttempts: Number(env.RETRY_MAX_ATTEMPTS ?? 4),
    baseDelayMs: Number(env.RETRY_BASE_MS ?? 200),
    maxDelayMs: Number(env.RETRY_MAX_MS ?? 2000),
    timeoutMs: Number(env.DOWNSTREAM_TIMEOUT_MS ?? 800),
    jitter: (env.RETRY_JITTER ?? "1") !== "0",
  };
}

export class PermanentError extends Error {
  readonly status: number | undefined;
  constructor(message: string, status?: number) {
    super(message);
    this.name = "PermanentError";
    this.status = status;
  }
}

export interface AttemptRecord {
  attempt: number;
  ok: boolean;
  ms: number;
  error?: string;
  /** Delay before the next attempt, if there is one. */
  backoffMs?: number;
}

export type RetryResult<T> =
  | { ok: true; value: T; attempts: AttemptRecord[] }
  | { ok: false; permanent: boolean; attempts: AttemptRecord[]; lastError: string };

// @learn:backoff
/** Exponential backoff with a cap: 200, 400, 800 ... never above maxDelayMs. Equal jitter keeps half the delay fixed. */
export function backoffDelay(policy: RetryPolicy, attempt: number, rand = Math.random): number {
  const exp = Math.min(policy.maxDelayMs, policy.baseDelayMs * 2 ** (attempt - 1));
  return policy.jitter ? Math.round(exp / 2 + (rand() * exp) / 2) : exp;
}
// @learn:end backoff

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

// @learn:retry-loop
/**
 * Bounded retries for temporary failures. Permanent errors (4xx) stop immediately;
 * after maxAttempts the caller dead-letters the event instead of blocking the partition forever.
 */
export async function withRetry<T>(policy: RetryPolicy, fn: (signal: AbortSignal) => Promise<T>, onAttempt?: (a: AttemptRecord) => void): Promise<RetryResult<T>> {
  const attempts: AttemptRecord[] = [];
  for (let attempt = 1; attempt <= policy.maxAttempts; attempt++) {
    const started = performance.now();
    try {
      const value = await fn(AbortSignal.timeout(policy.timeoutMs));
      const rec = { attempt, ok: true, ms: Math.round(performance.now() - started) };
      attempts.push(rec);
      onAttempt?.(rec);
      return { ok: true, value, attempts };
    } catch (e) {
      const err = e as Error;
      const message = err.name === "TimeoutError" ? `timeout after ${policy.timeoutMs}ms` : err.message;
      const permanent = err instanceof PermanentError;
      const last = permanent || attempt === policy.maxAttempts;
      const rec: AttemptRecord = { attempt, ok: false, ms: Math.round(performance.now() - started), error: message };
      if (!last) rec.backoffMs = backoffDelay(policy, attempt);
      attempts.push(rec);
      onAttempt?.(rec);
      if (last) return { ok: false, permanent, attempts, lastError: message };
      await sleep(rec.backoffMs!);
    }
  }
  throw new Error("unreachable");
}
// @learn:end retry-loop
