import { describe, expect, it } from "vitest";
import { backoffDelay, PermanentError, withRetry, type RetryPolicy } from "../../src/processor/retry.ts";

const P: RetryPolicy = { maxAttempts: 4, baseDelayMs: 10, maxDelayMs: 25, timeoutMs: 50, jitter: false };

describe("backoff", () => {
  it("doubles and caps", () => {
    expect([1, 2, 3, 4, 5].map((a) => backoffDelay(P, a))).toEqual([10, 20, 25, 25, 25]);
  });
  it("equal jitter stays between half and the full delay", () => {
    const p = { ...P, jitter: true, baseDelayMs: 200, maxDelayMs: 2000 };
    expect(backoffDelay(p, 3, () => 0)).toBe(400);
    expect(backoffDelay(p, 3, () => 1)).toBe(800);
  });
});

describe("withRetry", () => {
  it("succeeds on the third attempt and reports every attempt", async () => {
    let n = 0;
    const r = await withRetry(P, async () => {
      if (++n < 3) throw new Error("503");
      return "eta";
    });
    expect(r.ok).toBe(true);
    expect(r.attempts.map((a) => [a.attempt, a.ok, a.backoffMs])).toEqual([[1, false, 10], [2, false, 20], [3, true, undefined]]);
  });

  it("gives up after maxAttempts: bounded, not forever", async () => {
    let n = 0;
    const r = await withRetry(P, async () => {
      n++;
      throw new Error("503");
    });
    expect(n).toBe(4);
    expect(r).toMatchObject({ ok: false, permanent: false, lastError: "503" });
  });

  it("does not retry a permanent error", async () => {
    let n = 0;
    const r = await withRetry(P, async () => {
      n++;
      throw new PermanentError("eta 400", 400);
    });
    expect(n).toBe(1);
    expect(r).toMatchObject({ ok: false, permanent: true });
  });

  it("a hung call is cut off by the per-attempt timeout", async () => {
    const r = await withRetry({ ...P, maxAttempts: 2 }, (signal) => new Promise((_, reject) => signal.addEventListener("abort", () => reject(signal.reason))));
    expect(r).toMatchObject({ ok: false, lastError: "timeout after 50ms" });
    expect(r.attempts).toHaveLength(2);
  });
});
