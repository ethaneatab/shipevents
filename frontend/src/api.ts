export async function getJson<T = any>(url: string, init?: RequestInit): Promise<T> {
  const res = await fetch(url, init);
  const body = await res.json().catch(() => ({}));
  if (!res.ok) throw Object.assign(new Error((body as { message?: string; error?: string }).message ?? (body as { error?: string }).error ?? res.statusText), { status: res.status, body });
  return body as T;
}

export function postJson<T = any>(url: string, body: unknown, headers: Record<string, string> = {}): Promise<T> {
  return getJson<T>(url, { method: "POST", headers: { "content-type": "application/json", ...headers }, body: JSON.stringify(body) });
}

export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
