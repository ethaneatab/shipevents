import { Fragment, useEffect, useState } from "react";
import { getJson, postJson } from "../api";
import { Json } from "../components/Json";

const TOKEN_KEY = "shipevents.adminToken";
function initialToken(): string {
  try { return sessionStorage.getItem(TOKEN_KEY) ?? import.meta.env.VITE_ADMIN_TOKEN ?? ""; } catch { return import.meta.env.VITE_ADMIN_TOKEN ?? ""; }
}

export function Ops() {
  const [token, setToken] = useState(initialToken);
  const [lag, setLag] = useState<any>(null);
  const [faults, setFaults] = useState<any>(null);
  const [dlq, setDlq] = useState<any[]>([]);
  const [openRow, setOpenRow] = useState<number | null>(null);
  const [shipment, setShipment] = useState("");
  const [plan, setPlan] = useState<any>(null);
  const [result, setResult] = useState<any>(null);
  const [audit, setAudit] = useState<any[]>([]);
  const [err, setErr] = useState<string | null>(null);
  const auth = { authorization: `Bearer ${token}` };
  const get = (u: string) => getJson(`/proc${u}`, { headers: auth });

  async function refresh() {
    try {
      setErr(null);
      const [l, f, d, a] = await Promise.all([get("/admin/lag"), get("/admin/faults"), get("/admin/dlq?status=open"), get("/admin/replay/audit")]);
      setLag(l); setFaults(f); setDlq(d.entries); setAudit(a.audit);
    } catch (e) {
      setErr((e as { status?: number }).status === 401 ? "token 不对（401）" : (e as Error).message);
    }
  }
  useEffect(() => {
    try { sessionStorage.setItem(TOKEN_KEY, token); } catch { /* private mode */ }
    void refresh();
    const t = setInterval(refresh, 2000);
    return () => clearInterval(t);
  }, [token]);

  const setMode = async (body: object) => setFaults(await getJson("/proc/admin/faults", { method: "PUT", headers: { ...auth, "content-type": "application/json" }, body: JSON.stringify(body) }));
  async function dryRun() {
    setResult(null);
    setPlan(await postJson("/proc/admin/replay/plan", shipment.trim() ? { shipmentIds: [shipment.trim()] } : {}, auth));
  }
  async function execute() {
    try { setResult(await postJson("/proc/admin/replay/execute", { dryRunId: plan.dryRunId }, auth)); } catch (e) { setResult({ error: (e as Error).message }); }
    await refresh();
  }

  return (
    <div className="ops">
      <section className="panel row">
        <label>Admin token <input data-testid="ops-token" type="password" value={token} onChange={(e) => setToken(e.target.value)} /></label>
        {err && <span className="error" data-testid="ops-error">{err}</span>}
      </section>
      <div className="cols">
        <section className="panel">
          <h2>队列深度（每个分区的 lag）</h2>
          <div className="bars" data-testid="ops-lag">
            {(lag?.partitions ?? []).map((p: any) => (
              <div key={p.partition} className="bar-row"><span className="mono">p{p.partition}</span><span className="bar"><i style={{ width: `${Math.min(100, p.lag * 5)}%` }} /></span><span className="mono">{p.lag}</span><span className="muted">offset {p.committed}/{p.high}</span></div>
            ))}
          </div>
          <p className="muted">采样时间 {lag?.at ?? "-"}</p>
        </section>
        <section className="panel">
          <h2>故障注入（假 ETA 服务）</h2>
          <p>当前模式：<b data-testid="ops-fault-mode">{faults?.mode ?? "-"}</b></p>
          <div className="actions">
            <button onClick={() => setMode({ mode: "ok" })}>正常</button>
            <button data-testid="fault-error" onClick={() => setMode({ mode: "error" })}>全部 503</button>
            <button onClick={() => setMode({ mode: "slow", latencyMs: 300 })}>慢 300ms</button>
            <button onClick={() => setMode({ mode: "timeout", latencyMs: 3000 })}>挂住</button>
            <button onClick={() => setMode({ mode: "flaky", errorRate: 0.3 })}>三成失败</button>
            <button data-testid="fault-reset" onClick={async () => setFaults(await postJson("/proc/admin/faults/reset", {}, auth))}>重置</button>
          </div>
        </section>
      </div>
      <section className="panel">
        <h2>DLQ（open，{dlq.length} 条）</h2>
        <table className="grid" data-testid="ops-dlq">
          <thead><tr><th>id</th><th>运单</th><th>seq</th><th>原因</th><th>来源</th><th>进入时间</th><th /></tr></thead>
          <tbody>{dlq.map((d) => (
            <Fragment key={d.id}>
              <tr data-dlq-id={d.id}><td>{d.id}</td><td className="mono">{d.shipment_id}</td><td>{d.seq ?? "-"}</td><td>{d.reason}</td><td className="mono">p{d.source_partition}@{d.source_offset}</td><td>{new Date(d.created_at).toLocaleTimeString()}</td>
                <td><button className="link" data-testid={`dlq-expand-${d.id}`} onClick={() => setOpenRow(openRow === d.id ? null : d.id)}>{openRow === d.id ? "收起" : "详情"}</button></td></tr>
              {openRow === d.id && <tr><td colSpan={7}><Json value={d.detail} testId={`dlq-detail-${d.id}`} /></td></tr>}
            </Fragment>
          ))}</tbody>
        </table>
      </section>
      <section className="panel">
        <h2>安全重放</h2>
        <div className="row">
          <input data-testid="replay-shipment" value={shipment} onChange={(e) => setShipment(e.target.value)} placeholder="运单号（留空 = 所有 open 条目）" />
          <button data-testid="ops-dry-run" onClick={dryRun}>dry-run</button>
          <button className="primary" data-testid="ops-execute" disabled={!plan} onClick={execute}>执行</button>
        </div>
        {plan && (
          <table className="grid" data-testid="ops-plan">
            <thead><tr><th>dlqId</th><th>运单</th><th>seq</th><th>原因</th><th>结论</th><th>from → to</th><th>说明</th></tr></thead>
            <tbody>{plan.plan.items.map((i: any) => <tr key={i.dlqId}><td>{i.dlqId}</td><td className="mono">{i.shipmentId}</td><td>{i.seq ?? "-"}</td><td>{i.reason}</td><td><b>{i.verdict}</b></td><td>{i.from ?? "-"} → {i.to ?? "-"}</td><td className="muted">{i.note}</td></tr>)}</tbody>
          </table>
        )}
        {plan && <p className="muted">dry-run <code>{plan.dryRunId}</code>，哈希 <code>{plan.plan.hash}</code>。执行时会重算，哈希不一致就拒绝。</p>}
        {result && <Json value={result} testId="ops-result" />}
        <h3>审计</h3>
        <table className="grid" data-testid="ops-audit">
          <thead><tr><th>id</th><th>类型</th><th>操作人</th><th>基于</th><th>结果</th><th>时间</th></tr></thead>
          <tbody>{audit.map((a) => <tr key={a.replay_id}><td className="mono">{a.replay_id}</td><td>{a.kind}</td><td>{a.actor}</td><td className="mono">{a.based_on ?? "-"}</td><td className="mono">{JSON.stringify(a.counts)}</td><td>{new Date(a.created_at).toLocaleTimeString()}</td></tr>)}</tbody>
        </table>
      </section>
    </div>
  );
}
