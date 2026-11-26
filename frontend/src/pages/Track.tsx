import { useEffect, useState } from "react";
import { getJson } from "../api";

const STEPS = ["CREATED", "PICKED_UP", "IN_TRANSIT", "OUT_FOR_DELIVERY", "DELIVERED"];
const LABEL: Record<string, string> = { CREATED: "已建单", PICKED_UP: "已揽收", IN_TRANSIT: "运输中", OUT_FOR_DELIVERY: "派送中", DELIVERED: "已签收", EXCEPTION: "异常", RETURNED: "已退回" };

export function Track() {
  const initial = new URLSearchParams(window.location.search).get("id") ?? "";
  const [id, setId] = useState(initial);
  const [data, setData] = useState<any>(null);
  const [lat, setLat] = useState<any>(null);
  const [err, setErr] = useState<string | null>(null);

  async function load(target = id) {
    setErr(null);
    try {
      setData(await getJson(`/api/v1/shipments/${encodeURIComponent(target.trim())}`));
      setLat(await getJson(`/api/v1/shipments/${encodeURIComponent(target.trim())}/latency`));
    } catch (e) {
      setData(null);
      setLat(null);
      setErr((e as { status?: number }).status === 404 ? "没有这个运单" : (e as Error).message);
    }
  }
  useEffect(() => { if (initial) void load(initial); }, []);

  const s = data?.shipment;
  const idx = s ? STEPS.indexOf(s.status) : -1;
  return (
    <div className="track">
      <section className="panel">
        <h1>运单追踪</h1>
        <form className="row" onSubmit={(e) => { e.preventDefault(); void load(); }}>
          <input data-testid="track-input" value={id} onChange={(e) => setId(e.target.value)} placeholder="运单号，例如 SHP-DEMO0001" />
          <button className="primary" data-testid="track-go">查询</button>
        </form>
        {err && <p className="error" data-testid="track-error">{err}</p>}
      </section>
      {s && (
        <section className="panel label-card" data-testid="track-card">
          <div className="label-top">
            <div><div className="muted">运单号</div><div className="mono big">{s.shipment_id}</div></div>
            <div className="status-big" data-testid="track-status" data-status={s.status}>{LABEL[s.status] ?? s.status}</div>
          </div>
          <div className="progress">{STEPS.map((x, i) => <span key={x} className={i <= idx ? "on" : ""}>{LABEL[x]}</span>)}</div>
          <div className="facts">
            <div><span className="muted">最新位置</span> {s.last_location ?? "-"}</div>
            <div><span className="muted">预计送达</span> {s.eta_at ? new Date(s.eta_at).toLocaleString() : "-"}</div>
            <div><span className="muted">更新次数</span> {s.version}</div>
          </div>
          <h3>时间线</h3>
          <ul className="history" data-testid="track-history">
            {data.history.map((h: any) => (
              <li key={h.id} className={`h-${h.outcome}`}>
                <span className="mono">seq {h.seq}</span> <b>{LABEL[h.status_to] ?? h.status_to}</b> <span className="muted">{h.detail?.location ?? ""}</span>
                <span className={`pill p-${h.outcome}`}>{h.outcome}</span>
                {h.detail?.reason && <span className="muted"> · {h.detail.reason}</span>}
              </li>
            ))}
          </ul>
          {lat && (
            <>
              <h3>延迟拆解（每条事件的时间花在哪）</h3>
              <table className="grid" data-testid="latency-table">
                <thead><tr><th>seq</th><th>分区</th><th>端到端 ms</th><th>下游 ms</th><th>排队+写库 ms</th><th>重试</th><th>主因</th></tr></thead>
                <tbody>{lat.events.map((e: any) => (
                  <tr key={e.event_id}><td>{e.seq}</td><td>{e.kafka_partition}</td><td>{e.end_to_end_ms}</td><td>{e.downstream_ms}</td><td>{e.queue_and_db_ms}</td><td>{e.attempts}</td><td>{e.slowest}</td></tr>
                ))}</tbody>
              </table>
            </>
          )}
        </section>
      )}
    </div>
  );
}
