import { useCallback, useEffect, useRef, useState } from "react";
import { getJson, postJson, sleep } from "../api";
import { Json } from "../components/Json";
import { VideoSlot } from "../components/VideoSlot";
import { srcAnchor } from "../links";
import { STAGES } from "../learn/stages";

type Kind = "main" | "duplicate" | "stale" | "malformed" | "outage" | "replay";
interface Sample { kind: Kind; shipmentId: string; eventId: string; key: string; body: unknown; partition: number; offset: string }
interface Step { stage: string; status: "ok" | "fail" | "skip" | "retry" | "info"; at: string; summary: string; data?: unknown }
interface Trace { steps: Step[]; rows: Record<string, unknown> }
type StageStatus = "idle" | "active" | "done" | "skip" | "fail" | "info";

const KIND_LABEL: Record<Kind, string> = {
  main: "样例：PICKED_UP（第一次调下游会失败）",
  duplicate: "支线：同一条事件再发一次",
  stale: "支线：迟到的旧事件（seq 1）",
  malformed: "支线：畸形事件（seq 写成字符串）",
  outage: "支线：下游连续失败",
  replay: "重放：从 DLQ 放回主 topic 的那条",
};

function stageStatus(id: string, sample: Sample | null, trace: Trace | null, api: unknown): { status: StageStatus; summary: string; steps: Step[] } {
  if (!sample) return { status: "idle", summary: "", steps: [] };
  const steps = (trace?.steps ?? []).filter((s) => s.stage === (id === "produce" ? "consume" : id));
  if (id === "produce") {
    return { status: "done", summary: `分区 ${sample.partition} · offset ${sample.offset}${steps.length ? " · 已被消费" : " · 等待消费"}`, steps };
  }
  if (id === "api") {
    const finished = trace?.steps.some((s) => s.stage === "write" || s.stage === "dlq" || (s.stage === "dedupe" && s.status === "skip"));
    if (!finished || !api) return { status: finished ? "active" : "idle", summary: "", steps };
    const sh = (api as { shipment?: { status: string; version: number; last_seq: number } }).shipment;
    return { status: "done", summary: sh ? `status ${sh.status} · last_seq ${sh.last_seq} · version ${sh.version}` : "运单还不存在", steps };
  }
  const last = steps.at(-1);
  if (!last) return { status: "idle", summary: "", steps };
  const map: Record<Step["status"], StageStatus> = { ok: "done", fail: "fail", skip: "skip", retry: "active", info: "info" };
  return { status: map[last.status], summary: steps.map((s) => s.summary).join("  →  "), steps };
}

export function Learn() {
  const [sample, setSample] = useState<Sample | null>(null);
  const [trace, setTrace] = useState<Trace | null>(null);
  const [api, setApi] = useState<unknown>(null);
  const [mainShipment, setMainShipment] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [open, setOpen] = useState<string | null>(null);
  const [video, setVideo] = useState<string | null>(null);
  const [plan, setPlan] = useState<any>(null);
  const [replay, setReplay] = useState<any>(null);
  const [error, setError] = useState<string | null>(null);
  const run = useRef(0);

  const follow = useCallback(async (s: Sample) => {
    const me = ++run.current;
    setSample(s);
    setTrace(null);
    setApi(null);
    const qs = new URLSearchParams({ partition: String(s.partition), offset: s.offset, eventId: s.eventId, shipmentId: s.shipmentId });
    const deadline = Date.now() + 30_000;
    while (run.current === me && Date.now() < deadline) {
      const t = await getJson<Trace>(`/proc/learn/trace?${qs}`);
      if (run.current !== me) return;
      setTrace(t);
      const done = t.steps.some((x) => x.stage === "write" || x.stage === "dlq" || (x.stage === "dedupe" && x.status === "skip"));
      if (done) {
        const a = await getJson(`/api/v1/shipments/${s.shipmentId}`).catch(() => ({ shipment: null, history: [] }));
        if (run.current === me) setApi(a);
        return;
      }
      await sleep(250);
    }
  }, []);

  async function submit(kind: Exclude<Kind, "replay">) {
    setBusy(true);
    setError(null);
    setPlan(null);
    setReplay(null);
    try {
      const s = await postJson<Sample>("/proc/learn/sample", { kind, shipmentId: mainShipment });
      if (kind === "main") setMainShipment(s.shipmentId);
      await follow({ ...s, kind });
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  }

  async function dryRun() {
    setPlan(await postJson("/proc/learn/replay/plan", { shipmentId: mainShipment }));
  }

  async function execute() {
    setBusy(true);
    try {
      const r = await postJson("/proc/learn/replay/execute", { dryRunId: plan.dryRunId });
      setReplay(r);
      const p = r.published[0];
      if (p) await follow({ kind: "replay", shipmentId: mainShipment!, eventId: p.eventId, key: mainShipment!, body: null, partition: p.partition, offset: p.offset });
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  }

  useEffect(() => () => { run.current++; }, []);
  const rows = trace?.rows ?? {};
  const dataFor = (id: string, steps: Step[]): Record<string, unknown> => {
    const base: Record<string, unknown> = { steps: steps.map((s) => ({ at: s.at, status: s.status, summary: s.summary, data: s.data })) };
    if (id === "produce" && sample) Object.assign(base, { message: { key: sample.key, partition: sample.partition, offset: sample.offset, value: sample.body } });
    if (id === "dedupe") base.processed_events_row = rows.processed_event;
    if (id === "write") Object.assign(base, { shipment_history_row: rows.history, shipments_row: rows.shipment });
    if (id === "dlq") base.dlq_entries_row = rows.dlq_entry;
    if (id === "api") base.response = api;
    return base;
  };
  const hasDlq = trace?.steps.some((s) => s.stage === "dlq");

  return (
    <div className="learn">
      <section className="panel intro">
        <h1>一条货运事件的一生</h1>
        <p>点「提交样例」，处理器会真的从 Kafka 读到这条消息。每一步亮起来，都是轮询后端拿到的真实状态。教学样例带了慢放头 <code>x-learn-pace-ms: 700</code>，处理器在每一步之间真的停七百毫秒，方便看清楚。</p>
        <div className="actions">
          <button className="primary" data-testid="submit-sample" disabled={busy} onClick={() => submit("main")}>提交样例</button>
          <button data-testid="branch-duplicate" disabled={busy || !mainShipment} onClick={() => submit("duplicate")}>发重复事件</button>
          <button data-testid="branch-stale" disabled={busy || !mainShipment} onClick={() => submit("stale")}>发迟到旧事件</button>
          <button data-testid="branch-malformed" disabled={busy || !mainShipment} onClick={() => submit("malformed")}>发畸形事件</button>
          <button data-testid="branch-outage" disabled={busy || !mainShipment} onClick={() => submit("outage")}>下游宕机</button>
        </div>
        {error && <p className="error" data-testid="learn-error">{error}</p>}
        {sample && (
          <p className="current" data-testid="current-sample">
            <b>{KIND_LABEL[sample.kind]}</b> · 运单 <code data-testid="sample-shipment">{sample.shipmentId}</code> · event <code>{sample.eventId.slice(0, 8)}…</code>
          </p>
        )}
      </section>

      <ol className="timeline" data-testid="timeline">
        {STAGES.filter((s) => s.id !== "dlq" || hasDlq).map((s) => {
          const st = stageStatus(s.id, sample, trace, api);
          const link = srcAnchor(s.anchor);
          return (
            <li key={s.id} className={`stage st-${st.status}`} data-stage={s.id} data-status={st.status} data-testid={`stage-${s.id}`}>
              <div className="dot" />
              <div className="stage-body">
                <div className="stage-head">
                  <h3>{s.title}</h3>
                  <span className="badge" data-testid={`status-${s.id}`}>{statusLabel(st.status)}</span>
                </div>
                <p className="hint">{s.hint}</p>
                {st.summary && <p className="summary" data-testid={`summary-${s.id}`}>{st.summary}</p>}
                <div className="stage-links">
                  <button className="link" data-testid={`expand-${s.id}`} disabled={st.status === "idle"} onClick={() => setOpen(open === s.id ? null : s.id)}>{open === s.id ? "收起数据" : "展开数据"}</button>
                  <a className="src-link" data-testid={`src-${s.id}`} href={link.href} target="_blank" rel="noreferrer" title={link.href}>看源码 · {link.label}</a>
                  <button className="link" data-testid={`video-${s.id}`} onClick={() => setVideo(video === s.id ? null : s.id)}>看讲解视频</button>
                </div>
                {open === s.id && <Json value={dataFor(s.id, st.steps)} testId={`data-${s.id}`} />}
                {video === s.id && <VideoSlot src={`/videos/${s.part}.mp4`} testId={`videoslot-${s.id}`} />}
              </div>
            </li>
          );
        })}
      </ol>

      {((sample?.kind === "outage" && hasDlq) || replay) && (
        <section className="panel replay" data-testid="replay-panel">
          <h2>把它放回去：dry-run → 重放</h2>
          <p>下游恢复了（只坑了这个运单的四次调用）。先 dry-run：按 seq 排序、拿当前状态模拟状态机，一条都不写。</p>
          <div className="actions">
            <button data-testid="dry-run" onClick={dryRun}>dry-run</button>
            <button className="primary" data-testid="execute-replay" disabled={!plan || busy} onClick={execute}>执行重放</button>
          </div>
          {plan && (
            <table className="grid" data-testid="plan-table">
              <thead><tr><th>dlqId</th><th>seq</th><th>原因</th><th>结论</th><th>from → to</th></tr></thead>
              <tbody>{plan.plan.items.map((i: any) => (
                <tr key={i.dlqId} data-verdict={i.verdict}><td>{i.dlqId}</td><td>{i.seq ?? "-"}</td><td>{i.reason}</td><td><b>{i.verdict}</b></td><td>{i.from ?? "-"} → {i.to ?? "-"}</td></tr>
              ))}</tbody>
            </table>
          )}
          {plan && <p className="muted">dry-run id <code data-testid="dry-run-id">{plan.dryRunId}</code> · 计划哈希 <code>{plan.plan.hash}</code></p>}
          {replay && <p data-testid="replay-result">重放 <code>{replay.replayId}</code>：发回 {replay.published.length} 条，跳过 {replay.skipped.length} 条</p>}
        </section>
      )}
    </div>
  );
}

function statusLabel(s: StageStatus): string {
  return { idle: "未开始", active: "进行中", done: "完成", skip: "跳过", fail: "失败", info: "已记录" }[s];
}
