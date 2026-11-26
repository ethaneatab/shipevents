// The /learn timeline. Each stage maps to a real processor step, a @learn source anchor,
// a video part (video/parts.json) and a tutorial chapter.
export interface StageDef {
  id: string;
  title: string;
  hint: string;
  anchor: string;
  part: string;
}

export const STAGES: StageDef[] = [
  { id: "produce", title: "生产与分区", hint: "按 shipmentId 做 key，落到哪个分区、哪个 offset", anchor: "produce-key", part: "p1-intake" },
  { id: "schema", title: "Schema 校验", hint: "zod 校验字段和版本，坏消息不抛异常", anchor: "schema-decode", part: "p2-schema" },
  { id: "dedupe", title: "去重检查", hint: "event id 有没有处理过", anchor: "dedupe-claim", part: "p3-dedupe" },
  { id: "state", title: "状态机判定", hint: "from → to，或者 stale / reject 的原因", anchor: "state-decide", part: "p4-state" },
  { id: "downstream", title: "调用下游 ETA", hint: "有界重试，失败超预算进 DLQ", anchor: "retry-loop", part: "p5-retry" },
  { id: "write", title: "写追踪记录", hint: "同一个事务：去重表 + 状态 + 历史，然后才提交 offset", anchor: "write-record", part: "p3-dedupe" },
  { id: "api", title: "客户 API 返回", hint: "GET /v1/shipments/:id 读到的那一行", anchor: "api-shipment", part: "p7-observe" },
  { id: "dlq", title: "死信队列（支线）", hint: "坏消息、重试耗尽、非法转移都停在这里", anchor: "dlq-send", part: "p6-replay" },
];
