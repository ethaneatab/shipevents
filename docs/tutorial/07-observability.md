# 第七章：看得见，才查得到「为什么晚了」

> 讲解视频：[p7-observe](../videos/p7-observe.mp4)

**这一章解决什么问题？** 前六章让系统在各种故障下都不丢、不重、不倒退。但客户打电话来说「签收两小时了，页面还显示派送中」，我们得能回答：晚在哪。
这一章加四类指标（lag、队列深度、吞吐、按原因的失败率）、Prometheus + Grafana，一个延迟拆解接口，还有一份每条都带回归测试的 [runbook](../runbook.md)。

## 「晚」有几种？

同样是晚，原因不同：

1. 这条事件自己调下游慢。
2. 这条事件本身很快，但它在分区里排在一条慢事件后面。
3. 消费者根本没在跑，或者在反复重平衡。

第 2 种最容易误判。只看这条事件的日志，它自己只花了几毫秒，查不出问题。

## 指标怎么选？

`src/obs/metrics.ts`：

| 指标 | 回答什么 |
|---|---|
| `shipevents_events_total{outcome}` | 吞吐，按 applied / stale / rejected / duplicate / dlq 分 |
| `shipevents_failures_total{reason}` | 失败率，按原因分 |
| `shipevents_processing_lag_seconds` | 从 Kafka 收下这条消息到追踪记录提交，花了多久 |
| `shipevents_consumer_lag_messages{partition}` | 队列深度：每个分区还有多少条没处理 |
| `shipevents_downstream_*`、`shipevents_retries_total` | 下游慢不慢、重试多不多 |
| `shipevents_dlq_open_entries{reason}` | DLQ 里还有多少等着处理 |

队列深度是后台任务每两秒算一次（`src/obs/lag.ts`）：分区末尾 offset 减去我们 consumer group 已提交的 offset。

## 一条事件的时间花在哪？

第五章开始，历史表每行都记了 `kafka_ts`（Kafka 收下的时间）、`processed_at`、`attempts`、`downstream_ms`、`trace_id`。
`GET /v1/shipments/:id/latency` 把它们算成：

- `end_to_end_ms` = `processed_at - kafka_ts`
- `queue_and_db_ms` = `end_to_end_ms - downstream_ms`
- `slowest` = 下游占了一半以上就是 `downstream`，否则是 `queue_or_db`

## 真跑一次：我写错的那个测试

假设：把下游调成每次慢 250 毫秒，给同一个运单发四条事件。最后一条应该是「慢在下游」。
测试真跑出来是红的：

```text
× a slow dependency shows up as partition lag, then as downstream time in the latency breakdown
  → expected 'queue_or_db' to be 'downstream'
```

假设错了，代码是对的。最后一条自己确实花了 250 毫秒调下游，但它在分区里排在前三条后面，等了七百多毫秒。排队才是它晚的主因。
这正是上面说的第 2 种「晚」。测试改成了：第一条慢事件 `slowest = downstream`；最后一条 `slowest = queue_or_db`，而且 `queue_and_db_ms > 500`。
同一个测试还断言：处理过程中，这个运单所在分区的 lag 至少到过 2；处理完回到 0。

## 观测栈

`docker-compose.yml` 里的 `prometheus`（9210）和 `grafana`（3210）放在 `obs` profile 里，不需要时不占内存：

```bash
docker-compose -p shipevents --profile obs up -d
```

Prometheus 在容器里，处理器跑在宿主机上，所以抓取地址是 `host.docker.internal:8900`，compose 里配了 `extra_hosts: host-gateway`。
Grafana 匿名只读，dashboard JSON 在 `infra/grafana/dashboards/shipevents.json`，七张图。

实测抓取（发了二十条行程和一条坏消息之后）：

```text
targets: ['up']
sum by (outcome) (shipevents_events_total): applied 120, dlq 1
GET /admin/lag: {"partitions":[{"partition":0,"high":49,"committed":49,"lag":0}, …]}
```

整个项目容器内存实测：Kafka 409 MiB、Grafana 97 MiB、Postgres 27 MiB、Prometheus 19 MiB。

## 日志怎么串起来？

每条处理日志都带 `shipmentId`、`eventId`、`seq`、`partition`、`offset`、`traceId`。从 latency 接口拿到 `trace_id`，`grep` 日志就能看到那次每一次 ETA 尝试和退避的毫秒数。

## 测试

`test/integration/observability.test.ts`：上面那个延迟用例，加一个 `/metrics` 输出里四类指标都在的用例。
`docs/runbook.md` 的每一条都列了它的回归测试。

## 怎么 checkout 到这一步跑起来

```bash
git checkout step-07
docker-compose -p shipevents --profile obs up -d && npm ci
node src/downstream/main.ts & node src/processor/main.ts & node src/api/main.ts &
curl -XPUT localhost:8902/faults -H 'content-type: application/json' -d '{"mode":"slow","latencyMs":300}'
node src/sim/cli.ts journey --shipment SHP-SLOW0001
curl -s localhost:8901/v1/shipments/SHP-SLOW0001/latency
open http://localhost:3210
npm test
```
