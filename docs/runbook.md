# Runbook

每一条都对应一个回归测试：事故先变成一个会红的测试，再修。表在最后。
看板：Grafana `http://localhost:3210`（dashboard「shipevents processor」），Prometheus `http://localhost:9210`。

## R1. 客户说「我的状态更新晚了」

1. 查这个运单的延迟拆解：`curl localhost:8901/v1/shipments/<id>/latency`。每条事件有 `kafka_partition`、`end_to_end_ms`、`downstream_ms`、`queue_and_db_ms`、`slowest`。
2. `slowest = downstream`：这条自己慢在 ETA 服务上。看「Downstream attempt p99」和「retries」两张图。
3. `slowest = queue_or_db`：它在分区里排队，前面有别的事件卡住了。看「Queue depth」里**这个分区**的 lag，再看同一时间段的 downstream 图。通常是前面的事件在重试。
4. 用 `trace_id` 在处理器日志里搜：`grep <trace_id> .runtime/logs/processor.log`，能看到每次 ETA 尝试和退避的毫秒数。

回归测试：`test/integration/observability.test.ts`「a slow dependency shows up as partition lag, then as downstream time」。

## R2. 客户看到状态倒退（已签收变回运输中）

1. 查历史：`curl localhost:8901/v1/shipments/<id>`。倒退前后两条的 `seq` 和 `outcome`。
2. 如果旧事件是 `applied` 而不是 `stale`，状态机保护失效了。检查是不是有人设了 `LAB_SABOTAGE`，或者改了 `src/domain/stateMachine.ts`。

回归测试：`test/integration/ordering.test.ts`（关掉保护时五个里四个变红）。

## R3. 同一条事件在历史里出现两次

1. `select event_id, count(*) from shipment_history group by 1 having count(*) > 1`。
2. 查 `processed_events` 里有没有这个 id。没有：去重被绕过了。有：看是不是有人绕过消费者直接写库（重放必须走 topic，见 `docs/replay.md`）。

回归测试：`test/integration/duplicates.test.ts`；`test/integration/replay.test.ts`「replaying the same events twice」。

## R4. 处理器重启后少了事件

1. 对比 Kafka 里这个运单的消息数和 `shipment_history` 行数。
2. 检查 offset 提交顺序：必须先写库后提交（`processThenCommit`）。

回归测试：`test/integration/restart.test.ts`（确定性崩溃点 + SIGKILL 突发）。

## R5. DLQ 突然涨了（downstream_exhausted）

1. 「Failure rate by reason」里看是哪个 reason。`downstream_exhausted` 说明 ETA 服务在出错或超时。
2. 下游恢复以后，按 `docs/replay.md` 做 dry-run 再重放。不要手工批量发回 topic。

回归测试：`test/integration/downstream.test.ts`；`test/integration/replay.test.ts`「dependency down, then back」。

## R6. 一个分区完全不动了

1. 「Queue depth」里某个分区 lag 只涨不降，「Throughput」接近零。
2. 看日志里有没有同一个 offset 反复出现的异常。毒丸消息应该进 DLQ（`schema_invalid`、`invalid_json`），不应该抛异常。

回归测试：`test/integration/malformed.test.ts`「keeps consuming the same partition」。

## R7. 重放被拒绝

`plan_changed`：dry-run 之后情况变了，重新 dry-run，看一眼新计划再执行。`replay_in_progress`：等另一个重放结束。见 `docs/replay.md`。

回归测试：`test/integration/replay.test.ts`「execute refuses without a matching, fresh dry-run」。

## 对应关系

| Runbook | 回归测试 |
|---|---|
| R1 更新晚了 | `test/integration/observability.test.ts` |
| R2 状态倒退 | `test/integration/ordering.test.ts`，`test/unit/stateMachine.test.ts` |
| R3 历史重复 | `test/integration/duplicates.test.ts`，`test/integration/replay.test.ts` |
| R4 重启丢事件 | `test/integration/restart.test.ts` |
| R5 DLQ 涨了 | `test/integration/downstream.test.ts`，`test/integration/replay.test.ts` |
| R6 分区卡住 | `test/integration/malformed.test.ts`，`test/unit/schema.test.ts` |
| R7 重放被拒 | `test/integration/replay.test.ts` |
