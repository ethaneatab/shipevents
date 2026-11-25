# 安全重放流程（Safe replay procedure）

DLQ 里的事件不会自己回来。把它们放回去要走下面这套流程，每一步都有记录。
代码在 `src/replay/replay.ts`，命令行在 `src/replay/cli.ts`，运维页面（`/ops`）调的是同一套函数（`POST /admin/replay/*`）。

## 什么时候可以重放

| DLQ 原因 | 能不能重放 | 先做什么 |
|---|---|---|
| `downstream_exhausted` | 能 | 确认下游恢复：`curl localhost:8902/healthz`，看 Grafana 上 `shipevents_downstream_requests_total` 的 5xx 已经停了 |
| `downstream_rejected` | 修了请求以后能 | 4xx 说明请求本身有问题，先查 `detail.lastError` |
| `illegal_transition` | 一般不能 | 事件 id 已经记成 rejected，dry-run 会显示 `skip_already_processed`。要么上游改数据重发（新 event id），要么 discard |
| `schema_invalid` / `invalid_json` / `unsupported_version` / `key_mismatch` | 不能 | 原始字节就是坏的。让上游修了重发，然后 discard 这条 |

## 步骤

### 1. 看清楚有什么

```bash
node src/replay/cli.ts list
```

### 2. dry-run（必须）

```bash
node src/replay/cli.ts plan --shipment SHP-RPLY0001 --actor <你的名字>
```

dry-run 做的事：

- 只取 `status = 'open'` 的条目，按运单分组，组内按 `seq` 排序。不是按进 DLQ 的先后排。
- 拿**当前**的追踪记录，逐条模拟状态机。一条写操作都没有。
- 每条给一个结论：`apply`（会改状态）、`stale`（只进历史）、`reject`、`skip_invalid`（字节是坏的）、`skip_already_processed`（event id 已经处理过）。
- 把整个计划和它的哈希写进 `replay_audit`（`kind = 'dry_run'`），返回一个 `dry-xxxxxxxx`。

### 3. 执行

```bash
node src/replay/cli.ts execute --dry-run-id dry-xxxxxxxx --actor <你的名字>
```

执行前会重新算一遍计划。下面任何一条不满足就拒绝（退出码 3，HTTP 409）：

- `dry_run_required`：没有这个 dry-run。
- `dry_run_expired`：dry-run 超过十五分钟。
- `plan_changed`：重新算出来的计划和你看过的不一样（有新的死信进来、追踪记录变了、或者这些条目已经被别人重放了）。
- `replay_in_progress`：别人正在重放（Postgres advisory lock，同一时间只有一个）。

通过以后：

- 只发 `apply` 和 `stale` 的条目。其他的留在 `open`，结果里列在 `skipped`。
- 原始字节、原始 key（就是 `shipmentId`）原样发回主 topic，加两个 header：`x-replay-id`、`x-dlq-entry-id`。同一个 key 进同一个分区；一条发完、broker 确认了才发下一条，所以分区里的顺序就是计划的顺序。
- **不直接改追踪记录。** 重放的事件和普通事件走同一条消费路径：去重、状态机、下游、写库。重放两次也只生效一次。
- 条目标成 `replayed`，写上 `replay_id`。历史表里这次重放带来的每一行，`detail.replayId` 都是它。

### 4. 核对

```bash
curl -s localhost:8901/v1/shipments/SHP-RPLY0001     # 状态和历史
docker exec shipevents-postgres-1 psql -U shipevents -c \
  "select replay_id, kind, actor, based_on, status, counts from replay_audit order by created_at desc limit 5"
```

如果下游又挂了，重放的事件会**重新**进 DLQ，变成新的 `open` 条目（来源 offset 是重放那次的）。不会丢。

### 5. 关掉修不了的

```bash
node src/replay/cli.ts discard --id 42 --note "carrier resent with a new event id, ticket OPS-123" --actor <你的名字>
```

`--note` 必填。写进 `replay_audit`（`kind = 'discard'`）。

## 一次真实的记录

下游挂掉期间，运单 SHP-RPLY0001 的三条事件**乱序**进了 DLQ（到达顺序是 seq 1、3、2，所以 dlqId 是 8、9、10）。dry-run 按 seq 排，而不是按 dlqId：

```text
│ dlqId │ shipment       │ seq │ reason                 │ verdict │ move                      │
│ 8     │ 'SHP-RPLY0001' │ 1   │ 'downstream_exhausted' │ 'apply' │ '- -> CREATED'            │
│ 10    │ 'SHP-RPLY0001' │ 2   │ 'downstream_exhausted' │ 'apply' │ 'CREATED -> PICKED_UP'    │
│ 9     │ 'SHP-RPLY0001' │ 3   │ 'downstream_exhausted' │ 'apply' │ 'PICKED_UP -> IN_TRANSIT' │
dry-run dry-7f47697a (plan 2002cac8e6ebe256)
```

执行后的历史，三行都带同一个 replay id：

```text
 seq | status_from | status_to  | outcome |    replay
   1 |             | CREATED    | applied | rpl-7fff91b7
   2 | CREATED     | PICKED_UP  | applied | rpl-7fff91b7
   3 | PICKED_UP   | IN_TRANSIT | applied | rpl-7fff91b7
```

另一个运单 SHP-RPLY0002：下游挂的时候 seq 1 到 5 进了 DLQ，seq 6（DELIVERED，不需要 ETA）照常应用了。dry-run 的结论是五条全是 `stale`；执行后它们只进历史，状态停在 DELIVERED。
同一个 dry-run 再执行一次：

```text
refused (plan_changed): plan changed since dry-90d571aa (9cd963e46529e4dd -> 4f53cda18c2baa0c), run the dry-run again
```

条目已经不是 `open`，计划变成空的，哈希对不上，所以拒绝。
