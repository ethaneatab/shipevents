# 第三章：同一条事件来两次，进程半路被杀

> 讲解视频：[p3-dedupe](../videos/p3-dedupe.mp4)

**这一章解决什么问题？** Kafka 的投递语义是至少一次（at-least-once）。生产者重试、消费者重平衡（rebalance）、我们自己崩溃重启，都会让同一条消息再来一次。
第一章的版本收到第二次就再写一遍历史。另外它用 kafkajs 的自动提交，offset 和数据库写入谁先谁后没人管，崩在中间就可能丢。
这一章把两件事一起解决，因为它们是同一个问题的两半：**先写库，后提交 offset；重复来的，靠 event id 挡掉。**

## 去重的记录放哪？

假设：放在 Redis 或内存里行不行？
不行。内存重启就没了。Redis 和 Postgres 是两个系统，「记下 id」和「改状态」之间崩了，两边就不一致：要么 id 记了状态没改（丢），要么状态改了 id 没记（下次重复）。
所以放在同一个 Postgres、**同一个事务**里。新表 `processed_events`，主键就是 `event_id`。

```ts
// src/processor/handle.ts（@learn:dedupe-claim）
const claim = await client.query(
  `INSERT INTO processed_events (event_id, shipment_id, outcome, kafka_partition, kafka_offset)
   VALUES ($1, $2, 'applied', $3, $4) ON CONFLICT (event_id) DO NOTHING RETURNING event_id`, …);
if (claim.rowCount === 0) { await client.query("ROLLBACK"); return "duplicate"; }
```

先「占位」再改状态。占不到，说明别人（或者上一个我）已经做完了，整个事务回滚。
两个消费者同时拿到同一条消息（重平衡的时候会发生）也没事：主键冲突保证只有一个能占到。

## offset 什么时候提交？

kafkajs 默认 `autoCommit: true`，它在后台按时间或条数提交，和我们的数据库写入没有先后关系。
改成 `autoCommit: false`，每条消息处理完自己提交（`src/processor/consumer.ts`）：

```ts
// @learn:commit-order
const outcome = await handleMessage(deps, raw);   // 1. 数据库事务已提交
crashPoint(peekEventId(raw.value));               //    （测试用的崩溃点）
await commit();                                   // 2. 再告诉 Kafka 处理完了
```

崩在 1 和 2 之间会怎样？offset 没提交，重启后这条消息再来一次。dedupe 把它挡掉。结果是不丢、不重。
反过来（先提交再写库）崩在中间呢？Kafka 以为处理完了，数据库里却没有。这条事件永远丢了。这条反面路径保留在 `commitThenProcessLegacy` 里，只有 `LAB_SABOTAGE=commit_before_write` 时才走，用来证明测试是活的。

## 怎么让「崩在中间」可以重复？

随机 `kill -9` 撞不准那几毫秒的缝。
所以加了一个确定性的崩溃点：环境变量 `LAB_CRASH_ON_EVENT=<eventId>`，处理到这条事件、刚做完第一步的时候，进程给自己发 SIGKILL。

真跑一次（第四条事件是崩溃点）：

```text
{"shipmentId":"SHP-CRASH001","partition":0,"offset":"2","to":"IN_TRANSIT","msg":"applied"}
{"shipmentId":"SHP-CRASH001","partition":0,"offset":"3","to":"IN_TRANSIT","msg":"applied"}
LAB_CRASH_ON_EVENT hit for c35343c8-…, killing pid 44640
--- restarted ---
{"shipmentId":"SHP-CRASH001","eventId":"c35343c8-…","partition":0,"offset":"3","msg":"duplicate skipped"}
{"shipmentId":"SHP-CRASH001","partition":0,"offset":"4","to":"OUT_FOR_DELIVERY","msg":"applied"}
{"shipmentId":"SHP-CRASH001","partition":0,"offset":"5","to":"DELIVERED","msg":"applied"}
```

offset 3 写进了库，但没来得及提交。重启后它又来了，被 `duplicate skipped`。

## 测试

- `test/integration/duplicates.test.ts`：同一条 PICKED_UP 发三次。断言这条事件在历史里只有一行，`version` 是 3，`processed_events` 三行。
- `test/integration/restart.test.ts`，两个用例，都是把真的 `src/processor/main.ts` 当子进程起：
  1. 确定性崩溃点。断言六条事件在历史里各出现一次：`[1,1,1,1,1,1]`。
  2. 四十条行程、二百四十条事件，处理到三分之一左右 `kill -9`，重启。断言二百四十条全在、没有一条重复。

一个实测的观察：用 `LAB_SABOTAGE=no_dedupe` 跑，用例 1 变红（`[1,1,1,2,1,1]`），**用例 2 仍然是绿的**。随机杀进程很少正好杀在「写完库、没提交」那几毫秒里。所以用例 2 只能当冒烟，真正守门的是用例 1 的确定性崩溃点。

## 怎么 checkout 到这一步跑起来

```bash
git checkout step-03
docker-compose -p shipevents up -d && npm ci
node src/processor/main.ts &
node src/sim/cli.ts duplicate          # 看日志里的 duplicate skipped
npm test
LAB_SABOTAGE=no_dedupe npx vitest run test/integration/duplicates.test.ts           # 应该红
LAB_SABOTAGE=commit_before_write npx vitest run test/integration/restart.test.ts    # 应该红
```
