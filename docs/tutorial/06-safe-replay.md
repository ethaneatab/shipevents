# 第六章：把死信放回去，而且只放一次

> 讲解视频：[p6-replay](../videos/p6-replay.mp4)

**这一章解决什么问题？** 第五章让下游挂掉时事件进 DLQ，分区不被卡住。但 DLQ 只是一个停车场。下游恢复以后，这些事件得回去。
最省事的做法是写个脚本把 DLQ topic 从头读一遍、全部发回主 topic。这样做会出三种事故：
重放了不该重放的（坏字节又进一次 DLQ）；同一批放了两次（两个人同时操作）；放回去的顺序和原来不一样。
这一章做一套**带 dry-run、带审计、走去重**的重放流程。完整的操作手册在 [`docs/replay.md`](../replay.md)。

## 重放要不要绕过消费者，直接改库？

假设：直接 `UPDATE shipments` 最快。
但那样等于写了第二套业务逻辑。状态机、去重、ETA 都要再实现一遍，迟早和主路径不一致。
所以重放只做一件事：**把原始字节、原始 key 发回主 topic**。后面完全走正常的消费路径。好处是白送的：

- 重放两次？第二次 event id 已经在 `processed_events` 里，`duplicate skipped`。
- 重放的事件比当前状态旧？状态机判 stale，只进历史。
- 下游又挂了？再进 DLQ，变成新的 open 条目。

## 顺序怎么保证？

两层。

1. **计划按 seq 排**。DLQ 里的顺序是「进死信的先后」，可能已经乱了。`planReplay` 按 `shipment_id, seq` 排，不按 dlqId。
2. **发送一条一条来**。key 还是 `shipmentId`，所以进同一个分区。每次 `await producer.send(...)` 拿到 broker 确认才发下一条。

## dry-run 为什么是强制的？

dry-run 不只是「看一眼」。`planReplay` 拿当前追踪记录逐条模拟状态机，给每条一个结论，然后把**整个计划的哈希**记进 `replay_audit`。
执行时重新算一遍，哈希对不上就拒绝：

```ts
// @learn:replay-execute
const plan = await planReplay(pool, dry.filter as ReplayFilter);
if (plan.hash !== dry.plan_hash) throw new ReplayRefused("plan_changed", …);
```

这样「你看过的」和「真正执行的」是同一个东西。中间有新死信进来、有人先重放了、追踪记录变了，都会被拦住。
另外有一把 Postgres advisory lock，同一时间只有一个重放在跑。

## 真跑一次

假设：下游挂的时候，SHP-RPLY0001 的三条事件按 seq 1、3、2 的顺序进了 DLQ。dry-run 应该按 seq 排，结论都是 apply。

```text
$ node src/replay/cli.ts plan --shipment SHP-RPLY0001
│ dlqId │ seq │ reason                 │ verdict │ move                      │
│ 8     │ 1   │ 'downstream_exhausted' │ 'apply' │ '- -> CREATED'            │
│ 10    │ 2   │ 'downstream_exhausted' │ 'apply' │ 'CREATED -> PICKED_UP'    │
│ 9     │ 3   │ 'downstream_exhausted' │ 'apply' │ 'PICKED_UP -> IN_TRANSIT' │
dry-run dry-7f47697a (plan 2002cac8e6ebe256)

$ node src/replay/cli.ts execute --dry-run-id dry-7f47697a
{ "replayId": "rpl-7fff91b7", "published": [ {"dlqId":8,"seq":1,…}, {"dlqId":10,"seq":2,…}, {"dlqId":9,"seq":3,…} ] }

 seq | status_from | status_to  | outcome |    replay
   1 |             | CREATED    | applied | rpl-7fff91b7
   2 | CREATED     | PICKED_UP  | applied | rpl-7fff91b7
   3 | PICKED_UP   | IN_TRANSIT | applied | rpl-7fff91b7
```

dlqId 是 8、10、9，seq 是 1、2、3。按 seq 放回去了。
同一个 dry-run 再执行一次，被拒绝：`refused (plan_changed)`。

## 修不了的怎么办？

坏字节、非法转移，重放解决不了。dry-run 标成 `skip_invalid` 或 `skip_already_processed`，执行时跳过，留在 open。
确认上游已经修好重发以后，用 `discard --note "…"` 关掉。理由必填，也进审计表。

## 测试

`test/integration/replay.test.ts`，六个用例：

1. 下游挂了再恢复：三条乱序进 DLQ，dry-run 按 seq 1、2、3 排、什么都不写；执行后三条都 applied，历史带 replay id，审计行 `counts = {published: 3, skipped: 0}`。
2. 手工把同一条死信再发一遍：历史还是一行，`version` 还是 1。
3. dry-run 以后又进来一条新死信：执行被拒（`plan_changed`）；不存在的 dry-run 也被拒。
4. 非法转移和坏 schema：dry-run 分别标 `skip_already_processed` 和 `skip_invalid`，执行时一条都不发。
5. 重放的事件比当前状态旧：结论 stale，状态不倒退。
6. `/admin/*` 不带 token 是 401，带错的也是 401；`/healthz` 不需要。

## 怎么 checkout 到这一步跑起来

```bash
git checkout step-06
docker-compose -p shipevents up -d && npm ci
node src/downstream/main.ts & node src/processor/main.ts &
curl -XPUT localhost:8902/faults -H 'content-type: application/json' -d '{"mode":"error"}'
node src/sim/cli.ts journey --shipment SHP-RPLY0003
curl -XPOST localhost:8902/faults/reset
node src/replay/cli.ts plan --shipment SHP-RPLY0003
node src/replay/cli.ts execute --dry-run-id <上一步输出的 dry-xxxxxxxx>
npm test
```
