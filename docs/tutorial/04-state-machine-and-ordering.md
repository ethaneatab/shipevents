# 第四章：状态机，旧事件只进历史不改状态

> 讲解视频：[p4-state](../videos/p4-state.mp4)

**这一章解决什么问题？** 第一章最后那个 bug：已经签收的包裹，被一条迟到的到站扫描改回了「运输中」。
Kafka 分区内有序，但承运商发出来的顺序本身就可能乱：扫描枪离线攒了一批、对方系统重发、两个数据中心各发一半。
第三章的 dedupe 只认 event id。迟到的是**另一条**事件，dedupe 挡不住。这一章用两样东西挡它：每个运单的序号（`seq`）和一张合法转移表。

## 按什么判断「新」和「旧」？

三个候选：到达顺序、事件时间 `occurredAt`、承运商给的 `seq`。

- 到达顺序就是我们现在的问题，不能用。
- 事件时间来自不同设备的时钟，会漂，两条事件可能同一毫秒。
- `seq` 是承运商为每个运单单调递增的编号。最可靠。

所以规则是：`seq` 小于等于已应用的 `last_seq`，就是旧事件（stale）。写进历史，标记 stale，状态不动。

## 新事件就一定能应用吗？

不一定。`seq` 更大，但动作本身不合法，比如签收以后又来一条到站扫描。这不是乱序，是数据错了，或者我们的规则错了。
这种叫 reject：写历史、标记 rejected，再送进 DLQ 等人看。不能悄悄吞掉，也不能应用。

合法转移表在 `src/domain/stateMachine.ts`：

```ts
export const ALLOWED: Record<Status, readonly Status[]> = {
  CREATED:          ["PICKED_UP", "IN_TRANSIT", "OUT_FOR_DELIVERY", "DELIVERED", "EXCEPTION"],
  PICKED_UP:        ["IN_TRANSIT", "OUT_FOR_DELIVERY", "DELIVERED", "EXCEPTION"],
  IN_TRANSIT:       ["IN_TRANSIT", "OUT_FOR_DELIVERY", "DELIVERED", "EXCEPTION"],
  OUT_FOR_DELIVERY: ["DELIVERED", "EXCEPTION"],
  EXCEPTION:        ["IN_TRANSIT", "OUT_FOR_DELIVERY", "DELIVERED", "EXCEPTION", "RETURNED"],
  DELIVERED: [], RETURNED: [],
};
```

两个取舍：

1. **允许往前跳**。承运商会漏扫，`PICKED_UP` 直接到 `OUT_FOR_DELIVERY` 是正常的。
2. **允许从中间开始**。一个运单的第一条事件就是 `PICKED_UP`（因为 `CREATED` 迟到了），我们照样建档。等 `CREATED` 到了，它是 stale，进历史。

## 判定函数长什么样？

一个纯函数，输入当前状态和事件，输出三种结果之一。没有数据库，没有 Kafka，单元测试直接测：

```ts
// @learn:state-decide
if (current === null) return ENTRY_STATUSES.includes(to) ? apply : reject;
if (event.seq <= current.lastSeq) return { kind: "stale", … };
if (!ALLOWED[current.status].includes(to)) return { kind: "reject", … };
return { kind: "apply", from: current.status, to };
```

它在 `handle.ts` 的事务里被调用，调用前 `SELECT … FOR UPDATE` 锁住这一行。
reject 要进 DLQ，那一步放在 `COMMIT` **之前**：崩在中间的话，事务没提交，消息会重来，DLQ 索引按 offset 唯一，还是一行。

## 真跑一次

假设：发送顺序是 seq 1、2、5、3、4。最后应该停在 `OUT_FOR_DELIVERY`，3 和 4 只进历史。

```text
$ node src/sim/cli.ts out-of-order --shipment SHP-LATE0001
{"seq":5,"decision":{"kind":"apply","from":"PICKED_UP","to":"OUT_FOR_DELIVERY"},"msg":"applied"}
{"seq":3,"decision":{"kind":"stale","from":"OUT_FOR_DELIVERY","to":"IN_TRANSIT","reason":"seq 3 <= last applied seq 5"},"msg":"stale"}
{"seq":4,"decision":{"kind":"stale","from":"OUT_FOR_DELIVERY","to":"IN_TRANSIT","reason":"seq 4 <= last applied seq 5"},"msg":"stale"}

 seq |    event_type    |   status_from    |    status_to     | outcome |           reason
   1 | SHIPMENT_CREATED |                  | CREATED          | applied |
   2 | PICKED_UP        | CREATED          | PICKED_UP        | applied |
   5 | OUT_FOR_DELIVERY | PICKED_UP        | OUT_FOR_DELIVERY | applied |
   3 | HUB_SCAN         | OUT_FOR_DELIVERY | IN_TRANSIT       | stale   | seq 3 <= last applied seq 5
   4 | HUB_SCAN         | OUT_FOR_DELIVERY | IN_TRANSIT       | stale   | seq 4 <= last applied seq 5

$ node src/sim/cli.ts illegal --shipment SHP-ILLEG001
{"seq":7,"decision":{"kind":"reject","from":"DELIVERED","to":"IN_TRANSIT","reason":"illegal transition DELIVERED -> IN_TRANSIT"},"msg":"rejected"}
```

## 测试

- `test/unit/stateMachine.test.ts`：八个用例，包括「同一个 seq 来两条不同事件」「RETURNED 只能从 EXCEPTION 来」「终态没有出口」。
- `test/integration/ordering.test.ts`：五个用例，真 Kafka。乱序扫描、`CREATED` 迟到、签收后又扫描（断言进了 DLQ）、没有异常就退回、异常后退回。

用 `LAB_SABOTAGE=no_transition_guard` 把判定换成「谁最后到谁说了算」（`decideLegacy`），五个里四个变红：

```text
× late hub scans … → expected { status: 'IN_TRANSIT', lastSeq: 4 } to deeply equal { status: 'OUT_FOR_DELIVERY', …(1) }
× a shipment whose CREATED event is late … → expected 'CREATED' to be 'PICKED_UP'
× a newer scan after DELIVERED … → expected 'IN_TRANSIT' to be 'DELIVERED'
× RETURNED without an EXCEPTION first … → expected 'RETURNED' to be 'IN_TRANSIT'
✓ EXCEPTION then RETURNED is legal
```

最后一个是绿的，这是对的：合法路径上，有没有保护结果都一样。

## 怎么 checkout 到这一步跑起来

```bash
git checkout step-04
docker-compose -p shipevents up -d && npm ci
node src/processor/main.ts &
node src/sim/cli.ts out-of-order && node src/sim/cli.ts illegal
npm test
```
