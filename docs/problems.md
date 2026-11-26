# 日常问题与解决

做这个项目时真的踩到的坑，加上这类系统在生产上最典型的几种事故（就是失败对照演示的那些）。
每条都标了来源：**【真实踩坑】** 是开发这个仓库时真的发生过的；**【典型事故】** 是用 `LAB_SABOTAGE` 开关在本仓库里复现出来的。
源码链接由 `scripts/learn_anchors.mjs` 生成，行号不手写。逐行讲解视频的脚本在 `video/problems.src.json`。

## q01 随机杀进程的测试，抓不到丢和重 【真实踩坑】
<!-- id: q01-random-kill-misses -->

**现象**：把去重关掉（`LAB_SABOTAGE=no_dedupe`）跑重启测试，期望它红。随机 `kill -9` 那个用例是绿的：

```text
× worker restarts > killed between the database write and the offset commit … → expected [ 1, 1, 1, 2, 1, 1 ] to deeply equal [ 1, 1, 1, 1, 1, 1 ]
✓ worker restarts > SIGKILL in the middle of a burst of 40 journeys (240 events): every event lands exactly once  16229ms
```

**在哪里**：[src/lab/sabotage.ts](../src/lab/sabotage.ts#L15-L21 "anchor:crash-point")（修复后的确定性崩溃点）；修复前只有随机杀的写法，保留在 [docs/problems/snippets/q01-random-kill.ts](problems/snippets/q01-random-kill.ts#L5-L12 "anchor:q01-random-kill-before")。

**怎么定位**：
- 假设一：去重开关没生效。验证：同一次运行里，确定性用例是红的，说明开关生效了。排除。
- 假设二：随机杀没打中那条缝。会出重复的窗口只有「事务已提交、offset 还没提交」那几毫秒。验证：二百四十条事件、处理是串行的，随机一杀几乎都落在事务中间（回滚，不会重复）。成立。

**怎么解决**：加一个按 event id 触发的崩溃点，放在两步之间。

```ts
// 修复前：处理到三分之一时随手 kill -9
first.kill("SIGKILL");
// 修复后：处理到指定事件、刚做完第一步时自杀
const outcome = await handleMessage(deps, raw);
crashPoint(peekEventId(raw.value));   // LAB_CRASH_ON_EVENT=<eventId> → SIGKILL
await commit();
```

**怎么证明修好了**：`test/integration/restart.test.ts` 第一个用例。`no_dedupe` 下红（`[1,1,1,2,1,1]`），`commit_before_write` 下红（`[1,1,1,0,1,1]`），保护全开时绿。随机杀的用例保留，只当冒烟。

**面试怎么讲**：S 重启测试在关掉保护后仍然是绿的。T 要证明测试真的能抓到丢和重。A 加了确定性崩溃点，正好死在写库和提交 offset 之间。R 关保护必红、开保护必绿，现在两种破坏都能抓到。

## q02 刚建好的 topic，第一条消息发不出去 【真实踩坑】
<!-- id: q02-topic-not-ready -->

**现象**：全套测试跑了很多遍，有一次出现「1 skipped」，同时打印：

```text
KafkaJSProtocolError: This server does not host this topic-partition
 ❯ createErrorFromCode node_modules/kafkajs/src/protocol/error.js:581:10
      Tests  34 passed | 1 skipped (35)
```

**在哪里**：[src/kafka/client.ts](../src/kafka/client.ts#L23-L34 "anchor:topic-ready")；修复前的写法在 [docs/problems/snippets/q02-create-topics.ts](problems/snippets/q02-create-topics.ts#L3-L10 "anchor:q02-create-topics-before")。

**怎么定位**：
- 每个测试文件都会新建自己的 topic，三个进程并行建。
- 报错的意思是 broker 还不认这个分区。假设：`createTopics({ waitForLeaders: true })` 返回时，元数据还没在所有地方生效。
- 只复现到一次，没法稳定复现。所以修法要做成「等条件成立」，而不是「多 sleep 一会儿」。

**怎么解决**：

```ts
// 修复前
await admin.createTopics({ waitForLeaders: true, topics: … });
// 修复后：再轮询元数据，直到分区数对得上、每个分区都有 leader（最多 15 秒）
const md = await admin.fetchTopicMetadata({ topics: missing });
const ready = md?.topics.every((t) => t.partitions.length === numPartitions && t.partitions.every((p) => p.leader >= 0));
```

**怎么证明修好了**：老实说，偶发问题没有一个「修复前必红」的测试。证据只有：修复后连续三轮全套测试 `35 passed`，之后所有轮次也没再出现。这一点在面试里要如实说。

**面试怎么讲**：S 测试偶发失败，一次。T 不想靠重跑糊过去。A 读懂报错，判断是「创建成功」和「可用」之间有时间差，改成等条件成立。R 之后没再出现；同时承认偶发问题只能用观察证明。

## q03 我以为慢在下游，其实是在排队 【真实踩坑】
<!-- id: q03-queue-not-downstream -->

**现象**：观测测试第一次跑就红了：

```text
× tracing a delayed update > a slow dependency shows up as partition lag, then as downstream time in the latency breakdown
  → expected 'queue_or_db' to be 'downstream' // Object.is equality
```

**在哪里**：[src/api/server.ts](../src/api/server.ts#L18-L34 "anchor:latency-breakdown")（延迟拆解接口）；我写错的断言在 [docs/problems/snippets/q03-wrong-assertion.ts](problems/snippets/q03-wrong-assertion.ts#L3-L7 "anchor:q03-assert-before")。

**怎么定位**：
- 假设一：拆解接口算错了。验证：手算最后一条，下游二百五十多毫秒，端到端一千毫秒左右。接口没算错。
- 假设二：我的假设错了。最后一条在同一个分区里排在前三条后面，每条都要等二百五十毫秒的下游。成立。

**怎么解决**：代码不动，改断言，让测试守住正确的结论。

```ts
// 修复前
expect(last.slowest).toBe("downstream");
// 修复后
expect(first.slowest).toBe("downstream");     // 第一条慢事件：慢在下游
expect(last.slowest).toBe("queue_or_db");     // 最后一条：慢在排队
expect(last.queue_and_db_ms).toBeGreaterThan(2 * 250);
```

**怎么证明修好了**：`test/integration/observability.test.ts` 现在是绿的，而且它断言的是「排队是主因」这个真实现象。

**面试怎么讲**：S 客户说更新晚了，第一反应是怪下游。T 搞清楚时间到底花在哪。A 做了按事件的延迟拆解，发现后面的事件主要在排队。R 排查方向从「调下游」变成「看分区 lag 和前面卡住的那条」，写进了 runbook R1。

## q04 启动脚本跑完了，终端却不返回 【真实踩坑】
<!-- id: q04-demo-hangs -->

**现象**：`./scripts/demo.sh 2>&1 | tail -9` 三分钟不结束；可是四个服务的日志都已经 listening。`frontend.pid` 里的进程是 `bash ./scripts/demo.sh`，不是 vite。

**在哪里**：[scripts/demo.sh](../scripts/demo.sh#L47-L49 "anchor:frontend-start")；修复前的写法在 [docs/problems/snippets/q04-demo-start.sh](problems/snippets/q04-demo-start.sh#L3-L5 "anchor:q04-frontend-start-before")。

**怎么定位**：
- 看 `pgrep -P <frontend.pid>`：它的子进程才是 `node …/vite.js`。所以 PID 文件记的是子壳。
- `cd frontend && node … &` 整个被放进后台子壳，`$!` 是子壳。
- 子壳自己的 stdout 没重定向，继承了管道，`tail` 一直等不到 EOF。

**怎么解决**：

```bash
# 修复前
(cd frontend && … node node_modules/vite/bin/vite.js … >log 2>&1 & echo $! >frontend.pid)
# 修复后：exec 让子壳变成 vite；整个子壳重定向
(cd frontend && exec env … node node_modules/vite/bin/vite.js …) >"$RT/logs/frontend.log" 2>&1 &
echo $! >"$RT/frontend.pid"
```

`stop.sh` 只按 PID 文件杀，杀之前还核对命令行是不是我们的进程。

**怎么证明修好了**：`demo.sh` 正常退出（exit 0），`stop.sh` 按号停掉四个进程；最后验收时从空数据卷跑通。

**面试怎么讲**：S 一键启动脚本挂住，而且停止脚本会杀错进程。T 保证只停自己起的进程。A 查进程树，发现记的是子壳的号，用 exec 和整体重定向修掉。R 启动脚本能退出，停止只按核对过的 PID。

## q05 已签收的包裹，又变回运输中 【典型事故】
<!-- id: q05-status-goes-back -->

**现象**：第一章的版本真的跑出来过：

```text
$ (补发 seq=3 HUB_SCAN)
$ curl … | 取 status、last_seq、version、历史条数
IN_TRANSIT 3 7 7
```

**在哪里**：[src/domain/stateMachine.ts](../src/domain/stateMachine.ts#L32-L51 "anchor:state-decide")；修复前的逻辑保留为 [decideLegacy](../src/domain/stateMachine.ts#L55-L58 "anchor:state-legacy")（`LAB_SABOTAGE=no_transition_guard`）。

**怎么定位**：
- 假设：是重复投递。验证：迟到的事件 event id 是新的，`processed_events` 里没有。排除。
- 假设：Kafka 乱序。验证：同一个 key，offset 递增，Kafka 没乱。排除。
- 结论：承运商发晚了。只看到达顺序，旧事件就会覆盖新状态。

**怎么解决**：

```ts
// 修复前：谁最后到谁说了算
return { kind: "apply", from: current?.status ?? null, to: TARGET_STATUS[event.type] };
// 修复后
if (event.seq <= current.lastSeq) return { kind: "stale", … };            // 只进历史
if (!ALLOWED[current.status].includes(to)) return { kind: "reject", … };   // 进 DLQ
return { kind: "apply", from: current.status, to };
```

**怎么证明修好了**：`test/integration/ordering.test.ts`。打开 `no_transition_guard`，五个用例红四个（例如 `expected 'IN_TRANSIT' to be 'DELIVERED'`）；关掉，全绿。

**面试怎么讲**：S 客户看到已签收的包裹倒退。T 让迟到事件可以被记录但不能回退状态。A 用承运商的 seq 判新旧，加合法转移表，非法的进 DLQ。R 乱序测试守住，关保护必红。

## q06 重启以后少了一条事件 【典型事故】
<!-- id: q06-lost-on-restart -->

**现象**：`LAB_SABOTAGE=commit_before_write` 下的重启测试：

```text
× worker restarts > killed between the database write and the offset commit: nothing lost, nothing doubled
  → expected [ 1, 1, 1, +0, 1, 1 ] to deeply equal [ 1, 1, 1, 1, 1, 1 ]
```

**在哪里**：[src/processor/consumer.ts](../src/processor/consumer.ts#L73-L79 "anchor:commit-order")；反面路径 [commitThenProcessLegacy](../src/processor/consumer.ts#L83-L88 "anchor:commit-legacy")。

**怎么定位**：
- 第四条事件 Kafka 里有，数据库里没有。
- 看提交顺序：先 `commit()`，后写库。崩溃点在两者之间触发。Kafka 认为处理完了，不会再投递。

**怎么解决**：

```ts
// 修复前
await commit();
return handleMessage(deps, raw);
// 修复后
const outcome = await handleMessage(deps, raw);
await commit();
```

**怎么证明修好了**：同一个测试，正确顺序下 `[1,1,1,1,1,1]`；日志里能看到重投的那条被 `duplicate skipped`。

**面试怎么讲**：S 重启后偶尔丢事件。T 保证不丢。A 改成先写库后提交 offset，重投靠 event id 去重吸收。R 确定性崩溃测试证明不丢不重。

## q07 同一条事件，历史里出现三次 【典型事故】
<!-- id: q07-duplicate-history -->

**现象**：`LAB_SABOTAGE=no_dedupe` 下：

```text
× duplicate delivery > the same event delivered three times changes the record once and appears once in history
  → expected 3 to be 1 // Object.is equality
```

**在哪里**：[src/processor/handle.ts](../src/processor/handle.ts#L62-L70 "anchor:dedupe-claim")；第一章没有去重的写法在 [docs/problems/snippets/q07-no-dedupe.ts](problems/snippets/q07-no-dedupe.ts#L3-L8 "anchor:q07-no-dedupe-before")。

**怎么定位**：
- 三行历史的 `event_id` 一样、offset 不同：是上游重发，不是我们重复消费。
- 我们没有任何「这条处理过」的记录。

**怎么解决**：

```ts
// 修复前：来一次写一次
await client.query(`INSERT INTO shipment_history …`);
// 修复后：同一事务里先占位
INSERT INTO processed_events (event_id, …) VALUES (…) ON CONFLICT (event_id) DO NOTHING RETURNING event_id
// rowCount = 0 → ROLLBACK，返回 duplicate
```

**怎么证明修好了**：`test/integration/duplicates.test.ts`：历史一行、`version = 3`、`processed_events` 三行。手工重发重放过的死信也只生效一次（`replay.test.ts`）。

**面试怎么讲**：S 上游重发导致历史重复。T 幂等处理。A event id 去重表，和状态更新同一事务，主键冲突保证并发也只有一个赢。R 重复投递测试守住，关保护必红。

## q08 一条坏消息卡住整个分区 【典型事故】
<!-- id: q08-poison-message -->

**现象**：第一章的处理器遇到 `{oops` 时，`JSON.parse` 抛异常，kafkajs 反复重试同一个 offset，这个分区后面的事件全部排队。

**在哪里**：[src/events/schema.ts](../src/events/schema.ts#L28-L58 "anchor:schema-decode")；第一章的写法在 [docs/problems/snippets/q08-parse.ts](problems/snippets/q08-parse.ts#L3-L8 "anchor:q08-parse-before")。

**怎么定位**：
- Grafana「Queue depth」里只有一个分区的 lag 只涨不降，「Throughput」接近零。
- 日志里同一个 offset 的异常反复出现。

**怎么解决**：

```ts
// 修复前
const event = JSON.parse(String(msg.value)) as ShipmentEvent;
// 修复后：从不抛异常，返回事件或原因
const decoded = decodeEvent(msg.value, msg.key);
if (!decoded.ok) await sendToDlq(…, { reason: decoded.reason, detail: { issues: decoded.issues }, … });
```

**怎么证明修好了**：`test/integration/malformed.test.ts`：同一个分区依次发「好、坏、非 JSON、好」，最后那条好的照样被应用；死信带原因和原始字节。

**面试怎么讲**：S 一条坏消息让一个分区停摆。T 坏消息不能阻塞好消息。A 解码不抛异常，坏消息带原因进 DLQ。R 分区持续前进，DLQ 里能直接看到哪个字段错了。
