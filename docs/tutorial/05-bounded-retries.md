# 第五章：下游会挂，重试要有上限

> 讲解视频：[p5-retry](../videos/p5-retry.mp4)

**这一章解决什么问题？** 追踪记录里要带预计送达时间（ETA）。ETA 来自另一个服务，它会抖、会慢、会整个挂掉。
不重试，一次网络抖动就让客户看不到 ETA。无限重试，下游挂一小时，这个分区就卡一小时，后面所有运单的更新都排着队。
我们要的是**有界重试**：临时失败按指数退避重试几次，超过预算就进 DLQ，分区继续往前走。

## 假下游长什么样？

`src/downstream/server.ts`，端口 8902。`POST /v1/eta` 按状态返回一个 ETA。重点是它能注入故障：

| 开关 | 效果 |
|---|---|
| `PUT /faults {"mode":"error"}` | 每次都 503 |
| `PUT /faults {"mode":"timeout","latencyMs":2000}` | 挂住不回 |
| `PUT /faults {"mode":"flaky","errorRate":0.3}` | 三成概率 503 |
| `POST /faults/fail-next {"shipmentId":"…","count":2}` | 只让这个运单接下来两次失败 |

最后一个是给测试和教学页面用的：只坑一个运单，别的流量不受影响。

## 哪些错误该重试？

`src/processor/etaClient.ts` 把错误分成两类：

- **临时的**：5xx、429、网络错误、超时。再试一次可能就好了。
- **永久的**：其他 4xx。请求本身有问题，再试一百次也是 400。抛 `PermanentError`，立刻停。

## 退避怎么算？

`src/processor/retry.ts`：

```ts
// @learn:backoff
const exp = Math.min(policy.maxDelayMs, policy.baseDelayMs * 2 ** (attempt - 1));
return policy.jitter ? Math.round(exp / 2 + (rand() * exp) / 2) : exp;
```

默认四次尝试，退避 200、400、800 毫秒，封顶 2 秒，每次尝试 800 毫秒超时。
加抖动（jitter）是为了不让一大批消费者在同一毫秒一起重试，把刚恢复的下游再打挂。这里用的是「一半固定、一半随机」。

最坏情况一条消息占住分区多久？四次超时加三次退避：`4 × 800 + 200 + 400 + 800 = 4600` 毫秒。这就是「有界」的意思：上限算得出来。

## 为什么在分区里原地重试，不用重试 topic？

重试 topic 的好处是不堵分区。坏处是同一个运单的后续事件会**越过**正在重试的那条，先被处理。
我们的状态机按 `seq` 判新旧，越过了也不会把状态改错。但「先到的 ETA 被后到的覆盖」这类细节会变复杂。
所以这里选了更简单的：在分区里原地重试，用上限换简单。代价写进 README 的局限里：下游完全挂掉的时候，每条消息都要等满 4.6 秒才进 DLQ，lag 会涨。生产上应该再加熔断（circuit breaker）。

## 下游调用放在事务里面还是外面？

外面。事务里调一个可能挂 800 毫秒的 HTTP，会一直锁着那一行。
顺序是：读一眼 `processed_events`（重复的直接跳过，连下游都不调）→ 预判状态机 → 只有 apply 才调下游 → 开事务，在行锁下**重新**判定一次，以这次为准。

重试耗尽时，事件**不写** `processed_events`。它没被处理，第六章的重放要能处理它。

## 真跑一次

假设 1：下游对这个运单先失败两次。应该第三次成功，日志里能看到退避的毫秒数。

```text
$ node src/sim/cli.ts flaky --shipment SHP-FLAKY001
{"shipmentId":"SHP-FLAKY001","attempt":{"attempt":1,"ok":false,"ms":12,"error":"eta 503: …injected failure…","backoffMs":104},"msg":"eta attempt failed"}
{"shipmentId":"SHP-FLAKY001","attempt":{"attempt":2,"ok":false,"ms":2,"error":"eta 503: …","backoffMs":246},"msg":"eta attempt failed"}
{"shipmentId":"SHP-FLAKY001","attempt":{"attempt":3,"ok":true,"ms":2},"msg":"eta ok"}
{"shipmentId":"SHP-FLAKY001","decision":{"kind":"apply","from":null,"to":"CREATED"},"msg":"applied"}
```

退避 104 和 246，分别落在 [100, 200] 和 [200, 400] 里，抖动生效。

假设 2：下游整个挂掉，发一条完整行程。每条都应该四次以后进 DLQ。

```text
$ curl -XPUT localhost:8902/faults -d '{"mode":"error"}'
$ node src/sim/cli.ts journey --shipment SHP-DOWN0001
{"seq":1,"reason":"downstream_exhausted","attempts":4,"lastError":"eta 503: {\"error\":\"eta service unavailable\"}","msg":"dead-lettered"}

 shipment_id  | seq |        reason        | attempts |                  last_error
 SHP-DOWN0001 |   1 | downstream_exhausted |        4 | eta 503: {"error":"eta service unavailable"}
 …            |   … | …                    |        4 | …
 SHP-DOWN0001 |   5 | downstream_exhausted |        4 | eta 503: {"error":"eta service unavailable"}
```

一个没想到的结果：seq 6（DELIVERED）不需要 ETA，所以它**被应用了**，运单直接从「不存在」跳到 DELIVERED，`last_seq = 6`。
这是第四章「允许从中间开始」的规则在起作用，结果是对的：包裹确实签收了。前五条在 DLQ 里，第六章重放它们时，它们都会变成 stale 进历史。

## 测试

- `test/unit/retry.test.ts`：退避翻倍并封顶、抖动范围、第三次成功、四次后放弃、永久错误不重试、挂住的调用被超时切断。
- `test/integration/downstream.test.ts`：四个场景，都用真 Kafka 和进程内的假下游：
  1. 两次 503 后成功，历史里 `attempts = 3`。
  2. 一直 503：正好四次，进 DLQ，**没有**写 `processed_events`，同一个分区的下一条照常处理。
  3. 挂住：每次 300 毫秒超时（测试用的快速配置），四次后进 DLQ。
  4. 400：只试一次，原因 `downstream_rejected`。

## 怎么 checkout 到这一步跑起来

```bash
git checkout step-05
docker-compose -p shipevents up -d && npm ci
node src/downstream/main.ts &       # 8902
node src/processor/main.ts &
node src/sim/cli.ts flaky
curl -XPUT localhost:8902/faults -H 'content-type: application/json' -d '{"mode":"error"}'
node src/sim/cli.ts journey
curl -XPOST localhost:8902/faults/reset
npm test
```
