# 第一章：最小能跑的版本，事件进来，状态出去

> 讲解视频：[p1-intake](../videos/p1-intake.mp4)

**这一章解决什么问题？** 承运商不停地推送货运事件（揽收、到站扫描、派送、签收）。客户只想知道一件事：我的包裹现在在哪。
我们先做一条最短的通路：事件写进 Kafka，一个消费者（consumer）读出来，更新一行追踪记录（tracking record），查询 API 把这一行返回给客户。
先让它跑起来，再看它哪里会坏。后面每一章修一个坏法。

## 为什么要按 shipmentId 做 key？

Kafka 只保证**同一个分区（partition）内**有序。
如果同一个运单的事件散在不同分区，三个分区各自消费，「签收」可能比「揽收」先被处理。
所以生产者发消息时把 `shipmentId` 当 key。默认分区器对 key 做 murmur2 哈希，同一个运单永远落在同一个分区。

```ts
// src/sim/produce.ts
await producer.send({ topic, messages: [{ key: e.shipmentId, value, headers }] });
```

这只解决了「Kafka 里的顺序」。承运商那边自己发晚了、发重了，Kafka 管不了。这是第三、四章的事。

## 最小版本长什么样？

三块：

1. `src/sim/` 事件模拟器：按真实的六步造一条行程（journey），seq 从 1 到 6。
2. `src/processor/handle.ts`：解析 JSON，把状态直接覆盖成事件对应的状态，再往历史表追加一行。两步在一个事务里。
3. `src/api/server.ts`：`GET /v1/shipments/:id`，返回追踪记录和历史。

数据库只有两张表（`src/db/migrations/001_init.sql`）：`shipments` 一个运单一行，`shipment_history` 只追加不修改。

## 真跑一次是什么样？

假设：发一条完整行程，六条事件应该落在同一个分区，offset 连续，最后状态是 DELIVERED。

```text
$ node src/sim/cli.ts journey --shipment SHP-DEMO0001
{"scenario":"journey","shipmentId":"SHP-DEMO0001","records":[
  {"eventId":"5f0eeb63-…","partition":1,"offset":"0"}, … ,
  {"eventId":"2baa0634-…","partition":1,"offset":"5"}]}

$ curl -s localhost:8901/v1/shipments/SHP-DEMO0001
{"shipment":{"shipment_id":"SHP-DEMO0001","status":"DELIVERED","last_seq":6,"version":6, …},
 "history":[{"seq":1,"event_type":"SHIPMENT_CREATED","status_from":null,"status_to":"CREATED","kafka_partition":1,"kafka_offset":"0", …}, …]}
```

六条都在分区 1，offset 0 到 5。假设成立。

## 这一版哪里会坏？

故意补发一条迟到的「到站扫描」（seq 3），它在签收之后才到：

```text
$ (补发 seq=3 HUB_SCAN，落在分区 1 offset 6)
$ curl … | 取 status、last_seq、version、历史条数
IN_TRANSIT 3 7 7
```

已经签收的包裹，状态被改回了「运输中」。客户会看到包裹「倒退」。
还有两个坑这里看不出来，但一样存在：

- 同一条事件投递两次（at-least-once 投递很正常），历史表就多一行。
- 消费者用的是 kafkajs 默认的自动提交 offset。处理到一半进程挂了，有的消息会丢。

这三个问题分别在第三章（去重）、第四章（状态机）、第三章（先写库后提交）解决。

## 测试

- `test/unit/journey.test.ts`：模拟器造出的 seq 是 1 到 6，时间单调递增。
- `test/integration/journey.test.ts`：起真 Kafka 和 Postgres，每个测试文件用自己的 topic 和 consumer group，所以测试文件可以并行跑。断言六条事件落在同一个分区，最后状态 DELIVERED，历史六行。

## 怎么 checkout 到这一步跑起来

```bash
git checkout step-01
docker-compose -p shipevents up -d          # Kafka 19210、Postgres 54310
npm ci
node src/processor/main.ts &                # 处理服务，健康检查 8900
node src/api/main.ts &                      # 查询 API 8901
node src/sim/cli.ts journey --shipment SHP-DEMO0001
curl -s localhost:8901/v1/shipments/SHP-DEMO0001
npm test
```
