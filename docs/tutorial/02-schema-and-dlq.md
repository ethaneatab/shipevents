# 第二章：先验货，坏消息进死信队列

> 讲解视频：[p2-schema](../videos/p2-schema.mp4)

**这一章解决什么问题？** 第一章的处理器直接 `JSON.parse` 然后信任里面的字段。承运商只要发来一条坏消息，比如 `seq` 写成了字符串，或者干脆不是 JSON，`JSON.parse` 就抛异常。
kafkajs 会重试这条消息，再抛，再重试。这个分区后面的所有事件都被卡住了。这叫毒丸消息（poison message）。
我们要的是：坏消息带着原因挪到一边（dead-letter queue，DLQ），分区继续往前走。

## 校验放在哪一层？

放在最前面，在碰数据库之前。`src/events/schema.ts` 的 `decodeEvent` 把字节变成内部事件，或者给出一个原因。它**从不抛异常**。
四种原因：

| reason | 什么时候 |
|---|---|
| `invalid_json` | 根本不是 JSON |
| `unsupported_version` | `schemaVersion` 不是 1 或 2 |
| `schema_invalid` | 字段缺了、类型错了、枚举外的事件类型 |
| `key_mismatch` | Kafka key 和 `shipmentId` 不一致 |

最后一条容易漏。key 决定分区，分区决定顺序。key 错了，这个运单的事件会跑到别的分区，顺序就悄悄没了。所以它也算坏消息。

## 为什么用 zod，而且要有版本号？

zod 的 schema 本身就是 TypeScript 类型，校验通过以后的对象不用再 `as` 一次。
版本号让我们能同时接两种格式。v2 把 `location` 换成了 `facility: {code, city}`。解码器把两种都变成同一个内部形状，后面的代码只认内部形状。

```ts
const location = d.schemaVersion === 2
  ? (d.facility ? `${d.facility.city} (${d.facility.code})` : undefined)
  : d.location;
```

## 死信里要放什么？

假设：运维半夜看到一条死信，他要能不翻日志就知道三件事：为什么坏、原始字节是什么、它原来在哪个分区哪个 offset。
所以 DLQ 消息体带上完整上下文：`reason`、`detail.issues`、`source.{topic, partition, offset, key, value, headers}`。

## 写 topic 还是写表？

两个都写。`src/processor/dlq.ts`：

1. 先发到 `shipments.dlq` topic。这是持久的那份，别的团队也能订阅。
2. 再往 `dlq_entries` 表插一行。这是运维页面列表、第六章 dry-run 和重放用的索引。

如果在两步之间崩了呢？原消息的 offset 还没提交，会被重新投递。表上有 `UNIQUE (source_topic, source_partition, source_offset)`，所以表里还是一行。topic 里可能多一份，重放时按 eventId 去重，无害。

## 真跑一次

```text
$ node src/sim/cli.ts malformed --shipment SHP-BAD00001
{"scenario":"malformed","shipmentId":"SHP-BAD00001","records":[{"eventId":"(raw)","partition":2,"offset":"0"}]}

# 处理器日志
{"level":40,"reason":"schema_invalid","issues":["seq: Invalid input: expected number, received string"],
 "partition":2,"offset":"0","dlq":{"partition":2,"offset":"0"},"msg":"dead-lettered"}

# dlq_entries
 id | shipment_id  |     reason     |                         detail                          | source_partition | source_offset | status
  1 | SHP-BAD00001 | schema_invalid | {"issues": ["seq: Invalid input: expected number, …"]}  |                2 |             0 | open
```

## 测试

- `test/unit/schema.test.ts`：六个用例，覆盖 v1、v2、非 JSON、未知版本、多个字段同时错（每个错都带路径）、key 不一致。
- `test/integration/malformed.test.ts`：同一个分区里依次发「好、坏、非 JSON、好」。断言最后那条好的照样被处理（分区没卡住），表里两行死信原因正确，DLQ topic 里的消息带着原始字节和原始 offset。

## 怎么 checkout 到这一步跑起来

```bash
git checkout step-02
docker-compose -p shipevents up -d && npm ci
node src/processor/main.ts &
node src/sim/cli.ts malformed
docker exec shipevents-postgres-1 psql -U shipevents -c "select reason, detail from dlq_entries"
npm test
```
