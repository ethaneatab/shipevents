# 面试题：shipevents

面试里一般会问的问题，按类别分组。每题先给「一句话答案」，再「展开」，最后是追问和答法。
答案都落在这个仓库的代码和实测数据上。源码链接由 `scripts/learn_anchors.mjs` 生成。
**仓库实测数字和简历数字是两回事**：简历上的是公司生产环境的结果；这里引用的只是本机跑这个仓库测出来的（见 README「实测」一节和 `docs/bench/results.json`）。

## 一、项目概述

### 讲讲这个项目（1 分钟版）

**一句话答案**：一个用 TypeScript 写的货运事件处理服务：从 Kafka 消费承运商事件，校验、去重、按状态机更新追踪记录，下游失败有界重试，进不去的进死信队列，再用带 dry-run 和审计的流程安全重放。

**展开**（1 分钟口述版）：
承运商会推送揽收、到站、派送、签收这些事件。客户只关心一件事：包裹现在在哪。
难点不在「读消息写数据库」，而在消息会重复、会乱序、会是坏的，下游会挂，进程会被杀。
我按 shipmentId 做 key 保证分区内有序；zod 校验，坏消息带原因进 DLQ；event id 去重表和状态更新放在同一个事务里，先写库再提交 offset；状态机按承运商的 seq 判新旧，旧事件只进历史不回退状态。
下游 ETA 服务失败时指数退避、最多四次，超了进 DLQ。DLQ 可以 dry-run 再重放，重放的事件走正常消费路径，所以照样去重。
每个保护都有一个开关，关掉以后对应的测试必须变红，这是我证明测试有效的方式。

**追问**：你最得意的是哪一点？
**答**：「能失败的对照」。三个开关（`no_dedupe`、`no_transition_guard`、`commit_before_write`）各关掉一个保护，`scripts/failing-controls.sh` 跑出来关了必红、开了必绿，输出贴在 README 里。

### 一条事件从进来到客户看到，经过哪几步？

**一句话答案**：消费 → schema 校验 → 去重检查 → 状态机预判 → 调下游拿 ETA → 一个事务里占位、加锁重判、写状态和历史 → 提交 offset → 客户 API 读到。

**展开**：
入口在 [handle.ts](../src/processor/handle.ts#L150-L179 "anchor:write-record") 和 [consumer.ts](../src/processor/consumer.ts#L73-L79 "anchor:commit-order")。
去重先做一次只读检查，重复的直接跳过，连下游都不调。
状态机先在锁外预判，只有 apply 才调下游；进事务后在 `SELECT … FOR UPDATE` 下重判，以那次为准。
`/learn` 页面每一步都是轮询处理器真实记录的步骤点亮的。

**追问**：为什么状态机要判两次？
**答**：锁外那次只用来决定要不要花一次下游调用；真正的判定必须在行锁里做，因为调下游的几百毫秒里状态可能被别的路径（比如重放）改了。

### 这个项目里你没做什么？

**一句话答案**：没有多实例部署、没有熔断、没有 schema registry、没有真的承运商和 ETA 服务。

**展开**：
都在 README 的「What this is not」里。处理器是单进程，靠 Kafka consumer group 理论上能横向扩，但我只测了单实例。
下游完全挂掉时每条消息都要等满重试预算，生产上应该加熔断。
ETA 服务是假的，只用来注入 503、超时、慢响应。

## 二、设计取舍

### 为什么选 kafkajs，不选 @confluentinc/kafka-javascript？

**一句话答案**：纯 JS、不需要编译 librdkafka，本地和 CI 都是一条 `npm ci`；手动提交 offset 的 API 也够清楚。

**展开**：
代价是 kafkajs 2.2.4 是 2023 年的版本，已经不怎么维护了。
在 Node 24 上它会打印一条 `TimeoutNegativeWarning`，无害，但说明它和新 Node 的配合没人管了。
生产上我会换 Confluent 的客户端：性能更好、有人维护，API 也兼容 kafkajs 风格。

**追问**：换的话要改哪些地方？
**答**：只有 `src/kafka/client.ts` 和 `src/processor/consumer.ts` 碰客户端；处理逻辑 `handleMessage` 只认一个 `RawMessage`，不依赖 kafkajs。

### 去重为什么放 Postgres，不放 Redis？

**一句话答案**：去重记录和状态更新必须一起成功一起失败，只有放进同一个数据库事务才做得到。

**展开**：
放 Redis 的话，「记下 id」和「改状态」是两个系统的两次写，中间崩了就不一致。
代码在 [dedupe-claim](../src/processor/handle.ts#L62-L70 "anchor:dedupe-claim")：`INSERT … ON CONFLICT DO NOTHING RETURNING`，占不到就回滚。
主键冲突同时解决了并发重复：重平衡时两个消费者拿到同一条，只有一个能占到。

**追问**：`processed_events` 会无限涨吧？
**答**：会。生产上按时间分区、保留期比 Kafka 的保留期长一些就够了，超过 Kafka 保留期的消息不可能再被投递。这个仓库没做清理。

### 为什么按 seq 判新旧，不按事件时间？

**一句话答案**：事件时间来自不同设备的时钟，会漂、会撞；seq 是承运商给每个运单单调递增的编号，最可靠。

**展开**：
规则在 [state-decide](../src/domain/stateMachine.ts#L32-L51 "anchor:state-decide")：`seq <= last_seq` 就是 stale，只进历史。
`seq` 更大但动作不合法（签收后又扫描），是 reject，进 DLQ。
允许往前跳（承运商会漏扫），也允许从中间开始（`CREATED` 迟到）。

**追问**：如果承运商根本不给 seq 呢？
**答**：退一步用 `occurredAt` 加一个确定性的平局规则（比如 event id 字典序），并且接受偶尔判错；同时把这种运单打上标记，客户页面显示「可能有延迟数据」。

### 重试为什么在分区里原地做，不用重试 topic？

**一句话答案**：原地重试保证同一个运单的事件不会被越过；代价是下游挂掉时分区会慢，我用重试上限把这个代价封顶。

**展开**：
默认四次尝试、退避 200/400/800 毫秒、每次 800 毫秒超时，一条消息最坏占住分区约 4.6 秒（[retry-loop](../src/processor/retry.ts#L53-L81 "anchor:retry-loop")）。
重试 topic 不堵分区，但后续事件会先于重试中的事件被处理。我的状态机不会因此改错状态，但会让 ETA 之类的细节更复杂。
观测测试真实地展示了这个代价：后面的事件主要在排队（见 [q03](problems.md)）。

**追问**：下游挂一个小时会怎样？
**答**：每条都要等满预算才进 DLQ，lag 会持续上涨。所以生产上要加熔断：连续失败超过阈值就直接短路进 DLQ（或暂停消费），恢复后用重放补回来。

### 为什么重放要发回主 topic，而不是直接改数据库？

**一句话答案**：直接改库等于写第二套业务逻辑；发回 topic 就自动复用去重、状态机和下游调用。

**展开**：
重放只发原始字节和原始 key，加 `x-replay-id` header（[replay-execute](../src/replay/replay.ts#L126-L172 "anchor:replay-execute")）。
重放两次第二次会被去重；比当前状态旧的会变 stale；下游又挂了会重新进 DLQ。
测试 `replay.test.ts`「replaying the same events twice」证明了这一点。

**追问**：dry-run 为什么是强制的？
**答**：执行时重新算计划，哈希和 dry-run 记下的不一致就拒绝（`plan_changed`）。这样人看过的和真正执行的是同一份东西。

### 为什么用 zod，不用 JSON Schema（ajv）？

**一句话答案**：zod 的 schema 就是 TypeScript 类型，校验后的对象直接是强类型，少一层 `as`。

**展开**：
跨语言团队共享 schema 时，JSON Schema 或 Avro 加 schema registry 更合适。这里只有一个 TS 服务，zod 更省事。
版本号支持 v1 和 v2，解码成同一个内部形状（[schema-decode](../src/events/schema.ts#L28-L58 "anchor:schema-decode")）。

**追问**：要加 v3 怎么办？
**答**：加一个 zod schema 和一个映射到内部形状的分支，`SUPPORTED_VERSIONS` 加上 3；旧版本照常解。生产者先发 v3 之前，消费者必须先部署。

### 为什么查询 API 和处理器分成两个进程？

**一句话答案**：读和写的扩展方式、故障方式都不一样；处理器卡住不应该让客户查不到。

**展开**：
API 只读 Postgres（[api-shipment](../src/api/server.ts#L9-L14 "anchor:api-shipment")），不碰 Kafka。
处理器在重试下游的时候，API 照样能返回上一个状态。

**追问**：读多了怎么办？
**答**：先加只读副本；追踪记录是一个运单一行，也很适合放缓存，以 `version` 做失效。

## 三、正确性与故障

### 同一条消息投递两次会怎样？

**一句话答案**：第二次在只读检查时就被跳过；万一并发到达，事务里的主键冲突也只让一个成功。

**展开**：
测试 `duplicates.test.ts`：同一条发三次，历史一行，`version = 3`。
关掉去重（`no_dedupe`）这个测试报 `expected 3 to be 1`。

**追问**：重复的消息为什么不写历史？
**答**：历史是「我们看过的不同事件」。重复投递是传输层的事，记在日志和 `shipevents_events_total{outcome="duplicate"}` 里就够了。

### 进程在写完库、还没提交 offset 时被杀，会怎样？

**一句话答案**：重启后这条消息再来一次，被去重挡掉，结果不丢不重。

**展开**：
这条缝我用确定性崩溃点测（[crash-point](../src/lab/sabotage.ts#L15-L21 "anchor:crash-point")）：`LAB_CRASH_ON_EVENT=<eventId>`，正好死在两步之间。
测试断言六条事件在历史里各一次：`[1,1,1,1,1,1]`。
反过来先提交再写库，同一个测试是 `[1,1,1,0,1,1]`。

**追问**：随机 `kill -9` 的测试不够吗？
**答**：不够，我真的踩过：关掉去重，随机杀的那个用例还是绿的，因为它几乎打不中那几毫秒（[q01](problems.md)）。

### 来了一条坏消息会怎样？

**一句话答案**：解码函数不抛异常，返回原因；消息带原因、原始字节、原始位置进 DLQ，分区继续往前。

**展开**：
四种原因：`invalid_json`、`unsupported_version`、`schema_invalid`、`key_mismatch`。
测试 `malformed.test.ts`：同一个分区「好、坏、非 JSON、好」，最后那条照样被应用。
DLQ 索引按 `(topic, partition, offset)` 唯一，重投也只有一行。

**追问**：为什么 key 不一致也算坏消息？
**答**：key 决定分区，分区决定顺序。key 错了，这个运单的事件会散到别的分区，顺序悄悄就没了。

### 下游一直返回 503 会怎样？

**一句话答案**：四次尝试后进 DLQ，原因 `downstream_exhausted`，带每次尝试的错误；不写 `processed_events`，留给重放。

**展开**：
400 这类永久错误只试一次，原因 `downstream_rejected`。挂住不回的由每次尝试的超时切断。
测试 `downstream.test.ts` 四个场景都有。
一个真实的副作用：下游挂的时候，`DELIVERED` 不需要 ETA，会被直接应用；之前的事件重放回来全是 stale。结果是对的（[tutorial 第五章](tutorial/05-bounded-retries.md)）。

**追问**：重试会不会把刚恢复的下游打挂？
**答**：退避加了抖动（一半固定一半随机），实测两次退避是 104 和 246 毫秒，落在 [100,200] 和 [200,400] 里。多实例时还要加熔断和限流。

### 两个运维同时点重放会怎样？

**一句话答案**：Postgres advisory lock，同一时间只有一个重放；另一个拿到 `replay_in_progress`。就算顺序执行两次，第二次也会因为计划变了被拒（`plan_changed`）。

**展开**：
真跑过：同一个 dry-run 执行第二次，输出 `refused (plan_changed): plan changed since dry-90d571aa (9cd963e46529e4dd -> 4f53cda18c2baa0c)`。
即使有人绕过流程手工重发，去重也会挡住。

**追问**：重放的审计记了什么？
**答**：`replay_audit` 记谁、什么过滤条件、完整计划、计划哈希、基于哪个 dry-run、发了几条跳过几条；历史表里每条重放带来的行都有 `detail.replayId`。

### 你遇到过最难的问题是什么？

**一句话答案**：测试是绿的，但它没有测到它声称测到的东西。

**展开**：
关掉去重，随机杀进程的重启测试还是绿的。我没有相信这个绿，而是去证明测试能红，结果它红不了。
最后加了确定性崩溃点，现在三种破坏都能被抓到。详细过程见 [problems.md q01](problems.md)。
另一个是延迟拆解的测试：我以为最后一条慢在下游，其实慢在排队（[q03](problems.md)）。

**追问**：你怎么知道该怀疑一个绿的测试？
**答**：我的习惯是每个保护都配一个开关，关掉以后测试必须红。没有这一步，绿灯只说明「没报错」，不说明「测到了」。

## 四、扩展

### 流量放大 10 倍，瓶颈在哪？

**一句话答案**：先是单进程的处理速度，其次是 Postgres 的每事件多次往返。

**展开**：
实测（本机共享 colima，3 次取中位数）：单进程、三个分区并行时积压排空约 261 条/秒；分区串行时约 89 条/秒。
每条事件要做去重查询、状态查询、一次 HTTP、一个五六条语句的事务。
10 倍：加分区数、起多个处理器实例（consumer group 自动分配分区），Postgres 用连接池并调大。

**追问**：分区并行为什么不会破坏顺序？
**答**：顺序只在分区内需要。kafkajs 的 `partitionsConsumedConcurrently` 让不同分区并行，同一分区仍然串行；同一个运单永远在同一个分区。

### 流量放大 100 倍呢？

**一句话答案**：要把每事件的同步往返变成批处理，并把下游调用从关键路径上拿掉。

**展开**：
按分区批量拉取，批内按运单分组，一个事务里处理一批；去重用批量 `INSERT … ON CONFLICT … RETURNING`。
ETA 改成异步补充：先更新状态，ETA 由另一个消费者算好再回写。
追踪记录读多写少，查询侧加缓存。分区数要提前规划，因为加分区会改变 key 到分区的映射。

**追问**：加分区会出什么问题？
**答**：同一个运单的新旧事件可能落到不同分区，短时间内乱序。我们的状态机按 seq 判，不会改错状态，但要接受短暂的 stale 增多。

### 某个大客户的运单特别多，会热点吗？

**一句话答案**：key 是 shipmentId，不是客户 id，所以一个客户的运单会散在所有分区上，不会热点。

**展开**：
真正的热点是单个运单事件特别多，那在物流里不现实（一个包裹几十个事件）。
如果按客户做 key，就会热点，所以不这么做。

**追问**：如果业务要求「同一个客户的事件有序」呢？
**答**：先问清楚为什么。通常是对账类需求，可以在下游按客户聚合时用事件时间排序，而不是在 Kafka 层面强制。

## 五、测试

### 你怎么证明它是对的？

**一句话答案**：单元测试测纯逻辑，集成测试起真 Kafka 和真 Postgres，覆盖简历上那五类场景，再加三个能失败的对照。

**展开**：
重复投递（`duplicates.test.ts`）、畸形消息（`malformed.test.ts`）、下游不可用（`downstream.test.ts`）、非法转移和乱序（`ordering.test.ts`）、进程重启（`restart.test.ts`，真的起子进程、真的 SIGKILL）。
还有重放（`replay.test.ts`）和观测（`observability.test.ts`）。每个测试文件用自己的 topic 和 consumer group，可以并行。
测试数量和结果见 README。

**追问**：集成测试慢吗？
**答**：全套四十多个用例，本机三个 worker 并行三十秒左右；最慢的是重启测试，因为要等 consumer group 的 session 超时（测试里设成六秒）。

### 「能失败的对照」是怎么设计的？

**一句话答案**：每个关键保护有一个 `LAB_SABOTAGE` 开关，关掉以后保护它的测试必须变红，打开必须变绿。

**展开**：
三个开关：`no_dedupe` 跳过占位，`no_transition_guard` 换成「最后到的说了算」（[state-legacy](../src/domain/stateMachine.ts#L55-L58 "anchor:state-legacy")），`commit_before_write` 换成先提交后写库（[commit-legacy](../src/processor/consumer.ts#L83-L88 "anchor:commit-legacy")）。
反面代码留在源码里，只有开关打开才走，这样对照永远可以重跑。
`scripts/failing-controls.sh` 依次跑，输出贴在 README。

**追问**：这样不会把危险代码带进生产吗？
**答**：生产不会设 `LAB_SABOTAGE`；更稳妥的做法是 `NODE_ENV=production` 时拒绝启动带这个变量的进程，这个仓库只对 `ADMIN_TOKEN` 做了这种检查，`LAB_SABOTAGE` 没做，是个已知的缺口。

### 端到端测试测了什么？

**一句话答案**：Playwright 无头浏览器从界面点：教学页每个阶段按顺序亮起、显示真实数据、源码链接格式正确且指向的行真的存在；支线和面试题、日常问题标签页也点一遍。

**展开**：
阶段顺序是用页面里的 MutationObserver 记录每个阶段第一次变成「完成」的时间戳，再断言单调递增。
源码链接用 `git show <sha>:<path>` 取那个提交的文件，检查锚点行。

**追问**：怎么保证页面不是在播假动画？
**答**：页面只轮询 `/learn/trace`，后端返回的是处理器真实记录的步骤和数据库里那几行；教学样例的慢放是处理器真的在每步之间停七百毫秒（`x-learn-pace-ms`），而且只对带这个 header 的消息生效。

## 六、系统设计延伸

### 上生产还缺什么？

**一句话答案**：多实例和优雅重平衡、熔断、`processed_events` 清理、schema registry、真正的鉴权和告警。

**展开**：
运维接口现在是一个共享的 bearer token，生产要换成 SSO 加按人审计（审计表里已经有 actor 字段）。
告警：lag 持续上涨、DLQ 新增速率、下游错误率，这三条最先加。
部署：处理器无状态，起多个实例即可；要处理重平衡时正在重试的消息（kafkajs 会等 `eachMessage` 结束）。

**追问**：重平衡时正在重试的那条消息怎么办？
**答**：最坏 4.6 秒，比 session 超时短，所以 kafkajs 会等它处理完再交出分区；如果超了，新的消费者会重新处理它，去重兜底。

### 换成 AWS 怎么改？

**一句话答案**：Kafka 换 MSK（代码基本不动）或者 SQS FIFO（按 MessageGroupId=shipmentId 保序），Postgres 换 RDS/Aurora，DLQ 用原生的 DLQ。

**展开**：
用 SQS FIFO 的话，「先写库后删除消息」对应「先写库后提交 offset」；可见性超时要大于重试预算。
SQS 自带去重窗口只有五分钟，所以我们自己的 event id 去重表仍然要留。
指标换 CloudWatch 或者继续用 Prometheus（AMP）。

**追问**：用 DynamoDB 存追踪记录可以吗？
**答**：可以，用条件写（`seq` 比现有的大才写）实现状态机的 stale 判断，去重用另一张表加事务写（TransactWriteItems）。代价是历史查询不如 SQL 方便。

### ETA 换成真实的模型服务，要注意什么？

**一句话答案**：超时和重试预算要按它的真实延迟重新定，并且要能降级：拿不到 ETA 也应该先更新状态。

**展开**：
现在 ETA 失败会让整条事件进 DLQ，这对「状态」来说太严格了。
生产上应该把状态更新和 ETA 解耦：状态先写，ETA 失败只记一个标记，由补偿任务重算。

**追问**：那为什么这个仓库不这么做？
**答**：为了让「下游失败 → 有界重试 → DLQ → 重放」这条链路完整可演示。README 局限里写了。

## 七、简历对应

### 简历上写「容忍重复和乱序」，会被怎么追问？

**一句话答案**：会问「靠什么」和「怎么证明」。答：event id 去重表加业务序号状态机；证明是两个能失败的对照。

**展开**：
仓库里能直接演示：`node src/sim/cli.ts duplicate` 和 `node src/sim/cli.ts out-of-order`，再打开 `/learn` 点「发重复事件」「发迟到旧事件」。
要分开讲：简历上的规模和效果是公司生产环境的结果；这个仓库证明的是机制，实测数字只是本机的（README「实测」一节）。

**追问**：生产上重复率大概多少？
**答**：如实说我记得的公司数据，并强调那不是这个仓库测的。这个仓库只能说「重复投递时历史只有一行」，不能说重复率。

### 简历上写「有界重试、DLQ、安全重放流程」，怎么用仓库回答？

**一句话答案**：打开 `docs/replay.md` 讲五步：看清楚、强制 dry-run、执行（哈希校验、锁、按 seq、走去重）、核对、关掉修不了的。

**展开**：
重试参数和最坏占用时间可以当场算：`4 × 800 + 200 + 400 + 800 = 4600` 毫秒。
`/ops` 页面可以现场注入 503、看 DLQ 涨、dry-run、重放、看审计。

**追问**：「保持顺序」具体指什么？
**答**：重放计划按运单分组、组内按 seq 排，一条确认了再发下一条，同一个 key 进同一个分区。实测里三条乱序进 DLQ（dlqId 8、9、10 对应 seq 1、3、2），重放顺序是 seq 1、2、3。

### 简历上写「指标：lag、队列深度、吞吐、失败率」，怎么讲？

**一句话答案**：每一类对应一个 Prometheus 指标和一张 Grafana 图，再加一个按事件的延迟拆解接口，用来把「客户说晚了」查到具体分区和下游。

**展开**：
`shipevents_processing_lag_seconds`、`shipevents_consumer_lag_messages`、`shipevents_events_total`、`shipevents_failures_total`，dashboard 在 `infra/grafana/dashboards/shipevents.json`。
本机实测稳态（每秒 50 条）处理延迟中位数 50 毫秒、p99 96 毫秒（三个分区并行，3 次取中位数）。这是本机数字，不是简历数字。

**追问**：「复发的事故变成回归测试和 runbook」体现在哪？
**答**：`docs/runbook.md` 每一条都列了它的回归测试，比如「客户看到状态倒退」对应 `ordering.test.ts`。

### 简历上写「PR 流程里的测试」，仓库里有吗？

**一句话答案**：有 `.github/workflows/ci.yml`：单元测试、锚点校验、类型检查和构建，以及用 service 容器起 Kafka 和 Postgres 跑集成测试。

**展开**：
CI 文件写了，但这个仓库还没推到 GitHub，所以 CI 没有真的跑过，这一点要如实说。
本地全套测试和 e2e 都跑过，结果在 README。

**追问**：e2e 在 CI 里跑吗？
**答**：没放进 CI。e2e 需要整套服务加浏览器，目前只在本地跑。
