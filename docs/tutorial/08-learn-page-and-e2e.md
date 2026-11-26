# 第八章：教学页、源码锚点和端到端测试

> 讲解视频：[p8-learn](../videos/p8-learn.mp4)

**这一章解决什么问题？** 前七章的东西都在日志、数据库和测试里。要给别人讲清楚，最好能「点一下，看一条事件一步步走完」。
难点是：页面上亮起来的每一步，必须是处理器**真的**做了的那一步，而不是前端定时器播的动画。源码链接也不能写死行号，代码一改就错。
这一章做三件事：`/learn` 页面读后端真实状态；`@learn` 锚点自动生成源码链接；Playwright 从界面点一遍。

## 页面怎么知道处理器走到哪一步了？

处理器在每一步调用 `recordStep(key, stage, status, summary, data)`（`src/processor/steps.ts`），key 是消息在 Kafka 里的位置 `p{分区}-o{offset}`。
用位置而不用 event id，是因为畸形消息可能根本解不出 event id。
页面提交样例后拿到分区和 offset，每 250 毫秒轮询 `GET /learn/trace`，后端返回步骤列表加上数据库里的几行：`processed_events`、`shipment_history`、`dlq_entries`、`shipments`。

## 太快了看不清怎么办？

真实处理一条事件只要几十毫秒，所有步骤几乎同时亮。
教学样例的消息带一个 header：`x-learn-pace-ms: 700`。处理器看到它，就在每一步之间**真的**停七百毫秒（上限 1500）。没有这个 header 的消息完全不受影响。
所以慢放的是后端，不是前端动画。

真跑一次 `POST /learn/sample` 再轮询的结果（时间戳是处理器记录的）：

```text
02:01:37.344 consume ok partition 0 offset 50
02:01:38.044 schema ok v1 PICKED_UP seq 2
02:01:38.746 dedupe ok new event id
02:01:39.447 state ok CREATED -> PICKED_UP
02:01:40.148 downstream retry attempt 1 failed: eta 503: {"error":"injected failure"}, retry in 146ms
02:01:40.296 downstream ok attempt 2 ok in 1ms
02:01:41.003 write ok tracking record + history committed
```

样例在发这条事件之前，给假下游下了一个「这个运单下一次调用失败」的指令，所以能看到一次真实的重试。
支线也一样真实：重复事件停在 dedupe（`skip`）；迟到事件在 state 判成 stale；畸形事件在 schema 失败后进 DLQ；「下游宕机」让这个运单接下来四次调用都失败，然后可以在页面上 dry-run 和重放。

## 源码链接的行号从哪来？

在源码里放成对的注释，标记在范围外面：

```ts
// @learn:state-decide
export function decide(…) { … }
// @learn:end state-decide
```

`scripts/learn_anchors.mjs` 扫 `src/`、`frontend/src/`、`scripts/`、`docs/problems/snippets/`，生成 `frontend/src/generated/anchors.json`：`{id: {path, start, end}}`。
同一个脚本还会改写文档里写成 `[文字](任意 "anchor:<id>")` 的链接，把相对路径和 `#Lx-Ly` 填对。
页面上的链接是 `https://github.com/<账号>/shipevents/blob/<sha>/<path>#L<start>-L<end>`。账号只在 `repo.config.json` 一处定义，sha 在构建时取 `git rev-parse HEAD`。不写死 `main`，因为 main 会变，行号就对不上了。

`scripts/check_anchors.mjs` 检查三件事：前端和文档引用的每个 id 都存在；标记行确实在 anchors.json 说的位置；生成文件和文档链接没过期。

## 端到端测试测什么？

`e2e/learn.spec.ts` 用 Playwright 无头 Chromium：

1. 点「提交样例」前，往页面里装一个 MutationObserver，记下每个阶段每次 `data-status` 变化的时间戳。等最后一步完成后，断言七个阶段第一次变成「完成」的时间严格递增，而且下游阶段中间出现过「进行中」（重试）。
2. 断言真实数据：分区和 offset、`CREATED -> PICKED_UP`、`attempt 1 failed: eta 503`、写库那一行 JSON 里的 `"status_to": "PICKED_UP"`。
3. 每个「看源码」链接：格式对、sha 是四十位、用 `git show <sha>:<path>` 取出那个提交的文件，检查锚点行就在那里。
4. 四条支线，包括 dry-run 计划表里一条 `apply`、一条 `skip_invalid`，执行后显示「发回 1 条，跳过 1 条」。
5. 面试题和日常问题标签页：点开一题能看到答案。

`e2e/app.spec.ts` 测追踪页（乱序运单显示两条 stale）和运维页（DLQ 列表、详情、dry-run、错误 token 显示 401）。

## 怎么 checkout 到这一步跑起来

```bash
git checkout step-08
./scripts/demo.sh                       # 起全部：infra + 三个服务 + 前端 5190
open http://127.0.0.1:5190/learn
npm test                                # 后端单元 + 集成
npm run e2e                             # 需要 demo.sh 在跑
node scripts/check_anchors.mjs
./scripts/stop.sh
```
