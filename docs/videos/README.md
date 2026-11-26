# 讲解视频

每个教学阶段一段短片，`/learn` 页面里每个阶段旁边的「看讲解视频」播放的就是这里的文件。清单见 [`video/parts.json`](../../video/parts.json)：

| 文件 | 内容 | 对应教程 |
|---|---|---|
| [`p1-intake.mp4`](p1-intake.mp4) | 事件怎么进来：按运单做 key | — |
| [`p2-schema.mp4`](p2-schema.mp4) | 先验货：schema 校验 | — |
| [`p3-dedupe.mp4`](p3-dedupe.mp4) | 去重和先写库后提交 | — |
| [`p4-state.mp4`](p4-state.mp4) | 状态机：旧事件不回退 | — |
| [`p5-retry.mp4`](p5-retry.mp4) | 下游失败：有界重试 | — |
| [`p6-replay.mp4`](p6-replay.mp4) | 死信和安全重放 | — |
| [`p7-observe.mp4`](p7-observe.mp4) | 客户看到什么，晚在哪里 | — |
| [`p8-learn.mp4`](p8-learn.mp4) | 教学页、面试题和日常问题 | — |

画面由 `video/record.mjs` 在真实页面上录制（会话里的数据都是这次真实请求产生的），红框标出每一步该看的地方；旁白来自 `video/narration.json`。旁白是本地 TTS 合成的（CosyVoice 2 自带的示例音色，不调任何云端 API），成片用本地 whisper 逐段回验过念的是稿子上的话。

## 日常问题（`problems/`）

每个问题一段代码讲解：先看修复前的写法和现象，再看修复后的写法，讲到哪几行就用红框框出哪几行。内容来自 [`video/problems.walk.json`](../../video/problems.walk.json)（由 `scripts/build_problem_videos.mjs` 按锚点从源码生成），问题的来龙去脉见 [`docs/problems.md`](../problems.md)。

- [`q01-random-kill-misses.mp4`](problems/q01-random-kill-misses.mp4) — 随机杀进程的测试，抓不到丢和重
- [`q02-topic-not-ready.mp4`](problems/q02-topic-not-ready.mp4) — 刚建好的主题，第一条消息就发不出去
- [`q03-queue-not-downstream.mp4`](problems/q03-queue-not-downstream.mp4) — 我以为慢在下游，其实是在排队
- [`q04-demo-hangs.mp4`](problems/q04-demo-hangs.mp4) — 启动脚本跑完了，终端却一直不返回
- [`q05-status-goes-back.mp4`](problems/q05-status-goes-back.mp4) — 已签收的包裹，又变回运输中
- [`q06-lost-on-restart.mp4`](problems/q06-lost-on-restart.mp4) — 重启以后，少了一条事件
- [`q07-duplicate-history.mp4`](problems/q07-duplicate-history.mp4) — 同一条事件，历史里出现三次
- [`q08-poison-message.mp4`](problems/q08-poison-message.mp4) — 一条坏消息，卡住了整个分区
