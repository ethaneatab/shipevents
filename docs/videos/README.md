# 讲解视频

由主会话按 `video/timeline.json` 的 part 切片、配音后放在这里（每段 10 MB 以内）。缺文件时页面显示「视频待生成」。

| 文件 | part 标题 | learn 阶段 | 章节 |
|---|---|---|---|
| `p1-intake.mp4` | 事件怎么进来：按运单做 key | produce | 01 |
| `p2-schema.mp4` | 先验货：schema 校验 | schema | 02 |
| `p3-dedupe.mp4` | 去重和先写库后提交 | dedupe, write | 03 |
| `p4-state.mp4` | 状态机：旧事件不回退 | state | 04 |
| `p5-retry.mp4` | 下游失败：有界重试 | downstream | 05 |
| `p6-replay.mp4` | 死信和安全重放 | dlq | 06 |
| `p7-observe.mp4` | 客户看到什么，晚在哪里 | api | 07 |
| `p8-learn.mp4` | 教学页、面试题和日常问题 | 全部 | 08 |
| `problems/<id>.mp4` | `video/problems.walk.json` 里的八个问题 | 日常问题标签页 | — |

录制：`./scripts/demo.sh`，停掉测试，然后 `node video/record.mjs`（读 `video/durations.json`，没有就每段 4 秒），产物 `video/out/learn.webm` + `video/timeline.json`；`node video/check_frames.mjs s05 s10 s23 s30` 抽帧看红框。
