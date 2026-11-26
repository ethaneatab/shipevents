# shipevents

A TypeScript service that consumes shipment events from Kafka, validates them, tolerates duplicates and out-of-order delivery, applies a shipment state machine, retries a flaky downstream with a hard bound, parks what it cannot process in a DLQ, and replays it safely.

![learn page preview](docs/learn-preview.gif)

Walkthrough videos (Chinese, one per stage, narration synthesized locally with TTS): [docs/videos/](docs/videos/README.md) · Interview questions: [docs/interview.md](docs/interview.md) · Day-to-day problems: [docs/problems.md](docs/problems.md) · Tutorial: [docs/tutorial/](docs/tutorial/) · Runbook: [docs/runbook.md](docs/runbook.md) · Replay procedure: [docs/replay.md](docs/replay.md)

## Architecture

```mermaid
flowchart LR
  sim[simulator / carrier feed] -- "key = shipmentId" --> T[(Kafka<br/>shipments.events<br/>3 partitions)]
  T --> P[processor :8900<br/>decode → dedupe → state machine<br/>→ ETA call (bounded retry) → tx]
  P -- "same tx: processed_events + shipments + history,<br/>then commit offset" --> DB[(Postgres)]
  P -- "ETA" --> D[fake ETA service :8902<br/>fault injection]
  P -- "bad bytes / retries exhausted /<br/>illegal transition" --> DLQ[(Kafka shipments.dlq)]
  P -- index --> DB
  R[replay: dry-run → execute<br/>CLI or /ops] -- "original bytes, original key" --> T
  R -- audit --> DB
  API[customer API :8901] --> DB
  FE[React frontend :5190<br/>/track /ops /learn] --> API
  FE --> P
  P -- /metrics --> PR[Prometheus :9210] --> G[Grafana :3210]
```

One event, in order: decode with zod (never throws; failures go to the DLQ with a reason) → read-only dedupe check → state-machine preview → ETA call with bounded retries (only if the event will apply) → one transaction: claim the event id in `processed_events`, lock the shipment row, decide again, write the tracking record and history → commit the Kafka offset.

## Run it

```bash
./scripts/demo.sh        # infra in docker (compose project "shipevents") + 3 Node services + Vite; seeds demo shipments
open http://127.0.0.1:5190/learn
./scripts/stop.sh        # stops only the PIDs demo.sh wrote to .runtime/; --infra also stops the containers
```

Requires Node 24 and docker-compose. Ports: processor 8900, customer API 8901, fake ETA 8902, frontend 5190, Kafka 19210, Postgres 54310, Prometheus 9210, Grafana 3210.
The account placeholder for links lives in one place: `repo.config.json`.

Other entry points: `node src/sim/cli.ts <journey|duplicate|out-of-order|illegal|flaky|malformed>`, `node src/replay/cli.ts <list|plan|execute|discard>`.

## Technology choices

| Choice | Why | Cost |
|---|---|---|
| **kafkajs 2.2.4** over `@confluentinc/kafka-javascript` | pure JS, no librdkafka build, `npm ci` is enough locally and in CI; manual `commitOffsets` is explicit | unmaintained since 2023; prints a harmless `TimeoutNegativeWarning` on Node 24. Only `src/kafka/` and `src/processor/consumer.ts` touch it |
| **Postgres** for dedupe | the event-id claim and the state change commit in one transaction | `processed_events` grows; no retention job here |
| **zod** for schemas | schema = TypeScript type; v1 and v2 wire formats decode to one internal shape | not shareable across languages like Avro + registry |
| **seq** (carrier per-shipment counter) for ordering | device clocks drift; arrival order is what breaks | needs the carrier to provide it |
| **in-partition retries** | later events of a shipment never overtake the one being retried | a dead dependency slows the partition; capped at 4 attempts × 800 ms + 200/400/800 ms backoff ≈ 4.6 s per message (computed from the policy) |
| **replay through the main topic** | reuses dedupe, state machine and ETA logic; replaying twice is a no-op | needs the normal pipeline to be healthy |
| Node 24 native type stripping | services run as `node src/x.ts`, no build step at runtime | `erasableSyntaxOnly` (no enums/parameter properties) |

## Tests

```text
$ npx vitest run
 Test Files  12 passed (12)
      Tests  43 passed (43)
$ npm run e2e      # Playwright headless, against ./scripts/demo.sh
  7 passed
```

Integration tests run against real Kafka and Postgres (`shipevents_test` database). Each test file has its own topics and consumer group. The restart tests spawn the real processor as a child process and SIGKILL it.

| Resume claim | Test |
|---|---|
| duplicate delivery | `test/integration/duplicates.test.ts` |
| malformed payloads | `test/integration/malformed.test.ts`, `test/unit/schema.test.ts` |
| unavailable dependencies | `test/integration/downstream.test.ts`, `test/unit/retry.test.ts` |
| illegal state transitions / out of order | `test/integration/ordering.test.ts`, `test/unit/stateMachine.test.ts` |
| worker restarts | `test/integration/restart.test.ts` (deterministic crash point between DB commit and offset commit, plus a random SIGKILL burst) |
| safe replay | `test/integration/replay.test.ts` |
| delayed-update tracing | `test/integration/observability.test.ts` |

### Failing controls (proof the tests are alive)

Each protection has a switch (`LAB_SABOTAGE`). With the switch on, the test guarding it must fail; with it off, the same test must pass. `./scripts/failing-controls.sh`, measured output:

```text
$ ./scripts/failing-controls.sh
LAB_SABOTAGE           test                                       result   vitest summary
no_dedupe              test/integration/duplicates.test.ts        RED ok         Tests  1 failed (1)
     → expected 3 to be 1 // Object.is equality
(off)                  test/integration/duplicates.test.ts        GREEN ok       Tests  1 passed (1)
no_transition_guard    test/integration/ordering.test.ts          RED ok         Tests  4 failed | 1 passed (5)
     → expected { status: 'IN_TRANSIT', lastSeq: 4 } to deeply equal { status: 'OUT_FOR_DELIVERY', …(1) }
     → expected 'CREATED' to be 'PICKED_UP' // Object.is equality
(off)                  test/integration/ordering.test.ts          GREEN ok       Tests  5 passed (5)
commit_before_write    test/integration/restart.test.ts           RED ok         Tests  1 failed | 1 passed (2)
     → expected [ 1, 1, 1, +0, 1, 1 ] to deeply equal [ 1, 1, 1, 1, 1, 1 ]
(off)                  test/integration/restart.test.ts           GREEN ok       Tests  2 passed (2)
(exit 0)
```

Observed while building this: the *random* SIGKILL test stays green with `no_dedupe` on, because a random kill almost never lands in the few milliseconds between the DB commit and the offset commit. The deterministic crash point (`LAB_CRASH_ON_EVENT`) is what catches it. See [problems.md q01](docs/problems.md).

## Measured in this repo

Environment: Apple M5 Pro laptop, Node 24.15; Kafka and Postgres in a colima VM (4 vCPU, 6 GiB) **shared with other projects running at the same time**. Every number is the median of 3 runs (`node scripts/bench.ts`, raw runs in `docs/bench/results.json`). These are local numbers, not production numbers.

| Scenario | 1 partition at a time | 3 partitions in parallel |
|---|---|---|
| drain a 3,000-event backlog (events/s) | 89 (runs 90, 89, 87) | 261 (runs 261, 244, 288) |
| steady 50 events/s, processing lag p50 (Kafka append → DB commit) | 61 ms | 50 ms |
| steady 50 events/s, processing lag p99 | 101 ms (runs 390, 101, 92) | 96 ms |

Each event costs a dedupe read, a state read, one HTTP call to the fake ETA service and a 5–6 statement transaction, which is why a single process tops out in the low hundreds per second here.
Container memory with everything idle after a demo run: Kafka 409 MiB, Grafana 97 MiB, Postgres 27 MiB, Prometheus 19 MiB (limits add up to 1.16 GiB).

## Observability

`/metrics` on 8900: `shipevents_events_total{outcome}` (throughput), `shipevents_failures_total{reason}`, `shipevents_processing_lag_seconds`, `shipevents_consumer_lag_messages{partition}` (queue depth), `shipevents_downstream_*`, `shipevents_retries_total`, `shipevents_dlq_open_entries{reason}`. Dashboard JSON: `infra/grafana/dashboards/shipevents.json`. `GET /v1/shipments/:id/latency` splits each event's time into downstream vs queue+DB. Every runbook entry names its regression test ([docs/runbook.md](docs/runbook.md)).

## What this is not

- Not multi-instance tested. One processor process; consumer groups should scale it out, but that was not measured.
- No circuit breaker. With the ETA service fully down, every event waits the full retry budget before it is dead-lettered, and lag grows.
- ETA failure dead-letters the whole event. A production system would update the status first and backfill the ETA.
- No retention for `processed_events`, no schema registry, a single shared admin bearer token instead of real identity.
- `LAB_SABOTAGE` is not blocked in production mode (only `ADMIN_TOKEN` is checked).
- The ETA service and the carrier feed are simulators. CI is written (`.github/workflows/ci.yml`) but has not run on GitHub yet; e2e runs locally only.

## Teaching mode

`/learn` lights each stage from the processor's real step log (`GET /learn/trace`), shows the message, the `processed_events` row, the from→to decision, each ETA attempt and the history row, and links each stage to its source by commit SHA (`@learn` anchors, generated by `scripts/learn_anchors.mjs`, checked by `scripts/check_anchors.mjs`). Learn samples carry `x-learn-pace-ms: 700`, which makes the processor itself pause between steps; other messages are unaffected. Per-stage videos are served from `docs/videos/<part>.mp4` when present.

## License

MIT
