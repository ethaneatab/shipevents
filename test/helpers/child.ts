import { spawn, type ChildProcess } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { TEST_BROKERS, TEST_DB_URL } from "./env.ts";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");

export interface Child {
  proc: ChildProcess;
  output: () => string;
  exited: Promise<{ code: number | null; signal: NodeJS.Signals | null }>;
  kill(sig?: NodeJS.Signals): void;
}

/** Runs the real processor entrypoint as a separate OS process, pointed at the test topics and database. */
export function spawnProcessor(names: { topic: string; dlqTopic: string; group: string }, downstreamUrl: string, extraEnv: Record<string, string> = {}): Child {
  let buf = "";
  const proc = spawn(process.execPath, ["src/processor/main.ts"], {
    cwd: root,
    env: {
      ...process.env,
      DATABASE_URL: TEST_DB_URL,
      KAFKA_BROKERS: TEST_BROKERS.join(","),
      EVENTS_TOPIC: names.topic,
      DLQ_TOPIC: names.dlqTopic,
      CONSUMER_GROUP: names.group,
      PROCESSOR_PORT: "0",
      KAFKA_SESSION_TIMEOUT_MS: "6000",
      LOG_LEVEL: "info",
      DOWNSTREAM_URL: downstreamUrl,
      RETRY_MAX_ATTEMPTS: "4", RETRY_BASE_MS: "50", RETRY_MAX_MS: "400", DOWNSTREAM_TIMEOUT_MS: "300", RETRY_JITTER: "0",
      ...extraEnv,
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
  proc.stdout!.on("data", (d) => (buf += d));
  proc.stderr!.on("data", (d) => (buf += d));
  const exited = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve) => proc.on("exit", (code, signal) => resolve({ code, signal })));
  return { proc, output: () => buf, exited, kill: (sig = "SIGTERM") => void proc.kill(sig) };
}
