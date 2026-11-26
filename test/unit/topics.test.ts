import { describe, expect, it } from "vitest";
import type kafkajs from "kafkajs";
import { ensureTopics } from "../../src/kafka/client.ts";

// A broker that has created the topic but does not serve its partitions yet,
// the state the first GitHub CI run caught (docs/problems.md q09). kafkajs's
// createTopics({ waitForLeaders: true }) asks for metadata in that window and
// throws "This server does not host this topic-partition" instead of waiting,
// so the waiting has to be ours.
// @learn:q09-topics-test
function laggingBroker(notReadyPolls: number) {
  let polls = 0;
  const calls: string[] = [];
  const admin = {
    connect: async () => {},
    disconnect: async () => {},
    listTopics: async () => [] as string[],
    createTopics: async (opts: { waitForLeaders?: boolean }) => {
      calls.push(`create waitForLeaders=${opts.waitForLeaders}`);
      if (opts.waitForLeaders) throw new Error("This server does not host this topic-partition");
      return true;
    },
    fetchTopicMetadata: async ({ topics }: { topics: string[] }) => {
      polls += 1;
      const leader = polls > notReadyPolls ? 0 : -1;
      return { topics: topics.map((name) => ({ name, partitions: [0, 1, 2].map((partitionId) => ({ partitionId, leader })) })) };
    },
  };
  return { kafka: { admin: () => admin } as unknown as kafkajs.Kafka, calls, polls: () => polls };
}

describe("ensureTopics on a broker that lags behind its own createTopics", () => {
  it("waits for every partition to have a leader instead of failing", async () => {
    const b = laggingBroker(3);
    await expect(ensureTopics(b.kafka, ["shipments.q09"], 3)).resolves.toBeUndefined();
    expect(b.calls).toEqual(["create waitForLeaders=false"]);
    expect(b.polls()).toBe(4);
  });
});
// @learn:end q09-topics-test
