// The q02 fix as it stood until the first GitHub CI run (docs/problems.md q09):
// createTopics still waited for leaders itself, and that wait is what threw.
export {};
async function before(admin: any, missing: string[], numPartitions: number): Promise<void> {
  // @learn:q09-create-topics-before
    if (missing.length > 0) {
      await admin.createTopics({ waitForLeaders: true, topics: missing.map((topic) => ({ topic, numPartitions, replicationFactor: 1 })) });
      // createTopics can return before every partition has a leader that the broker will serve;
      // a produce in that window fails with "This server does not host this topic-partition".
      const deadline = Date.now() + 15_000;
      for (;;) {
        const md = await admin.fetchTopicMetadata({ topics: missing }).catch(() => undefined);
        const ready = md?.topics.every((t) => t.partitions.length === numPartitions && t.partitions.every((p) => p.leader >= 0));
        if (ready || Date.now() > deadline) break;
        await new Promise((r) => setTimeout(r, 100));
      }
    }
  // @learn:end q09-create-topics-before
}
