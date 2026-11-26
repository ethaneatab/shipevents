// Before: trust createTopics({ waitForLeaders: true }) and produce right away.
// @learn:q02-create-topics-before
if (missing.length > 0) {
  await admin.createTopics({
    waitForLeaders: true,
    topics: missing.map((topic) => ({ topic, numPartitions, replicationFactor: 1 })),
  });
}
// the next producer.send() sometimes failed:
// KafkaJSProtocolError: This server does not host this topic-partition
// @learn:end q02-create-topics-before
