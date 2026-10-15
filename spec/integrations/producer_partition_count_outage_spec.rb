# frozen_string_literal: true

# This integration test verifies that `produce(partition_key:)` keeps working when the cached
# partition count of a topic has expired and the cluster cannot be reached.
#
# Refreshing an expired entry used to raise the metadata error (`transport` when no broker
# answers), so once the 30 second TTL passed during an outage every `partition_key` produce
# raised, even though the last known count was still valid. The cache now serves the last known
# count when the refresh fails on a transient error.
#
# The cache is process-wide, so a producer connected to the broker warms it and a second producer
# pointed at an unreachable address stands in for the outage. The broker never has to stop.
#
# Requires a running Kafka broker at localhost:9092.
#
# Exit codes:
# - 0: the produce was accepted and the last known count was served
# - 1: the produce raised, or the cache lost the last known count

require "rdkafka"
require "securerandom"

$stdout.sync = true

TOPIC = "partition-count-outage-#{SecureRandom.hex(6)}"
PARTITIONS = 3
TTL_MS = 1_000

Rdkafka::Producer.partitions_count_cache = Rdkafka::Producer::PartitionsCountCache.new(
  ttl_ms: TTL_MS
)

admin = Rdkafka::Config.new("bootstrap.servers": "localhost:9092").admin
admin.create_topic(TOPIC, PARTITIONS, 1).wait
admin.close

online = Rdkafka::Config.new("bootstrap.servers": "localhost:9092").producer
warm = online.partition_count(TOPIC)
online.close

if warm != PARTITIONS
  warn "FAIL: expected #{PARTITIONS} partitions when warming the cache, got #{warm}"
  exit(1)
end

offline = Rdkafka::Config.new(
  "bootstrap.servers": "127.0.0.1:9",
  "message.timeout.ms": 1_000
).producer

sleep((TTL_MS / 1000.0) + 0.5)

begin
  offline.produce(topic: TOPIC, payload: "payload", partition_key: "key")
rescue Rdkafka::RdkafkaError => e
  warn "FAIL: produce with partition_key raised #{e.code} after the cached count expired"
  exit(1)
ensure
  served = Rdkafka::Producer.partitions_count_cache.to_h[TOPIC]
end

if served.nil? || served[1] != PARTITIONS
  warn "FAIL: expected the cache to keep #{PARTITIONS} partitions, got #{served.inspect}"
  exit(1)
end

offline.close

puts "PASS: partition_key produce accepted during the outage with the last known count"
exit(0)
