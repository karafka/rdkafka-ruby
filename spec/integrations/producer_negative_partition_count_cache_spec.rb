# frozen_string_literal: true

# This integration test verifies that `Producer#partition_count` caches a negative lookup (a topic
# that does not exist), so producing with a `partition_key` to a missing topic does not run a
# blocking metadata query on every single message.
#
# The `unknown_topic_or_part` rescue used to return `RD_KAFKA_PARTITION_UA` outside `cache.get`, so
# nothing was stored: every `partition_count` call for a missing topic performed a fresh,
# synchronous metadata RPC (despite the doc comment promising a negative cache). The miss is now
# resolved to `RD_KAFKA_PARTITION_UA` inside the cache block, so it is cached like any other value.
#
# We look up a missing topic once and assert the process-wide cache now holds the negative entry.
#
# Requires a running Kafka broker at localhost:9092.
#
# Exit codes:
# - 0: the negative result was cached (subsequent produces won't re-query)
# - 1: nothing was cached (a metadata RPC would run on every produce)

require "rdkafka"
require "securerandom"

$stdout.sync = true

MISSING_TOPIC = "negative-partition-count-probe-#{SecureRandom.hex(6)}"

# Start from a clean process-wide cache so the assertion is unambiguous.
Rdkafka::Producer.partitions_count_cache = Rdkafka::Producer::PartitionsCountCache.new

producer = Rdkafka::Config.new("bootstrap.servers": "localhost:9092").producer

count = producer.partition_count(MISSING_TOPIC)
cached = Rdkafka::Producer.partitions_count_cache.to_h[MISSING_TOPIC]

producer.close

# The pass/fail criterion is whether the lookup was cached. The exact value depends on broker
# behavior (a broker that raises unknown_topic_or_part yields RD_KAFKA_PARTITION_UA; one that
# auto-creates or returns empty metadata yields a real count) - either way it must be cached so a
# partition_key produce does not re-query on every message.
if cached.nil?
  warn "FAIL: the lookup for a missing topic was not cached - a metadata RPC would run on every produce"
  exit(1)
end

puts "PASS: missing topic lookup cached as #{cached[1]} (count returned: #{count}), no per-message metadata RPC"
exit(0)
