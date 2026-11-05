# frozen_string_literal: true

# This integration test verifies that `Rdkafka::Config.partition_count_cache_ttl_ms=` changes how
# long `partition_key` produces rely on a cached partition count, for the whole process and
# without dropping counts that are already cached.
#
# We cache the count of a 1-partition topic under the default (30 s) TTL and add partitions. The
# cached count stays at 1. After lowering the TTL to 1 s, the next lookup refreshes it to the new
# count. Statistics are not enabled, so only the TTL can trigger that refresh.
#
# Requires a running Kafka broker at localhost:9092.
#
# Exit codes:
# - 0: the cached count was kept and refreshed according to the new TTL
# - 1: the count was not cached under the default TTL, or not refreshed after lowering it

require "rdkafka"
require "securerandom"

$stdout.sync = true

TOPIC = "partitions-count-cache-ttl-#{SecureRandom.hex(6)}"
TTL_MS = 1_000

Rdkafka::Producer.partition_count_cache = Rdkafka::Producer::PartitionCountCache.new

config = Rdkafka::Config.new("bootstrap.servers": "localhost:9092")

admin = config.admin
admin.create_topic(TOPIC, 1, 1).wait

producer = config.producer
initial = producer.partition_count(TOPIC)

if initial != 1
  warn "FAIL: expected 1 partition when warming the cache, got #{initial}"
  exit(1)
end

admin.create_partitions(TOPIC, 3).wait
admin.close

sleep((TTL_MS / 1000.0) + 0.5)

cached = producer.partition_count(TOPIC)

if cached != 1
  warn "FAIL: expected the default TTL to keep the cached count of 1, got #{cached}"
  exit(1)
end

Rdkafka::Config.partition_count_cache_ttl_ms = TTL_MS

refreshed = producer.partition_count(TOPIC)
producer.close

if refreshed != 3
  warn "FAIL: expected 3 partitions after lowering the TTL to #{TTL_MS} ms, got #{refreshed}"
  exit(1)
end

puts "PASS: the cached count was kept and refreshed once the lowered TTL passed"
exit(0)
