# frozen_string_literal: true

# This integration test verifies statistics emission for KIP-932 share consumers:
# - the statistics callback fires even though a share consumer has no background polling
#   thread (statistics are serviced from within share poll)
# - the consumer group section (cgrp) is present, since share consumers reuse the consumer
#   group machinery
# - documents how the karafka-rdkafka statistics.unassigned.include filter interacts with
#   share consumers, whose partitions never enter the regular fetch states
#
# Requires a running Kafka broker with share groups enabled at 127.0.0.1:9092.
#
# Exit codes:
# - 0: Statistics behave as expected (test passes)
# - 1: An assertion failed

require "rdkafka"
require "securerandom"

$stdout.sync = true

BOOTSTRAP = "127.0.0.1:9092"
TOPIC = "share-stats-#{SecureRandom.hex(6)}"
PARTITIONS = 10
MESSAGES = 50

def assert(condition, message)
  return if condition

  puts "FAILED: #{message}"
  exit 1
end

# Consumes for a few seconds with the given extra config and returns the collected stats
def collect_share_stats(topic, extra_config)
  stats = []
  Rdkafka::Config.statistics_callback = ->(published) { stats << published }

  group_id = "share-stats-group-#{SecureRandom.hex(4)}"

  # share.auto.offset.reset is a broker-side group config defaulting to latest; set it to
  # earliest before the group first attaches so the pre-produced records are delivered
  admin = Rdkafka::Config.new("bootstrap.servers": BOOTSTRAP).admin
  admin.incremental_alter_configs(
    [
      {
        resource_type: Rdkafka::Bindings::RD_KAFKA_RESOURCE_GROUP,
        resource_name: group_id,
        configs: [{ name: "share.auto.offset.reset", value: "earliest", op_type: 0 }]
      }
    ]
  ).wait(max_wait_timeout_ms: 15_000)
  admin.close

  consumer = Rdkafka::Config.new(
    {
      "bootstrap.servers": BOOTSTRAP,
      "group.id": group_id,
      "statistics.interval.ms": 500
    }.merge(extra_config)
  ).share_consumer

  consumer.subscribe(topic)

  # Captured before the first poll on purpose: ShareConsumer#name must be available straight
  # away (it is used to correlate this consumer with the global statistics/error callbacks) and
  # not only after some callback has populated it.
  name_before_poll = consumer.name

  consumed = 0
  20.times do
    consumed += consumer.poll(500).size
    break if consumed >= MESSAGES && stats.size >= 3
  end

  consumer.close
  Rdkafka::Config.statistics_callback = nil

  [stats, consumed, name_before_poll]
end

# Returns the number of partitions visible in the topics section of a stats payload
def visible_partitions(stat, topic)
  partitions = stat.dig("topics", topic, "partitions") || {}
  # The UA (-1) partition is bookkeeping, not a real partition
  partitions.keys.count { |k| k != "-1" }
end

admin = Rdkafka::Config.new("bootstrap.servers": BOOTSTRAP).admin
admin.create_topic(TOPIC, PARTITIONS, 1).wait(max_wait_timeout_ms: 15_000)
admin.close

producer = Rdkafka::Config.new("bootstrap.servers": BOOTSTRAP).producer
handles = MESSAGES.times.map do |i|
  producer.produce(topic: TOPIC, payload: "payload-#{i}", partition: i % PARTITIONS)
end
handles.each { |handle| handle.wait(max_wait_timeout_ms: 15_000) }
producer.close

# Unfiltered: default statistics.unassigned.include (true)
unfiltered_stats, unfiltered_consumed, unfiltered_name = collect_share_stats(TOPIC, {})

assert(unfiltered_stats.size >= 2, "expected statistics callbacks for a share consumer, got #{unfiltered_stats.size}")
assert(unfiltered_consumed >= MESSAGES, "expected #{MESSAGES} records consumed, got #{unfiltered_consumed}")
assert(unfiltered_stats.all? { |s| s["type"] == "consumer" }, "expected consumer-type stats")

# ShareConsumer#name must be usable to correlate this consumer with the statistics it emits.
# It has to be a real, non-empty name captured before the first poll, and it has to equal the
# "name" field librdkafka puts in the statistics JSON - otherwise a downstream that filters the
# global statistics callback by consumer name (e.g. Karafka) drops every share-consumer stat.
assert(unfiltered_name.is_a?(String) && !unfiltered_name.empty?, "expected ShareConsumer#name to be a non-empty String right after creation, got #{unfiltered_name.inspect}")
stat_names = unfiltered_stats.map { |s| s["name"] }.compact.uniq
assert(stat_names.include?(unfiltered_name), "expected ShareConsumer#name (#{unfiltered_name.inspect}) to match the statistics name field #{stat_names.inspect}")

cgrp_stat = unfiltered_stats.reverse.find { |s| s["cgrp"] }
assert(!cgrp_stat.nil?, "expected the cgrp section in share consumer statistics")
assert(cgrp_stat["cgrp"].key?("state"), "expected cgrp state in share consumer statistics")

unfiltered_visible = unfiltered_stats.map { |s| visible_partitions(s, TOPIC) }.max

# Filtered: statistics.unassigned.include=false (karafka-rdkafka patch). Empirically the
# share fetch path bypasses the regular per-partition stats bookkeeping entirely, so the
# topics section carries no partition entries for share consumers in either mode and the
# filter patch needs no share-consumer special-casing (see the visibility summary printed
# below). Share-group instrumentation has to be built from cgrp, per-message delivery counts
# and the acknowledgement commit callback until librdkafka exposes share stats.
filtered_stats, filtered_consumed = collect_share_stats(TOPIC, { "statistics.unassigned.include": false })

assert(filtered_stats.size >= 2, "expected statistics callbacks with the filter enabled, got #{filtered_stats.size}")
assert(filtered_consumed >= MESSAGES, "expected #{MESSAGES} records consumed with filter, got #{filtered_consumed}")

filtered_cgrp = filtered_stats.reverse.find { |s| s["cgrp"] }
assert(!filtered_cgrp.nil?, "expected the cgrp section to survive the unassigned filter")

filtered_visible = filtered_stats.map { |s| visible_partitions(s, TOPIC) }.max

puts "share consumer statistics OK"
puts "  unfiltered: #{unfiltered_stats.size} callbacks, max #{unfiltered_visible}/#{PARTITIONS} partitions visible"
puts "  filtered:   #{filtered_stats.size} callbacks, max #{filtered_visible}/#{PARTITIONS} partitions visible"
