# frozen_string_literal: true

# This integration test verifies KIP-932 share consumer main-queue servicing via
# ShareConsumer#events_poll:
# - the statistics callback keeps firing for this consumer while ONLY #events_poll is called and
#   #poll is never called - proving the global callbacks (statistics/error/log/oauth) are serviced
#   independently of record acquisition, which #poll would otherwise be the only driver of
# - events_poll acquires no records: a consumer that acquired records, then stops #poll and only
#   pumps #events_poll, sees its statistics advance without any further records being delivered
# - events_poll on a closed consumer raises ClosedConsumerError
#
# This covers a caller that is waiting on in-flight work, quieting, or draining on shutdown: it must
# not acquire new records (so it can't call #poll) yet still needs statistics/error/oauth callbacks
# flowing.
#
# Requires a running Kafka broker with share groups enabled at 127.0.0.1:9092.
#
# Exit codes:
# - 0: events_poll behaves as expected (test passes)
# - 1: An assertion failed

require "rdkafka"
require "securerandom"

$stdout.sync = true

BOOTSTRAP = "127.0.0.1:9092"
TOPIC = "share-events-poll-#{SecureRandom.hex(6)}"
GROUP = "share-events-poll-group-#{SecureRandom.hex(4)}"
PARTITIONS = 4
MESSAGES = 20

def assert(condition, message)
  return if condition

  puts "FAILED: #{message}"
  exit 1
end

# share.auto.offset.reset is a broker-side group config defaulting to latest; set it to earliest
# before the group first attaches so the pre-produced records can be delivered.
admin = Rdkafka::Config.new("bootstrap.servers": BOOTSTRAP).admin
admin.create_topic(TOPIC, PARTITIONS, 1).wait(max_wait_timeout_ms: 15_000)
admin.incremental_alter_configs(
  [
    {
      resource_type: Rdkafka::Bindings::RD_KAFKA_RESOURCE_GROUP,
      resource_name: GROUP,
      configs: [{ name: "share.auto.offset.reset", value: "earliest", op_type: 0 }]
    }
  ]
).wait(max_wait_timeout_ms: 15_000)
admin.close

producer = Rdkafka::Config.new("bootstrap.servers": BOOTSTRAP).producer
handles = MESSAGES.times.map do |i|
  producer.produce(topic: TOPIC, payload: "payload-#{i}", partition: i % PARTITIONS)
end
handles.each { |handle| handle.wait(max_wait_timeout_ms: 15_000) }
producer.close

stats = []
Rdkafka::Config.statistics_callback = ->(published) { stats << published }

consumer = Rdkafka::Config.new(
  "bootstrap.servers": BOOTSTRAP,
  "group.id": GROUP,
  "statistics.interval.ms": 100
).share_consumer

# ShareConsumer#name is available immediately (before the first poll); it is what correlates this
# consumer with the statistics it emits.
name = consumer.name
assert(name.is_a?(String) && !name.empty?, "expected a non-empty ShareConsumer#name, got #{name.inspect}")

consumer.subscribe(TOPIC)

# Phase 1: acquire some records with #poll so the consumer is a live, attached member.
consumed = 0
30.times do
  consumed += consumer.poll(500).size
  break if consumed >= 1
end
assert(consumed >= 1, "expected to acquire at least one record via #poll, got #{consumed}")

# Phase 2: stop polling for records entirely. Only pump #events_poll. Record how many events the
# main queue serves and how many stats callbacks for THIS consumer arrive during this window.
stats_before = stats.count { |s| s["name"] == name }
events_served = 0

# events_poll must never deliver records - it returns an Integer event count, not messages.
50.times do
  served = consumer.events_poll(100)
  assert(served.is_a?(Integer), "expected events_poll to return an Integer event count, got #{served.inspect}")
  events_served += served

  # Stop once we have observed fresh statistics arrive purely through events_poll.
  break if stats.count { |s| s["name"] == name } - stats_before >= 2
end

stats_after = stats.count { |s| s["name"] == name }
fresh_stats = stats_after - stats_before

assert(
  fresh_stats >= 2,
  "expected the statistics callback to keep firing for #{name.inspect} while only events_poll ran, " \
  "got #{fresh_stats} new callbacks (#{stats_before} -> #{stats_after})"
)
assert(events_served >= 1, "expected events_poll to serve at least one main-queue event, got #{events_served}")

# events_poll_nb (non-GVL-releasing variant) must behave the same way: an Integer count, no records.
nb_served = consumer.events_poll_nb(0)
assert(nb_served.is_a?(Integer), "expected events_poll_nb to return an Integer, got #{nb_served.inspect}")

consumer.close
Rdkafka::Config.statistics_callback = nil

# Phase 3: events_poll on a closed consumer raises ClosedConsumerError (mirrors #poll).
closed_raised = begin
  consumer.events_poll(0)
  false
rescue Rdkafka::ClosedConsumerError
  true
end
assert(closed_raised, "expected events_poll on a closed consumer to raise ClosedConsumerError")

closed_nb_raised = begin
  consumer.events_poll_nb(0)
  false
rescue Rdkafka::ClosedConsumerError
  true
end
assert(closed_nb_raised, "expected events_poll_nb on a closed consumer to raise ClosedConsumerError")

puts "share consumer events_poll OK"
puts "  acquired #{consumed} record(s) via poll, then served #{events_served} main-queue event(s) via events_poll only"
puts "  statistics callbacks for #{name}: +#{fresh_stats} during the events_poll-only window"
