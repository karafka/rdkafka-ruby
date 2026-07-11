# frozen_string_literal: true

# This integration test verifies that Consumer#list_offsets (and Consumer#lag built on top of
# it) is drained by librdkafka's background queue/thread and never depends on the application
# draining the consumer via #poll / #events_poll:
#
# 1. A consumer that is never polled fires the batched query and the handle resolves with the
#    correct offsets - no message-queue or events-queue draining happens at query time.
# 2. A query fired from inside a rebalance callback resolves while the application thread is
#    blocked mid-poll inside that callback (rebalance callbacks run in the middle of
#    rd_kafka_consumer_poll), proving result delivery does not depend on the thread that fired
#    the query returning to its poll loop.
# 3. A query fired from inside a statistics callback (also invoked mid-poll on the application
#    thread) resolves the same way.
# 4. Queries fired from a separate Ruby thread while another thread actively polls the same
#    consumer resolve correctly - the batched query neither depends on nor disturbs the poll
#    loop running concurrently.
#
# Requires a running Kafka broker at localhost:9092.
#
# Exit codes:
# - 0: all scenarios resolved with the expected offsets
# - 1: any scenario failed or timed out

require "rdkafka"
require "securerandom"

$stdout.sync = true

BOOTSTRAP = "localhost:9092"
TOPIC = "it-list-offsets-bg-#{SecureRandom.hex(6)}"
MESSAGES = 3
WAIT_MS = 30_000
DEADLINE_S = 60

failures = []

admin = Rdkafka::Config.new("bootstrap.servers": BOOTSTRAP).admin
admin.create_topic(TOPIC, 1, 1).wait(max_wait_timeout_ms: WAIT_MS)
admin.close

producer = Rdkafka::Config.new("bootstrap.servers": BOOTSTRAP).producer
MESSAGES.times { |i| producer.produce(topic: TOPIC, payload: "m#{i}", partition: 0).wait }
producer.close

def build_config(extra = {})
  Rdkafka::Config.new(
    {
      "bootstrap.servers": BOOTSTRAP,
      "group.id": "it-list-offsets-bg-#{SecureRandom.hex(6)}",
      "auto.offset.reset": "earliest"
    }.merge(extra)
  )
end

# Runs the batched query on the given consumer and returns the resolved latest offset of
# partition 0. Any error (including a wait timeout) is captured by the caller.
def query_latest(consumer)
  report = consumer.list_offsets(
    { TOPIC => [{ partition: 0, offset: :latest }] }
  ).wait(max_wait_timeout_ms: WAIT_MS)

  report.offsets.first[:offset]
end

# Scenario 1: the consumer is never polled - neither the consumer (message) queue nor the
# main (events) queue is ever drained by the application - yet the handle must resolve,
# because the result is delivered on the background queue served by librdkafka itself.
begin
  consumer = build_config.consumer

  offset = query_latest(consumer)
  if offset != MESSAGES
    failures << "never-polled consumer: expected latest offset #{MESSAGES}, got #{offset.inspect}"
  end

  # The public #lag path must work undrained as well.
  tpl = Rdkafka::Consumer::TopicPartitionList.new
  tpl.add_topic_and_partitions_with_offsets(TOPIC, 0 => 1)
  lag = consumer.lag(tpl, WAIT_MS)
  if lag != { TOPIC => { 0 => MESSAGES - 1 } }
    failures << "never-polled consumer: expected lag #{MESSAGES - 1}, got #{lag.inspect}"
  end
rescue => e
  failures << "never-polled consumer: #{e.class}: #{e.message}"
ensure
  consumer&.close
end

# Scenario 2: fire and wait from inside a rebalance callback. The callback runs on the
# application thread in the middle of rd_kafka_consumer_poll, so resolving here proves the
# delivery path is independent of the firing thread's poll loop.
begin
  holder = { consumer: nil, result: :not_run }

  listener = Object.new
  listener.define_singleton_method(:on_partitions_assigned) do |_list|
    holder[:result] = begin
      query_latest(holder[:consumer])
    rescue => e
      e
    end
  end
  listener.define_singleton_method(:on_partitions_revoked) { |_list| }

  config = build_config
  config.consumer_rebalance_listener = listener
  consumer = holder[:consumer] = config.consumer

  consumer.subscribe(TOPIC)
  deadline = Time.now + DEADLINE_S
  consumer.poll(250) while holder[:result] == :not_run && Time.now < deadline

  if holder[:result] != MESSAGES
    failures << "rebalance callback: expected latest offset #{MESSAGES}, got #{holder[:result].inspect}"
  end
rescue => e
  failures << "rebalance callback: #{e.class}: #{e.message}"
ensure
  consumer&.close
end

# Scenario 3: fire and wait from inside a statistics callback, which is likewise invoked
# mid-poll on the application thread.
begin
  holder = { consumer: nil, result: :not_run }

  Rdkafka::Config.statistics_callback = lambda do |_stats|
    next unless holder[:result] == :not_run

    holder[:result] = begin
      query_latest(holder[:consumer])
    rescue => e
      e
    end
  end

  consumer = holder[:consumer] = build_config("statistics.interval.ms": 500).consumer

  consumer.subscribe(TOPIC)
  deadline = Time.now + DEADLINE_S
  consumer.poll(250) while holder[:result] == :not_run && Time.now < deadline

  if holder[:result] != MESSAGES
    failures << "statistics callback: expected latest offset #{MESSAGES}, got #{holder[:result].inspect}"
  end
rescue => e
  failures << "statistics callback: #{e.class}: #{e.message}"
ensure
  Rdkafka::Config.statistics_callback = nil
  consumer&.close
end

# Scenario 4: fire from a separate Ruby thread while the application thread is actively
# polling the same consumer.
begin
  consumer = build_config.consumer
  consumer.subscribe(TOPIC)

  polling = true
  poller = Thread.new do
    while polling
      begin
        consumer.poll(100)
      rescue Rdkafka::RdkafkaError
        # Transient poll errors are irrelevant to the side-thread query under test
      end
    end
  end

  results = Array.new(3) { query_latest(consumer) }

  if results != [MESSAGES] * 3
    failures << "concurrent thread: expected #{[MESSAGES] * 3}, got #{results.inspect}"
  end
rescue => e
  failures << "concurrent thread: #{e.class}: #{e.message}"
ensure
  polling = false
  poller&.join
  consumer&.close
end

if failures.empty?
  puts "PASS: list_offsets resolved without polling, from rebalance and statistics callbacks, and from a concurrent thread"
  exit(0)
else
  failures.each { |failure| warn "FAIL: #{failure}" }
  exit(1)
end
