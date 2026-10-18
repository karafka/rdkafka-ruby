# frozen_string_literal: true

# This integration test verifies that a consumer with `group.id` gets coordinator errors
# (NOT_COORDINATOR and COORDINATOR_NOT_AVAILABLE) on OffsetFetch from `#committed`, and then
# reads the committed offset once its consumer group has found the coordinator again.
#
# This is the path that keeps re-querying the coordinator through the consumer group after
# librdkafka's OffsetFetch fix for clients without one (confluentinc/librdkafka#5611).
#
# Uses librdkafka's mock cluster, so it needs no Kafka broker. A crash in librdkafka kills the
# process with a signal instead of a 0 or 1 exit code.
#
# Exit codes:
# - 0: each error was raised and the committed offset was read afterwards
# - 1: otherwise

require_relative "support/mock_cluster"
require "securerandom"

$stdout.sync = true

TOPIC = "it-consumer-group-coordinator-errors-#{SecureRandom.hex(6)}"
WAIT_MS = 10_000
ERRORS = %i[coordinator_not_available not_coordinator].freeze

failures = []

def tpl(offset = nil)
  Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
    if offset
      list.add_topic_and_partitions_with_offsets(TOPIC, 0 => offset)
    else
      list.add_topic(TOPIC, [0])
    end
  end
end

MockCluster.run do |cluster|
  cluster.create_topic(TOPIC, 1)
  consumer = cluster.config(
    "group.id": "it-consumer-group-coordinator-errors-#{SecureRandom.hex(6)}",
    "enable.auto.commit": false
  ).consumer

  consumer.commit(tpl(3), false)

  ERRORS.each do |error|
    cluster.push_request_errors(:offset_fetch, error)

    begin
      consumer.committed(tpl, WAIT_MS)
      failures << "expected #{error} on OffsetFetch, got success"
    rescue Rdkafka::RdkafkaError => e
      failures << "expected #{error}, got #{e.code}" unless e.code == error
    end
  end

  # The consumer group re-queries the coordinator in the background after the errors
  deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + WAIT_MS / 1_000.0

  begin
    offset = consumer.committed(tpl, WAIT_MS).to_h.fetch(TOPIC).first.offset
  rescue Rdkafka::RdkafkaError => e
    raise unless ERRORS.include?(e.code)
    raise if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline

    sleep(0.1)
    retry
  end

  failures << "expected offset 3, got #{offset.inspect}" unless offset == 3
rescue => e
  failures << "#{e.class}: #{e.message}"
ensure
  consumer&.close
end

if failures.empty?
  puts "OK"
  exit(0)
end

failures.each { |failure| puts "FAIL: #{failure}" }
exit(1)
