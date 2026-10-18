# frozen_string_literal: true

# This integration test verifies that a consumer without `group.id` never sends an OffsetFetch,
# so coordinator errors (NOT_COORDINATOR and COORDINATOR_NOT_AVAILABLE) on it cannot crash it.
#
# librdkafka's OffsetFetch response handler re-queries the coordinator through the client's
# consumer group without checking that the client has one (confluentinc/librdkafka#5611).
# `#committed` on a consumer without a consumer group raises `unknown_group` before any request,
# even when the next OffsetFetch requests would fail with both errors.
#
# Uses librdkafka's mock cluster, so it needs no Kafka broker. A crash in librdkafka kills the
# process with a signal instead of a 0 or 1 exit code.
#
# Exit codes:
# - 0: `#committed` raised `unknown_group`
# - 1: otherwise

require_relative "support/mock_cluster"
require "securerandom"

$stdout.sync = true

TOPIC = "it-consumer-no-group-coordinator-errors-#{SecureRandom.hex(6)}"
WAIT_MS = 10_000

failures = []

MockCluster.run do |cluster|
  cluster.create_topic(TOPIC, 1)
  cluster.push_request_errors(:offset_fetch, :coordinator_not_available, :not_coordinator)
  consumer = cluster.config.consumer

  tpl = Rdkafka::Consumer::TopicPartitionList.new
  tpl.add_topic(TOPIC, [0])

  begin
    consumer.committed(tpl, WAIT_MS)
    failures << "expected unknown_group, got success"
  rescue Rdkafka::RdkafkaError => e
    failures << "expected unknown_group, got #{e.code}" unless e.code == :unknown_group
  end
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
