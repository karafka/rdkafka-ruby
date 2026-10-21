# frozen_string_literal: true

# This integration test verifies that coordinator errors (NOT_COORDINATOR and
# COORDINATOR_NOT_AVAILABLE) on AlterConsumerGroupOffsets do not crash the admin client, which is
# producer-backed and has no consumer group.
#
# librdkafka's OffsetFetch response handler re-queries the coordinator through the client's
# consumer group without checking that the client has one (confluentinc/librdkafka#5611). The
# OffsetCommit handler used by AlterConsumerGroupOffsets checks it, so each call raises the broker
# error and the next call succeeds.
#
# Uses librdkafka's mock cluster, so it needs no Kafka broker. A crash in librdkafka kills the
# process with a signal instead of a 0 or 1 exit code.
#
# Exit codes:
# - 0: each error was raised and the next call succeeded
# - 1: otherwise

require_relative "support/mock_cluster"
require "securerandom"

$stdout.sync = true

TOPIC = "it-admin-coordinator-errors-#{SecureRandom.hex(6)}"
GROUP_ID = "it-admin-coordinator-errors-#{SecureRandom.hex(6)}"
WAIT_MS = 10_000
ERRORS = %i[coordinator_not_available not_coordinator].freeze

failures = []

def tpl(offset)
  Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
    list.add_topic_and_partitions_with_offsets(TOPIC, 0 => offset)
  end
end

MockCluster.run do |cluster|
  cluster.create_topic(TOPIC, 1)
  admin = cluster.config.admin

  ERRORS.each do |error|
    cluster.push_request_errors(:offset_commit, error)

    begin
      admin.alter_consumer_group_offsets(GROUP_ID, tpl(1)).wait(max_wait_timeout_ms: WAIT_MS)
      failures << "expected #{error} on AlterConsumerGroupOffsets, got success"
    rescue Rdkafka::RdkafkaError => e
      failures << "expected #{error}, got #{e.code}" unless e.code == error
    end
  end

  admin.alter_consumer_group_offsets(GROUP_ID, tpl(7)).wait(max_wait_timeout_ms: WAIT_MS)
rescue => e
  failures << "#{e.class}: #{e.message}"
ensure
  admin&.close
end

if failures.empty?
  puts "OK"
  exit(0)
end

failures.each { |failure| puts "FAIL: #{failure}" }
exit(1)
