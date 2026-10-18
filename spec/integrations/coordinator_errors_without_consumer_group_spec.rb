# frozen_string_literal: true

# This integration test verifies that coordinator errors (NOT_COORDINATOR and
# COORDINATOR_NOT_AVAILABLE) on group offset requests do not crash clients that have no consumer
# group (no `group.id`), such as the producer-backed admin client.
#
# librdkafka's OffsetFetch response handler re-queries the coordinator through the client's
# consumer group without checking that the client has one (confluentinc/librdkafka#5611). Such
# a client must therefore never reach that handler:
#
# 1. The admin client gets both errors on AlterConsumerGroupOffsets (OffsetCommit). The call
#    raises the broker error and the next call succeeds.
# 2. A consumer without `group.id` is refused by `#committed` with `unknown_group` before any
#    OffsetFetch is sent, even when OffsetFetch would fail with both errors.
# 3. A consumer with `group.id` recovers from both errors on OffsetFetch and reads the committed
#    offset.
#
# Uses librdkafka's mock cluster, so it needs no Kafka broker. A crash in librdkafka kills the
# process with a signal instead of a 0 or 1 exit code.
#
# Exit codes:
# - 0: all scenarios behaved as expected
# - 1: any scenario failed

require "rdkafka"
require "securerandom"

$stdout.sync = true

# librdkafka mock cluster API, not part of the public bindings
module MockCluster
  extend FFI::Library

  ffi_lib Rdkafka::Bindings.ffi_libraries.map(&:name)

  attach_function :rd_kafka_mock_cluster_new, %i[pointer int], :pointer
  attach_function :rd_kafka_mock_cluster_destroy, %i[pointer], :void
  attach_function :rd_kafka_mock_cluster_bootstraps, %i[pointer], :string
  attach_function :rd_kafka_mock_topic_create, %i[pointer string int int], :int
  attach_function :rd_kafka_mock_push_request_errors_array, %i[pointer int16 size_t pointer], :void
end

API_KEY_OFFSET_COMMIT = 8
API_KEY_OFFSET_FETCH = 9
COORDINATOR_ERRORS = {
  coordinator_not_available: 15,
  not_coordinator: 16
}.freeze
TOPIC = "it-coordinator-errors-#{SecureRandom.hex(6)}"
WAIT_MS = 10_000

failures = []

# Makes the mock cluster fail the next request of the given API with the given error
# @param mcluster [FFI::Pointer] mock cluster
# @param api_key [Integer] Kafka protocol API key
# @param error_code [Integer] Kafka protocol error code
def push_error(mcluster, api_key, error_code)
  errors = FFI::MemoryPointer.new(:int, 1)
  errors.write_array_of_int([error_code])
  MockCluster.rd_kafka_mock_push_request_errors_array(mcluster, api_key, 1, errors)
end

# @param offset [Integer] offset of partition 0 of the topic
# @return [Rdkafka::Consumer::TopicPartitionList]
def tpl(offset = nil)
  Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
    if offset
      list.add_topic_and_partitions_with_offsets(TOPIC, 0 => offset)
    else
      list.add_topic(TOPIC, [0])
    end
  end
end

# Runs the block against its own mock cluster, so errors that one scenario injects and never
# triggers are not left for the next scenario
# @param host [Rdkafka::Producer] client that hosts the mock cluster
# @yieldparam mcluster [FFI::Pointer] mock cluster
# @yieldparam bootstraps [String] bootstrap servers of the mock cluster
def with_mock_cluster(host)
  mcluster = host.instance_variable_get(:@native_kafka).with_inner do |inner|
    MockCluster.rd_kafka_mock_cluster_new(inner, 1)
  end
  MockCluster.rd_kafka_mock_topic_create(mcluster, TOPIC, 1, 1)

  yield(mcluster, MockCluster.rd_kafka_mock_cluster_bootstraps(mcluster))
ensure
  MockCluster.rd_kafka_mock_cluster_destroy(mcluster) if mcluster
end

host = Rdkafka::Config.new.producer

# Scenario 1: producer-backed admin client
with_mock_cluster(host) do |mcluster, bootstraps|
  admin = Rdkafka::Config.new("bootstrap.servers": bootstraps).admin
  group_id = "it-coordinator-errors-#{SecureRandom.hex(6)}"

  COORDINATOR_ERRORS.each do |name, code|
    push_error(mcluster, API_KEY_OFFSET_COMMIT, code)

    begin
      admin.alter_consumer_group_offsets(group_id, tpl(1)).wait(max_wait_timeout_ms: WAIT_MS)
      failures << "admin: expected #{name} on AlterConsumerGroupOffsets, got success"
    rescue Rdkafka::RdkafkaError => e
      failures << "admin: expected #{name}, got #{e.code}" unless e.code == name
    end
  end

  admin.alter_consumer_group_offsets(group_id, tpl(7)).wait(max_wait_timeout_ms: WAIT_MS)
rescue => e
  failures << "admin: #{e.class}: #{e.message}"
ensure
  admin&.close
end

# Scenario 2: consumer without group.id
with_mock_cluster(host) do |mcluster, bootstraps|
  consumer = Rdkafka::Config.new("bootstrap.servers": bootstraps).consumer

  COORDINATOR_ERRORS.each_value { |code| push_error(mcluster, API_KEY_OFFSET_FETCH, code) }

  begin
    consumer.committed(tpl, WAIT_MS)
    failures << "consumer without group.id: expected unknown_group, got success"
  rescue Rdkafka::RdkafkaError => e
    unless e.code == :unknown_group
      failures << "consumer without group.id: expected unknown_group, got #{e.code}"
    end
  end
rescue => e
  failures << "consumer without group.id: #{e.class}: #{e.message}"
ensure
  consumer&.close
end

# Scenario 3: consumer with group.id
with_mock_cluster(host) do |mcluster, bootstraps|
  consumer = Rdkafka::Config.new(
    "bootstrap.servers": bootstraps,
    "group.id": "it-coordinator-errors-#{SecureRandom.hex(6)}",
    "enable.auto.commit": false
  ).consumer

  consumer.commit(tpl(3), false)

  COORDINATOR_ERRORS.each do |name, code|
    push_error(mcluster, API_KEY_OFFSET_FETCH, code)

    begin
      consumer.committed(tpl, WAIT_MS)
      failures << "consumer with group.id: expected #{name} on OffsetFetch, got success"
    rescue Rdkafka::RdkafkaError => e
      failures << "consumer with group.id: expected #{name}, got #{e.code}" unless e.code == name
    end
  end

  # The consumer group re-queries the coordinator in the background after the errors
  deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + WAIT_MS / 1_000.0

  begin
    offset = consumer.committed(tpl, WAIT_MS).to_h.fetch(TOPIC).first.offset
  rescue Rdkafka::RdkafkaError => e
    raise unless COORDINATOR_ERRORS.key?(e.code)
    raise if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline

    sleep(0.1)
    retry
  end

  failures << "consumer with group.id: expected offset 3, got #{offset.inspect}" unless offset == 3
rescue => e
  failures << "consumer with group.id: #{e.class}: #{e.message}"
ensure
  consumer&.close
end

host.close

if failures.empty?
  puts "OK"
  exit(0)
end

failures.each { |failure| puts "FAIL: #{failure}" }
exit(1)
