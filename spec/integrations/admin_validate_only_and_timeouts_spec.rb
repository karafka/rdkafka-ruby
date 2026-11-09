# frozen_string_literal: true

# This integration test verifies that the admin create and delete calls accept the
# `validate_only:`, `operation_timeout_ms:` and `request_timeout_ms:` keyword options, and that a
# topic config given as a hash without braces still works next to them.
#
# Requires a running Kafka broker at localhost:9092.
#
# Scenarios:
# - `create_topic(..., validate_only: true)` succeeds and creates no topic
# - `create_topic(name, 1, 1, "cleanup.policy" => "compact")` (no braces) applies the config
# - `create_partitions(..., validate_only: true)` leaves the partition count unchanged
# - `create_partitions` and `delete_topic` with both timeouts succeed
# - an out of range timeout raises `invalid_arg` before anything is sent
# - against an unreachable broker, `request_timeout_ms` fails the request long before the
#   default `socket.timeout.ms` (60 s)
#
# Exit codes:
# - 0: all scenarios behaved as expected
# - 1: a scenario did not

require "rdkafka"
require "securerandom"

WAIT_MS = 15_000

def wait_for_partition_count(admin, topic_name, expected)
  deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 10

  loop do
    begin
      count = admin.metadata(topic_name).topics.first[:partition_count]
      return count if count == expected
    rescue Rdkafka::RdkafkaError
      nil
    end

    return nil if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline

    sleep(0.1)
  end
end

def topic_exists?(admin, topic_name)
  admin.metadata(topic_name)
  true
rescue Rdkafka::RdkafkaError => e
  raise unless e.code == :unknown_topic_or_part

  false
end

admin = Rdkafka::Config.new("bootstrap.servers": "localhost:9092").admin
validated_topic = "it-validate-only-#{SecureRandom.hex(6)}"
topic_name = "it-admin-options-#{SecureRandom.hex(6)}"

begin
  report = admin.create_topic(validated_topic, 1, 1, validate_only: true).wait(max_wait_timeout_ms: WAIT_MS)
  exit(1) unless report.result_name == validated_topic
  sleep(1)
  exit(1) if topic_exists?(admin, validated_topic)

  admin.create_topic(topic_name, 1, 1, "cleanup.policy" => "compact").wait(max_wait_timeout_ms: WAIT_MS)
  exit(1) unless wait_for_partition_count(admin, topic_name, 1)

  cleanup_policy = admin
    .describe_configs([{ resource_type: 2, resource_name: topic_name }])
    .wait(max_wait_timeout_ms: WAIT_MS)
    .resources
    .first
    .configs
    .find { |config| config.name == "cleanup.policy" }
  exit(1) unless cleanup_policy&.value == "compact"

  admin.create_partitions(topic_name, 3, validate_only: true).wait(max_wait_timeout_ms: WAIT_MS)
  sleep(1)
  exit(1) unless wait_for_partition_count(admin, topic_name, 1)

  admin
    .create_partitions(topic_name, 3, operation_timeout_ms: 30_000, request_timeout_ms: 30_000)
    .wait(max_wait_timeout_ms: WAIT_MS)
  exit(1) unless wait_for_partition_count(admin, topic_name, 3)

  begin
    admin.create_partitions(topic_name, 4, request_timeout_ms: -1)
    exit(1)
  rescue Rdkafka::RdkafkaError => e
    exit(1) unless e.code == :invalid_arg
  end

  unreachable_admin = Rdkafka::Config.new("bootstrap.servers": "127.0.0.1:65010").admin
  handle = unreachable_admin.create_topic("it-unreachable-#{SecureRandom.hex(6)}", 1, 1, request_timeout_ms: 1_000)
  started_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)

  begin
    handle.wait(max_wait_timeout_ms: WAIT_MS)
    exit(1)
  rescue Rdkafka::RdkafkaError => e
    exit(1) unless e.code == :timed_out
    exit(1) if Process.clock_gettime(Process::CLOCK_MONOTONIC) - started_at > 10
  ensure
    unreachable_admin.close
  end

  report = admin
    .delete_topic(topic_name, operation_timeout_ms: 30_000, request_timeout_ms: 30_000)
    .wait(max_wait_timeout_ms: WAIT_MS)
  exit(1) unless report.result_name == topic_name
ensure
  admin.close
end
