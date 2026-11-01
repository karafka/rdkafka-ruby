# frozen_string_literal: true

# This integration test shows how to describe topics with `Admin#describe_topics` and verifies the
# result end to end.
#
# `describe_topics` returns one entry per requested topic: its name, its topic id (KIP-516, base64),
# whether it is internal, and every partition with its leader, replicas and in-sync replicas (as
# broker nodes). A topic that cannot be described does not fail the request; its entry carries an
# `:error` and a `nil` topic id instead.
#
# Requires a running Kafka broker at localhost:9092.
#
# Exit codes:
# - 0: the topics were described correctly
# - 1: a described value was missing or wrong

require "rdkafka"
require "securerandom"

$stdout.sync = true

PARTITIONS = 3

# A topic that was just created can be missing from the broker's metadata for a moment, so retry
# while the new topic is reported as unknown
def describe_topics_settled(admin, topic_name, topic_names)
  deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 10

  loop do
    report = admin.describe_topics(topic_names).wait(max_wait_timeout_ms: 15_000)
    error = report.topics.find { |topic| topic[:name] == topic_name }&.fetch(:error)

    return report unless error&.code == :unknown_topic_or_part
    return report if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline

    sleep(0.1)
  end
end

admin = Rdkafka::Config.new("bootstrap.servers": "localhost:9092").admin
topic_name = "it-describe-topics-#{SecureRandom.hex(6)}"
missing_topic_name = "it-describe-topics-missing-#{SecureRandom.hex(6)}"

begin
  admin.create_topic(topic_name, PARTITIONS, 1).wait(max_wait_timeout_ms: 15_000)

  nodes = admin.describe_cluster.wait(max_wait_timeout_ms: 15_000).nodes
  report = describe_topics_settled(admin, topic_name, [topic_name, missing_topic_name])
  topics = report.topics.to_h { |topic| [topic[:name], topic] }

  topic = topics.fetch(topic_name) { exit(1) }

  exit(1) unless topic[:error].nil?
  exit(1) if topic[:topic_id].to_s.empty?
  exit(1) unless topic[:is_internal] == false
  exit(1) unless topic[:partitions].map { |partition| partition[:partition] } == (0...PARTITIONS).to_a

  topic[:partitions].each do |partition|
    exit(1) unless nodes.include?(partition[:leader])
    exit(1) unless !partition[:replicas].empty? && (partition[:replicas] - nodes).empty?
    exit(1) unless !partition[:isr].empty? && (partition[:isr] - partition[:replicas]).empty?
  end

  missing = topics.fetch(missing_topic_name) { exit(1) }

  exit(1) unless missing[:error].is_a?(Rdkafka::RdkafkaError)
  exit(1) unless missing[:error].code == :unknown_topic_or_part
  exit(1) unless missing[:topic_id].nil?
ensure
  begin
    admin.delete_topic(topic_name).wait(max_wait_timeout_ms: 15_000)
  rescue Rdkafka::RdkafkaError
    nil
  end

  admin.close
end

exit(0)
