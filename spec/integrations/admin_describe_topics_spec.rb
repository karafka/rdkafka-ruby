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

def fail!(message)
  warn "FAIL: #{message}"
  exit(1)
end

admin = Rdkafka::Config.new("bootstrap.servers": "localhost:9092").admin
topic_name = "it-describe-topics-#{SecureRandom.hex(6)}"
missing_topic_name = "it-describe-topics-missing-#{SecureRandom.hex(6)}"

begin
  admin.create_topic(topic_name, PARTITIONS, 1).wait(max_wait_timeout_ms: 15_000)

  nodes = admin.describe_cluster.wait(max_wait_timeout_ms: 15_000).nodes
  report = admin.describe_topics([topic_name, missing_topic_name]).wait(max_wait_timeout_ms: 15_000)
  topics = report.topics.to_h { |topic| [topic[:name], topic] }

  topic = topics.fetch(topic_name) { fail!("#{topic_name} was not described") }

  puts "Topic: #{topic[:name]} id=#{topic[:topic_id]} internal=#{topic[:is_internal]}"
  topic[:partitions].each do |partition|
    puts "  Partition #{partition[:partition]}: leader=#{partition[:leader]&.fetch(:id)} " \
         "replicas=#{partition[:replicas].map { |node| node[:id] }} isr=#{partition[:isr].map { |node| node[:id] }}"
  end

  fail!("unexpected error: #{topic[:error].inspect}") unless topic[:error].nil?
  fail!("topic id is empty") if topic[:topic_id].to_s.empty?
  fail!("topic is reported as internal") unless topic[:is_internal] == false

  indexes = topic[:partitions].map { |partition| partition[:partition] }
  fail!("expected partitions #{(0...PARTITIONS).to_a}, got #{indexes}") unless indexes == (0...PARTITIONS).to_a

  topic[:partitions].each do |partition|
    fail!("leader #{partition[:leader].inspect} is not a cluster node") unless nodes.include?(partition[:leader])
    fail!("replicas #{partition[:replicas].inspect} are not cluster nodes") unless !partition[:replicas].empty? && (partition[:replicas] - nodes).empty?
    fail!("isr #{partition[:isr].inspect} is not a subset of replicas") unless !partition[:isr].empty? && (partition[:isr] - partition[:replicas]).empty?
  end

  missing = topics.fetch(missing_topic_name) { fail!("#{missing_topic_name} was not described") }

  puts "Missing topic: #{missing[:name]} error=#{missing[:error]&.code.inspect} id=#{missing[:topic_id].inspect}"

  fail!("missing topic has no error") unless missing[:error].is_a?(Rdkafka::RdkafkaError)
  fail!("missing topic has an unexpected error: #{missing[:error].code}") unless missing[:error].code == :unknown_topic_or_part
  fail!("missing topic has a topic id: #{missing[:topic_id]}") unless missing[:topic_id].nil?
ensure
  begin
    admin.delete_topic(topic_name).wait(max_wait_timeout_ms: 15_000)
  rescue Rdkafka::RdkafkaError
    nil
  end

  admin.close
end

puts "PASS: the topics were described correctly"
exit(0)
