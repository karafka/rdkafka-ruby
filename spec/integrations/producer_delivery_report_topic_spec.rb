# frozen_string_literal: true

# This integration test verifies that the delivery callback reports the topic each message was
# produced to, taking it from the delivery handle instead of reading it back from librdkafka.
#
# The delivery callback used to call `rd_kafka_topic_name` for every delivered message, allocating
# a new String that always equals the topic the message was produced to. The report now carries the
# topic String given to `produce`, as `DeliveryHandle#wait` already did.
#
# Scenarios:
# 1. Messages produced to two topics are reported with their own topic, in the delivery callback
#    and by `DeliveryHandle#wait`
# 2. A message that cannot be delivered is still reported with its topic
#
# In both, `rd_kafka_topic_name` must not be called.
#
# Requires a running Kafka broker at localhost:9092.
#
# Exit codes:
# - 0: every report carried its topic and no topic name was read back from librdkafka
# - 1: a report carried a wrong topic, or `rd_kafka_topic_name` was called

require "rdkafka"
require "securerandom"
require_relative "support/native_call_counter"

$stdout.sync = true

NativeCallCounter.watch(:rd_kafka_topic_name)

MESSAGES_PER_TOPIC = 50

def fail!(message)
  warn "FAIL: #{message}"
  exit(1)
end

config = Rdkafka::Config.new("bootstrap.servers": "localhost:9092")
admin = config.admin
producer = config.producer
topics = Array.new(2) { "it-delivery-topic-#{SecureRandom.hex(6)}" }

begin
  topics.each { |topic| admin.create_topic(topic, 1, 1).wait(max_wait_timeout_ms: 15_000) }

  deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 15
  until topics.all? { |topic| admin.metadata(topic).topics.first&.dig(:partitions)&.any? }
    fail!("topics not visible in metadata") if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline

    sleep(0.1)
  end

  callback_reports = Queue.new
  producer.delivery_callback = ->(report) { callback_reports << report }

  handles = topics.flat_map do |topic|
    Array.new(MESSAGES_PER_TOPIC) { [topic, producer.produce(topic: topic, payload: "payload")] }
  end

  handles.each do |topic, handle|
    report = handle.wait(max_wait_timeout_ms: 15_000)

    fail!("wait reported #{report.topic_name.inspect}, expected #{topic}") unless report.topic_name == topic
  end

  producer.flush

  reported = Array.new(callback_reports.size) { callback_reports.pop.topic_name }.tally
  expected = topics.to_h { |topic| [topic, MESSAGES_PER_TOPIC] }

  puts "Delivered: #{reported}"

  fail!("delivery callback reported #{reported}, expected #{expected}") unless reported == expected
ensure
  topics.each do |topic|
    admin.delete_topic(topic).wait(max_wait_timeout_ms: 15_000)
  rescue Rdkafka::RdkafkaError
    nil
  end

  producer.close
  admin.close
end

unreachable = Rdkafka::Config.new("bootstrap.servers": "127.0.0.1:9095", "message.timeout.ms": 500).producer
failed_reports = Queue.new
unreachable.delivery_callback = ->(report) { failed_reports << report }

begin
  report = unreachable.produce(topic: topics.first, payload: "payload")
    .wait(max_wait_timeout_ms: 15_000, raise_response_error: false)

  fail!("expected a delivery error") unless report.error.is_a?(Rdkafka::RdkafkaError)

  callback_report = failed_reports.pop(timeout: 15)

  puts "Undeliverable: error=#{report.error.code.inspect} topic=#{callback_report&.topic_name}"

  fail!("failed delivery reported #{callback_report&.topic_name.inspect}") unless callback_report&.topic_name == topics.first
ensure
  unreachable.close
end

calls = NativeCallCounter.count(:rd_kafka_topic_name)

fail!("rd_kafka_topic_name was called #{calls} times") unless calls.zero?

puts "PASS: delivery reports carried their topics without reading them back from librdkafka"
exit(0)
