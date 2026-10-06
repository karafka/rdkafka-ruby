# frozen_string_literal: true

# This integration test shows how to read the persistence status, latency and broker of a produced
# message from its delivery report, and verifies them end to end.
#
# `DeliveryReport#status` tells a caller whether a failed message is safe to retry:
#   - `#persisted?` - the broker acknowledged the message (trust this only with `acks=all`)
#   - `#possibly_persisted?` - the message may have been written, so a retry may duplicate it
#   - `#not_persisted?` - the message was not written, so a retry does not duplicate it
# `#latency` is the time from `produce` to the delivery report in microseconds, and `#broker_id`
# is the broker the message was produced to. Both are `nil` when librdkafka does not know them.
#
# Requires a running Kafka broker at localhost:9092.
#
# Exit codes:
# - 0: the delivery reports carried the right status, latency and broker
# - 1: a value was missing or wrong

require "rdkafka"
require "securerandom"

$stdout.sync = true

def fail!(message)
  warn "FAIL: #{message}"
  exit(1)
end

config = Rdkafka::Config.new("bootstrap.servers": "localhost:9092")
admin = config.admin
producer = config.producer
topic_name = "it-delivery-status-#{SecureRandom.hex(6)}"

begin
  admin.create_topic(topic_name, 1, 1).wait(max_wait_timeout_ms: 15_000)

  callback_reports = []
  producer.delivery_callback = ->(report) { callback_reports << report }

  report = producer.produce(topic: topic_name, payload: "payload", partition: 0).wait(max_wait_timeout_ms: 15_000)
  producer.flush

  leader = producer.metadata(topic_name).topics.first[:partitions].first[:leader]

  puts "Delivered: status=#{report.status} persisted=#{report.persisted?} " \
       "latency=#{report.latency}us broker_id=#{report.broker_id} (partition leader: #{leader})"

  fail!("expected a persisted message, got status #{report.status}") unless report.persisted?
  fail!("latency missing: #{report.latency.inspect}") unless report.latency.is_a?(Integer) && report.latency.positive?
  fail!("broker #{report.broker_id.inspect} is not the partition leader #{leader}") unless report.broker_id == leader

  callback_report = callback_reports.first

  fail!("delivery callback was not called") if callback_report.nil?

  unless [callback_report.status, callback_report.latency, callback_report.broker_id] ==
      [report.status, report.latency, report.broker_id]
    fail!("delivery callback report differs: #{callback_report.inspect}")
  end
ensure
  begin
    admin.delete_topic(topic_name).wait(max_wait_timeout_ms: 15_000)
  rescue Rdkafka::RdkafkaError
    nil
  end

  producer.close
  admin.close
end

# A message that cannot reach any broker is reported as not persisted, so a retry is safe
unreachable = Rdkafka::Config.new("bootstrap.servers": "127.0.0.1:9095", "message.timeout.ms": 500).producer

begin
  handle = unreachable.produce(topic: topic_name, payload: "payload")

  begin
    handle.wait(max_wait_timeout_ms: 15_000)
  rescue Rdkafka::RdkafkaError => e
    puts "Undeliverable: #{e.code}"
  end

  report = handle.create_result

  puts "Undeliverable: status=#{report.status} not_persisted=#{report.not_persisted?} broker_id=#{report.broker_id.inspect}"

  fail!("expected a not persisted message, got status #{report.status}") unless report.not_persisted?
  fail!("expected no broker, got #{report.broker_id}") unless report.broker_id.nil?
ensure
  unreachable.close
end

puts "PASS: delivery reports carried the right status, latency and broker"
exit(0)
