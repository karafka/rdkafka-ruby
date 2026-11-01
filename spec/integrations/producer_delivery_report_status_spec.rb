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

  exit(1) unless report.persisted?
  exit(1) unless report.latency.is_a?(Integer) && report.latency.positive?
  exit(1) unless report.broker_id == leader

  callback_report = callback_reports.first

  exit(1) if callback_report.nil?
  exit(1) unless [callback_report.status, callback_report.latency, callback_report.broker_id] ==
    [report.status, report.latency, report.broker_id]
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

  # `raise_response_error: false` returns the report of a failed delivery instead of raising, so
  # its `error` and `status` can be inspected to decide on a retry
  report = handle.wait(max_wait_timeout_ms: 15_000, raise_response_error: false)

  exit(1) unless report.error.is_a?(Rdkafka::RdkafkaError)
  exit(1) unless report.not_persisted?
  exit(1) unless report.broker_id.nil?
ensure
  unreachable.close
end

exit(0)
