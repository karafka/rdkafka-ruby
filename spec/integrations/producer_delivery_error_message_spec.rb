# frozen_string_literal: true

# This integration test verifies that a failed delivery carries the error text that librdkafka
# got for the message, not only the error code.
#
# A compacted topic rejects a record without a key with `invalid_record`, and the broker names
# the cause in the Produce response (KIP-467). The text must reach:
#   - `DeliveryReport#broker_message` of the delivery callback report
#   - `DeliveryReport#broker_message` and `#error.broker_message` from
#     `wait(raise_response_error: false)`
#   - `RdkafkaError#broker_message` raised by `DeliveryHandle#wait`
# A delivered message has no text.
#
# Requires a running Kafka broker at localhost:9092.
#
# Exit codes:
# - 0: the error text reached the delivery report and the raised error
# - 1: the text was missing or wrong

require "rdkafka"
require "securerandom"
require_relative "support/wait_for_topic"

BOOTSTRAP = "localhost:9092"
TOPIC = "it-delivery-error-message-#{SecureRandom.hex(6)}"

admin = Rdkafka::Config.new("bootstrap.servers": BOOTSTRAP).admin
admin.create_topic(TOPIC, 1, 1, "cleanup.policy" => "compact").wait(max_wait_timeout_ms: 15_000)
WaitForTopic.wait_for_topic(admin, TOPIC)

producer = Rdkafka::Config.new("bootstrap.servers": BOOTSTRAP).producer
callback_reports = []
producer.delivery_callback = ->(report) { callback_reports << report }

begin
  delivered = producer.produce(topic: TOPIC, payload: "payload", key: "key").wait(max_wait_timeout_ms: 15_000)

  exit(1) unless delivered.error.nil? && delivered.broker_message.nil?

  report = producer
    .produce(topic: TOPIC, payload: "payload")
    .wait(max_wait_timeout_ms: 15_000, raise_response_error: false)

  exit(1) unless report.error.code == :invalid_record
  exit(1) unless report.broker_message.is_a?(String) && report.broker_message.match?(/key/i)
  exit(1) unless report.error.broker_message == report.broker_message

  raised = begin
    producer.produce(topic: TOPIC, payload: "payload").wait(max_wait_timeout_ms: 15_000)
    nil
  rescue Rdkafka::RdkafkaError => e
    e
  end

  exit(1) unless raised&.code == :invalid_record
  exit(1) unless raised.broker_message == report.broker_message

  producer.flush

  failed_callback_reports = callback_reports.reject { |r| r.error.zero? }

  exit(1) unless failed_callback_reports.size == 2
  exit(1) unless failed_callback_reports.all? { |r| r.broker_message == report.broker_message }
  exit(1) unless callback_reports.find { |r| r.error.zero? }&.broker_message.nil?
ensure
  producer.close

  begin
    admin.delete_topic(TOPIC).wait(max_wait_timeout_ms: 15_000)
  rescue Rdkafka::RdkafkaError
    nil
  end

  admin.close
end
