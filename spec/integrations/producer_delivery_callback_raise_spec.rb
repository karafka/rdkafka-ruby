# frozen_string_literal: true

# This integration test verifies that an exception raised inside a user-provided
# `delivery_callback` cannot take down the producer process or strand the delivery handle.
#
# The delivery callback is invoked on librdkafka's background polling thread, which runs with
# `abort_on_exception = true`. Before the guard was added, a raising callback:
#   - unwound out of the FFI callback and crashed the whole process, and
#   - skipped the handle unlock, so `wait` blocked until its own timeout and then raised
#     `WaitTimeoutError` for a message that had in fact been delivered.
# The callback invocation is now wrapped so the exception is logged and swallowed (matching the
# rebalance callback) and the handle is always unlocked in an `ensure`.
#
# Because each integration spec runs in its own process, a regression here surfaces as a crashed
# (non-zero / signaled) process exit rather than taking the rest of the suite down with it.
#
# Requires a running Kafka broker at localhost:9092.
#
# Exit codes:
# - 0: the producer survived the raising callback and both deliveries resolved
# - 1: a delivery handle never resolved, or some other failure

require "rdkafka"
require "securerandom"

$stdout.sync = true

BOOTSTRAP = "localhost:9092"
TOPIC = "delivery-callback-raise-#{SecureRandom.hex(6)}"

admin = Rdkafka::Config.new("bootstrap.servers": BOOTSTRAP).admin
admin.create_topic(TOPIC, 1, 1).wait(max_wait_timeout_ms: 15_000)

# Topic creation ack does not guarantee the topic is visible to a produce yet
10.times do
  admin.metadata(TOPIC)
  break
rescue Rdkafka::RdkafkaError
  sleep 0.5
end
admin.close

producer = Rdkafka::Config.new("bootstrap.servers": BOOTSTRAP).producer
# Every delivery callback invocation raises. If the guard is missing, this both crashes the
# polling thread (process abort) and leaves the handle locked.
producer.delivery_callback = ->(_report) { raise "boom in delivery callback" }

begin
  first = producer
    .produce(topic: TOPIC, payload: "payload", key: "key")
    .wait(max_wait_timeout_ms: 15_000)

  # The polling thread must still be alive for this second delivery to resolve.
  second = producer
    .produce(topic: TOPIC, payload: "payload2", key: "key2")
    .wait(max_wait_timeout_ms: 15_000)
rescue Rdkafka::AbstractHandle::WaitTimeoutError => e
  warn "FAIL: delivery handle never resolved (#{e.class}); the raising callback skipped unlock"
  exit(1)
ensure
  producer.close
end

unless first.partition.is_a?(Integer) && second.offset >= 0
  warn "FAIL: unexpected delivery reports: #{first.inspect} / #{second.inspect}"
  exit(1)
end

puts "PASS: producer survived a raising delivery_callback and both deliveries resolved"
exit(0)
