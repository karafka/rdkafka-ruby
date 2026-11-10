# frozen_string_literal: true

# This integration test verifies that a failed consumer close is reported to
# `Rdkafka::Config.error_callback` also when another thread closes the consumer at the same time.
#
# The first closer runs `rd_kafka_consumer_close` under the access lock. If a late closer gets the
# lock right after it and destroys the native client before the first closer reports the failure,
# the report cannot read the client anymore. The error then went to `Rdkafka::Config.logger` as
# "Unhandled exception: Rdkafka::ClosedInnerError" and the error callback never got it.
#
# The native consumer is closed before `#close`, so the close fails with "Consumer already closed".
# The late closer runs when the first closer leaves the access lock, the moment the race needs.
#
# No broker is required: closing does not need a connection.
#
# Exit codes:
# - 0: the failed close was reported once to the error callback and nothing was logged
# - 1: the failed close was not reported, reported more than once, logged, or a `close` raised

require "rdkafka"
require "logger"
require "stringio"

close_errors = []
log = StringIO.new

Rdkafka::Config.error_callback = lambda do |error|
  close_errors << error if error.broker_message == "Consumer close failed"
end

Rdkafka::Config.logger = Logger.new(log)

# Runs a late close when the first closer leaves the access lock
module LateCloser
  class << self
    attr_accessor :consumer, :thread
  end

  def synchronize_unless_closed(&block)
    result = super

    if Thread.current == Thread.main && LateCloser.thread.nil?
      LateCloser.thread = Thread.new { LateCloser.consumer.close }
      LateCloser.thread.join(10)
    end

    result
  end
end

Rdkafka::NativeKafka.prepend(LateCloser)

consumer = Rdkafka::Config.new(
  "bootstrap.servers": "127.0.0.1:9999",
  "group.id": "consumer-concurrent-close-error"
).consumer

LateCloser.consumer = consumer

consumer.instance_variable_get(:@native_kafka).with_inner do |inner|
  Rdkafka::Bindings.rd_kafka_consumer_close(inner)
end

begin
  consumer.close
  LateCloser.thread.value
rescue
  exit(1)
end

exit(1) unless consumer.closed?
exit(1) unless close_errors.size == 1
exit(1) unless close_errors.first.code == :destroy
exit(1) unless log.string.empty?

exit(0)
