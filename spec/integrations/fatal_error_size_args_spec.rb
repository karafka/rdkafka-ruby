# frozen_string_literal: true

# This integration test verifies that `rd_kafka_fatal_error` gets its error buffer size as a full
# `size_t` value, so `Producer#fatal_error` reads the fatal error string as librdkafka wrote it.
#
# The size used to be bound as a 32-bit `int`, so a size above 32 bits raised `RangeError` on MRI
# and was cut to its low 32 bits on JRuby.
#
# No Kafka broker is needed: the fatal error is raised with `rd_kafka_test_fatal_error`.
#
# Scenarios:
# 1. `Producer#fatal_error` returns the code and the full string of a test fatal error every time
# 2. `rd_kafka_fatal_error` cuts the string to the given buffer size every time
# 3. `rd_kafka_fatal_error` accepts a size above 32 bits (`2**32 + 1`, odd) and returns the full
#    string. A 32-bit binding passes `1` (an empty string) or raises.
#
# Exit codes:
# - 0: every call returned the expected result
# - 1: a call returned an unexpected result

require "rdkafka"
require "rdkafka/producer/testing"

Rdkafka::Producer.include(Rdkafka::Testing)

$stdout.sync = true

ITERATIONS = 2_000
ERROR_CODE = Rdkafka::Bindings::RD_KAFKA_RESP_ERR_INVALID_PRODUCER_EPOCH
EXPECTED_ERROR = "test_fatal_error: Test producer fencing"
ERROR_SIZE = 16
LARGE_ERROR_SIZE = 2**32 + 1

failures = []

producer = Rdkafka::Config.new("bootstrap.servers": "127.0.0.1:9").producer

begin
  producer.trigger_test_fatal_error(ERROR_CODE, "Test producer fencing")

  ITERATIONS.times do |i|
    details = producer.fatal_error

    next if details == { error_code: ERROR_CODE, error_string: EXPECTED_ERROR }

    failures << "iteration #{i}: #{details.inspect}"
  end
ensure
  producer.close
end

native_client = Rdkafka::Bindings.rd_kafka_new(:rd_kafka_producer, nil, nil, 0)

begin
  Rdkafka::Bindings.rd_kafka_test_fatal_error(native_client, ERROR_CODE, "Test producer fencing")

  # librdkafka writes at most ERROR_SIZE - 1 characters and a NUL
  { ERROR_SIZE => EXPECTED_ERROR[0, ERROR_SIZE - 1], LARGE_ERROR_SIZE => EXPECTED_ERROR }.each do |size, expected|
    ITERATIONS.times do |i|
      error_buffer = FFI::MemoryPointer.from_string(" " * 256)
      response = Rdkafka::Bindings.rd_kafka_fatal_error(native_client, error_buffer, size)
      error = error_buffer.read_string

      next if response == ERROR_CODE && error == expected

      failures << "size #{size}, iteration #{i}: response #{response}, error #{error.inspect}"
    rescue RangeError => e
      failures << "size #{size}, iteration #{i}: #{e.class}: #{e.message}"
    end
  end
ensure
  Rdkafka::Bindings.rd_kafka_destroy(native_client)
end

unless failures.empty?
  warn "FAIL: #{failures.size} of #{ITERATIONS * 3} calls failed, first: #{failures.first}"
  exit(1)
end

puts "PASS: #{ITERATIONS * 3} fatal error reads returned the expected result"
exit(0)
