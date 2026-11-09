# frozen_string_literal: true

# This integration test verifies that `#oauthbearer_set_token` passes its extension count and
# error buffer size to librdkafka as full `size_t` values.
#
# Both arguments used to be bound as 32-bit `int`. The upper half of the 64-bit error buffer size,
# passed on the stack, then held garbage on MRI and JRuby, and on JRuby the error string could come
# back truncated.
#
# No Kafka broker is needed: the token is only stored on the client.
#
# Scenarios:
# 1. A client with SASL/OAUTHBEARER accepts the token, with and without extensions, every time
# 2. A client without SASL/OAUTHBEARER returns `__STATE` and its error string cut to the given
#    buffer size every time. A garbage upper half makes the size huge and the string complete.
# 3. A client with SASL/OAUTHBEARER rejects an extension count above 32 bits (`2**32 + 1`, odd)
#    with `invalid_arg` and reports the full count. A 32-bit binding passes `1` or raises.
#
# Exit codes:
# - 0: every call returned the expected result
# - 1: a call returned an unexpected result

require "rdkafka"

$stdout.sync = true

ITERATIONS = 2_000
ERROR_SIZE = 16
# librdkafka writes at most ERROR_SIZE - 1 characters and a NUL
EXPECTED_ERROR = "SASL/OAUTHBEARER is not the configured authentication mechanism"[0, ERROR_SIZE - 1]

producer = Rdkafka::Config.new(
  "bootstrap.servers": "127.0.0.1:9",
  "security.protocol": "sasl_ssl",
  "sasl.mechanisms": "OAUTHBEARER"
).producer

failures = []

begin
  ITERATIONS.times do |i|
    response = producer.oauthbearer_set_token(
      token: "token",
      lifetime_ms: (Time.now.to_i + 900) * 1000,
      principal_name: "kafka-cluster",
      extensions: i.even? ? nil : { "foo" => "bar" }
    )

    failures << "iteration #{i}: response #{response}" unless response.zero?
  end
ensure
  producer.close
end

native_client = Rdkafka::Bindings.rd_kafka_new(:rd_kafka_producer, nil, nil, 0)

begin
  ITERATIONS.times do |i|
    error_buffer = FFI::MemoryPointer.from_string(" " * 256)
    response = Rdkafka::Bindings.rd_kafka_oauthbearer_set_token(
      native_client, "token", (Time.now.to_i + 900) * 1000, "kafka-cluster", nil, 0, error_buffer, ERROR_SIZE
    )
    error = error_buffer.read_string

    next if response == Rdkafka::Bindings::RD_KAFKA_RESP_ERR__STATE && error == EXPECTED_ERROR

    failures << "iteration #{i}: response #{response}, error #{error.inspect}"
  end
ensure
  Rdkafka::Bindings.rd_kafka_destroy(native_client)
end

EXTENSION_SIZE = 2**32 + 1
EXPECTED_EXTENSION_ERROR = "Incorrect extension size (must be a non-negative multiple of 2): #{EXTENSION_SIZE}"

native_config = Rdkafka::Bindings.rd_kafka_conf_new
config_error = FFI::MemoryPointer.new(:char, 256)

{ "security.protocol" => "sasl_ssl", "sasl.mechanisms" => "OAUTHBEARER" }.each do |key, value|
  Rdkafka::Bindings.rd_kafka_conf_set(native_config, key, value, config_error, 256)
end

native_client = Rdkafka::Bindings.rd_kafka_new(:rd_kafka_producer, native_config, nil, 0)

begin
  ITERATIONS.times do |i|
    error_buffer = FFI::MemoryPointer.from_string(" " * 256)
    response = Rdkafka::Bindings.rd_kafka_oauthbearer_set_token(
      native_client, "token", (Time.now.to_i + 900) * 1000, "kafka-cluster", nil, EXTENSION_SIZE, error_buffer, 256
    )
    error = error_buffer.read_string

    next if Rdkafka::RdkafkaError.new(response).code == :invalid_arg && error == EXPECTED_EXTENSION_ERROR

    failures << "iteration #{i}: response #{response}, error #{error.inspect}"
  rescue RangeError => e
    failures << "iteration #{i}: #{e.class}: #{e.message}"
  end
ensure
  Rdkafka::Bindings.rd_kafka_destroy(native_client)
end

unless failures.empty?
  warn "FAIL: #{failures.size} of #{ITERATIONS * 3} calls failed, first: #{failures.first}"
  exit(1)
end

puts "PASS: #{ITERATIONS * 3} token set calls returned the expected result"
exit(0)
