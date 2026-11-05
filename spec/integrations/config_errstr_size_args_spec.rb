# frozen_string_literal: true

# This integration test verifies that `rd_kafka_conf_set`, `rd_kafka_topic_conf_set` and
# `rd_kafka_new` get their error buffer size as a full `size_t` value.
#
# The size used to be bound as a 32-bit `int`. The upper half of the 64-bit argument was then not
# set by the binding, and a size above 32 bits raised `RangeError` on MRI or was cut to its lower
# half on JRuby.
#
# No Kafka broker is needed: every call fails before the client connects.
#
# Scenarios:
# 1. An invalid option, an invalid topic option and an invalid option combination raise
#    `ConfigError`, `ConfigError` and `ClientCreationError` with the full librdkafka error
# 2. Each binding cuts the error string to a small given buffer size every time
# 3. Each binding accepts an error buffer size above 32 bits (`2**32 + 1`) and returns the full
#    error. A 32-bit binding raises `RangeError` or passes `1`, which gives an empty string.
#
# Exit codes:
# - 0: every call returned the expected result
# - 1: a call returned an unexpected result

require "rdkafka"

$stdout.sync = true

ITERATIONS = 1_000
ERROR_SIZE = 16
LARGE_ERROR_SIZE = 2**32 + 1
CONF_ERROR = 'No such configuration property: "not.a.property"'
NEW_ERROR = "`acks` must be set to `all` when `enable.idempotence` is true"

failures = []

calls = {
  "rd_kafka_conf_set" => lambda do |error_buffer, size|
    config = Rdkafka::Bindings.rd_kafka_conf_new
    result = Rdkafka::Bindings.rd_kafka_conf_set(config, "not.a.property", "1", error_buffer, size)
    Rdkafka::Bindings.rd_kafka_conf_destroy(config)
    raise "unexpected result #{result}" unless result == :config_unknown

    error_buffer.read_string
  end,
  "rd_kafka_topic_conf_set" => lambda do |error_buffer, size|
    config = Rdkafka::Bindings.rd_kafka_topic_conf_new
    result = Rdkafka::Bindings.rd_kafka_topic_conf_set(config, "not.a.property", "1", error_buffer, size)
    Rdkafka::Bindings.rd_kafka_topic_conf_destroy(config)
    raise "unexpected result #{result}" unless result == :config_unknown

    error_buffer.read_string
  end,
  "rd_kafka_new" => lambda do |error_buffer, size|
    config = Rdkafka::Bindings.rd_kafka_conf_new
    native_error = FFI::MemoryPointer.new(:char, 256)
    Rdkafka::Bindings.rd_kafka_conf_set(config, "enable.idempotence", "true", native_error, 256)
    Rdkafka::Bindings.rd_kafka_conf_set(config, "acks", "1", native_error, 256)
    handle = Rdkafka::Bindings.rd_kafka_new(:rd_kafka_producer, config, error_buffer, size)

    unless handle.null?
      Rdkafka::Bindings.rd_kafka_destroy(handle)
      raise "unexpected client"
    end

    # On failure the conf stays owned by the caller
    Rdkafka::Bindings.rd_kafka_conf_destroy(config)
    error_buffer.read_string
  end
}

expected = {
  "rd_kafka_conf_set" => CONF_ERROR,
  "rd_kafka_topic_conf_set" => CONF_ERROR,
  "rd_kafka_new" => NEW_ERROR
}

public_api = {
  "Config#producer with an invalid option" => [
    Rdkafka::Config::ConfigError,
    CONF_ERROR,
    -> { Rdkafka::Config.new("not.a.property": "1").producer }
  ],
  "Config#producer with an invalid option combination" => [
    Rdkafka::Config::ClientCreationError,
    NEW_ERROR,
    -> { Rdkafka::Config.new("enable.idempotence": true, acks: 1).producer }
  ]
}

public_api.each do |name, (error_class, message, call)|
  call.call
  failures << "#{name}: no error"
rescue error_class => e
  failures << "#{name}: #{e.message.inspect}" unless e.message == message
end

producer = Rdkafka::Config.new("bootstrap.servers": "127.0.0.1:9").producer

begin
  producer.produce(topic: "errstr-size", payload: "1", topic_config: { "not.a.property": "1" })
  failures << "Producer#produce with an invalid topic option: no error"
rescue Rdkafka::Config::ConfigError => e
  failures << "Producer#produce with an invalid topic option: #{e.message.inspect}" unless e.message == CONF_ERROR
ensure
  producer.close
end

calls.each do |name, call|
  ITERATIONS.times do |i|
    error = call.call(FFI::MemoryPointer.from_string(" " * 256), ERROR_SIZE)

    # librdkafka writes at most ERROR_SIZE - 1 characters and a NUL
    next if error == expected[name][0, ERROR_SIZE - 1]

    failures << "#{name} size #{ERROR_SIZE}, iteration #{i}: #{error.inspect}"
  end

  ITERATIONS.times do |i|
    error = call.call(FFI::MemoryPointer.from_string(" " * 256), LARGE_ERROR_SIZE)

    next if error == expected[name]

    failures << "#{name} size #{LARGE_ERROR_SIZE}, iteration #{i}: #{error.inspect}"
  rescue RangeError => e
    failures << "#{name} size #{LARGE_ERROR_SIZE}, iteration #{i}: #{e.class}: #{e.message}"
  end
end

total = public_api.size + 1 + calls.size * ITERATIONS * 2

unless failures.empty?
  warn "FAIL: #{failures.size} of #{total} calls failed, first: #{failures.first}"
  exit(1)
end

puts "PASS: #{total} calls returned the expected error"
exit(0)
