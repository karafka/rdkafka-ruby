# frozen_string_literal: true

require "test_helper"
require "zlib"

class BindingsTest < Minitest::Test
  def test_loads_librdkafka
    assert_includes Rdkafka::Bindings.ffi_libraries.map(&:name).first, "librdkafka"
  end

  def test_glibc_error_message_format
    glibc_error = LoadError.new("Could not open library 'librdkafka.so': /lib/x86_64-linux-gnu/libc.so.6: version `GLIBC_2.38' not found")
    error_message = glibc_error.message

    assert_match(/GLIBC_[\d.]+['"` ]?\s*not found/i, error_message)
    glibc_version = error_message[/GLIBC_([\d.]+)/, 1]

    assert_equal "2.38", glibc_version
  end

  def test_detects_various_glibc_error_formats
    test_cases = [
      "version `GLIBC_2.38' not found",
      "version 'GLIBC_2.33' not found",
      "GLIBC_2.34 not found",
      "version `GLIBC_2.39` not found"
    ]

    test_cases.each do |tc|
      assert_match(/GLIBC_[\d.]+['"` ]?\s*not found/i, tc)
    end
  end

  def test_handles_edge_cases_for_glibc_version_extraction
    error_message = "GLIBC_ not found"
    glibc_version = error_message[/GLIBC_([\d.]+)/, 1] || "unknown"

    assert_equal "unknown", glibc_version
  end

  def test_lib_extension_darwin
    original = RbConfig::CONFIG["host_os"]
    begin
      RbConfig::CONFIG["host_os"] = "darwin"

      assert_equal "dylib", Rdkafka::Bindings.lib_extension
    ensure
      RbConfig::CONFIG["host_os"] = original
    end
  end

  def test_lib_extension_linux
    original = RbConfig::CONFIG["host_os"]
    begin
      RbConfig::CONFIG["host_os"] = "linux"

      assert_equal "so", Rdkafka::Bindings.lib_extension
    ensure
      RbConfig::CONFIG["host_os"] = original
    end
  end

  def test_successfully_calls_librdkafka
    Rdkafka::Bindings.rd_kafka_conf_new
  end

  def test_has_rd_kafka_poll_nb
    assert_respond_to Rdkafka::Bindings, :rd_kafka_poll_nb
  end

  def test_has_rd_kafka_consumer_poll_nb
    assert_respond_to Rdkafka::Bindings, :rd_kafka_consumer_poll_nb
  end

  def test_log_callback_fatal_level_0
    log_queue = Rdkafka::Config.log_queue
    received = nil
    log_queue.stub(:<<, ->(val) { received = val }) do
      Rdkafka::Bindings::LogCallback.call(nil, 0, nil, "log line")
    end

    assert_equal [Logger::FATAL, "rdkafka: log line"], received
  end

  def test_log_callback_fatal_level_1
    log_queue = Rdkafka::Config.log_queue
    received = nil
    log_queue.stub(:<<, ->(val) { received = val }) do
      Rdkafka::Bindings::LogCallback.call(nil, 1, nil, "log line")
    end

    assert_equal [Logger::FATAL, "rdkafka: log line"], received
  end

  def test_log_callback_fatal_level_2
    log_queue = Rdkafka::Config.log_queue
    received = nil
    log_queue.stub(:<<, ->(val) { received = val }) do
      Rdkafka::Bindings::LogCallback.call(nil, 2, nil, "log line")
    end

    assert_equal [Logger::FATAL, "rdkafka: log line"], received
  end

  def test_log_callback_error
    log_queue = Rdkafka::Config.log_queue
    received = nil
    log_queue.stub(:<<, ->(val) { received = val }) do
      Rdkafka::Bindings::LogCallback.call(nil, 3, nil, "log line")
    end

    assert_equal [Logger::ERROR, "rdkafka: log line"], received
  end

  def test_log_callback_warn
    log_queue = Rdkafka::Config.log_queue
    received = nil
    log_queue.stub(:<<, ->(val) { received = val }) do
      Rdkafka::Bindings::LogCallback.call(nil, 4, nil, "log line")
    end

    assert_equal [Logger::WARN, "rdkafka: log line"], received
  end

  def test_log_callback_info_level_5
    log_queue = Rdkafka::Config.log_queue
    received = nil
    log_queue.stub(:<<, ->(val) { received = val }) do
      Rdkafka::Bindings::LogCallback.call(nil, 5, nil, "log line")
    end

    assert_equal [Logger::INFO, "rdkafka: log line"], received
  end

  def test_log_callback_info_level_6
    log_queue = Rdkafka::Config.log_queue
    received = nil
    log_queue.stub(:<<, ->(val) { received = val }) do
      Rdkafka::Bindings::LogCallback.call(nil, 6, nil, "log line")
    end

    assert_equal [Logger::INFO, "rdkafka: log line"], received
  end

  def test_log_callback_debug
    log_queue = Rdkafka::Config.log_queue
    received = nil
    log_queue.stub(:<<, ->(val) { received = val }) do
      Rdkafka::Bindings::LogCallback.call(nil, 7, nil, "log line")
    end

    assert_equal [Logger::DEBUG, "rdkafka: log line"], received
  end

  def test_log_callback_unknown
    log_queue = Rdkafka::Config.log_queue
    received = nil
    log_queue.stub(:<<, ->(val) { received = val }) do
      Rdkafka::Bindings::LogCallback.call(nil, 100, nil, "log line")
    end

    assert_equal [Logger::UNKNOWN, "rdkafka: log line"], received
  end

  def test_stats_callback_without_callback
    Rdkafka::Bindings::StatsCallback.call(nil, "{}", 2, nil)
  end

  def test_stats_callback_with_callback
    Rdkafka::Config.statistics_callback = lambda do |stats|
      $received_stats = stats
    end
    Rdkafka::Bindings::StatsCallback.call(nil, '{"received":1}', 13, nil)

    assert_equal({ "received" => 1 }, $received_stats)
  end

  def test_error_callback_without_callback
    Rdkafka::Bindings::ErrorCallback.call(nil, 1, "error", nil)
  end

  def test_error_callback_with_callback
    Rdkafka::Config.error_callback = lambda do |error|
      $received_error = error
    end
    Rdkafka::Bindings::ErrorCallback.call(nil, 8, "Broker not available", nil)

    assert_equal :broker_not_available, $received_error.code
    assert_equal "Broker not available", $received_error.broker_message
  end

  def test_error_callback_sets_nil_instance_name_for_null_ptr
    Rdkafka::Config.error_callback = lambda do |error|
      $received_error = error
    end
    Rdkafka::Bindings::ErrorCallback.call(nil, 8, "Broker not available", nil)

    assert_nil $received_error.instance_name
  end

  def test_error_callback_sets_instance_name_from_real_client
    received_errors = []
    Rdkafka::Config.error_callback = lambda do |error|
      received_errors << error
    end

    config = Rdkafka::Config.new(
      "bootstrap.servers": "127.0.0.1:19999",
      "statistics.interval.ms": 0
    )
    producer = config.producer
    sleep(2)
    producer.close

    errors_with_name = received_errors.select { |e| e.instance_name }

    refute_empty errors_with_name
    assert_includes errors_with_name.first.instance_name, "rdkafka#producer-"
  end

  def test_oauthbearer_set_token_with_args
    default_token_expiry_seconds = 900
    token_value = "token"
    md_lifetime_ms = Time.now.to_i * 1000 + default_token_expiry_seconds * 1000
    md_principal_name = "kafka-cluster"
    extensions = nil
    extension_size = 0
    error_buffer = FFI::MemoryPointer.from_string(" " * 256)

    RdKafkaTestConsumer.with do |consumer_ptr|
      response = Rdkafka::Bindings.rd_kafka_oauthbearer_set_token(consumer_ptr, token_value, md_lifetime_ms, md_principal_name, extensions, extension_size, error_buffer, 256)

      assert_equal Rdkafka::Bindings::RD_KAFKA_RESP_ERR__STATE, response
      assert_equal "SASL/OAUTHBEARER is not the configured authentication mechanism", error_buffer.read_string
    end
  end

  def test_oauthbearer_set_token_failure_without_args
    assert_raises(ArgumentError) do
      Rdkafka::Bindings.rd_kafka_oauthbearer_set_token_failure
    end
  end

  def test_oauthbearer_set_token_failure_with_args
    errstr = "error"
    RdKafkaTestConsumer.with do |consumer_ptr|
      Rdkafka::Bindings.rd_kafka_oauthbearer_set_token_failure(consumer_ptr, errstr)
    end
  end

  def test_oauthbearer_callback_without_callback
    Rdkafka::Bindings::OAuthbearerTokenRefreshCallback.call(nil, "", nil)
  end

  def test_oauthbearer_callback_with_callback
    Rdkafka::Config.oauthbearer_token_refresh_callback = lambda do |config, client_name|
      $received_config = config
      $received_client_name = client_name
    end

    RdKafkaTestConsumer.with do |consumer_ptr|
      Rdkafka::Bindings::OAuthbearerTokenRefreshCallback.call(consumer_ptr, "{}", nil)

      assert_equal "{}", $received_config
      assert_match(/consumer/, $received_client_name)
    end
  end
end
