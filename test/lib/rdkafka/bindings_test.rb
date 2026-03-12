# frozen_string_literal: true

require "zlib"

describe Rdkafka::Bindings do
  before do
    Rdkafka::Config.oauthbearer_token_refresh_callback = nil
  end

  it "loads librdkafka" do
    assert_includes Rdkafka::Bindings.ffi_libraries.map(&:name).first, "librdkafka"
  end

  describe "glibc error handling" do
    it "provides a helpful error message for glibc compatibility issues" do
      glibc_error = LoadError.new("Could not open library 'librdkafka.so': /lib/x86_64-linux-gnu/libc.so.6: version `GLIBC_2.38' not found")
      error_message = glibc_error.message

      assert_match(/GLIBC_[\d.]+['"` ]?\s*not found/i, error_message)
      glibc_version = error_message[/GLIBC_([\d.]+)/, 1]

      assert_equal "2.38", glibc_version
    end

    it "detects various glibc error message formats" do
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

    it "handles edge cases where version extraction fails gracefully" do
      error_message = "GLIBC_ not found"
      glibc_version = error_message[/GLIBC_([\d.]+)/, 1] || "unknown"

      assert_equal "unknown", glibc_version
    end
  end

  describe ".lib_extension" do
    it "knows the lib extension for darwin" do
      original = RbConfig::CONFIG["host_os"]
      begin
        RbConfig::CONFIG["host_os"] = "darwin"

        assert_equal "dylib", Rdkafka::Bindings.lib_extension
      ensure
        RbConfig::CONFIG["host_os"] = original
      end
    end

    it "knows the lib extension for linux" do
      original = RbConfig::CONFIG["host_os"]
      begin
        RbConfig::CONFIG["host_os"] = "linux"

        assert_equal "so", Rdkafka::Bindings.lib_extension
      ensure
        RbConfig::CONFIG["host_os"] = original
      end
    end
  end

  it "successfully calls librdkafka" do
    Rdkafka::Bindings.rd_kafka_conf_new
  end

  describe "non-blocking poll bindings" do
    it "has rd_kafka_poll_nb attached" do
      assert_respond_to Rdkafka::Bindings, :rd_kafka_poll_nb
    end

    it "has rd_kafka_consumer_poll_nb attached" do
      assert_respond_to Rdkafka::Bindings, :rd_kafka_consumer_poll_nb
    end
  end

  describe "log callback" do
    it "logs fatal messages for level 0" do
      log_queue = Rdkafka::Config.log_queue
      received = nil
      log_queue.stub(:<<, ->(val) { received = val }) do
        Rdkafka::Bindings::LogCallback.call(nil, 0, nil, "log line")
      end

      assert_equal [Logger::FATAL, "rdkafka: log line"], received
    end

    it "logs fatal messages for level 1" do
      log_queue = Rdkafka::Config.log_queue
      received = nil
      log_queue.stub(:<<, ->(val) { received = val }) do
        Rdkafka::Bindings::LogCallback.call(nil, 1, nil, "log line")
      end

      assert_equal [Logger::FATAL, "rdkafka: log line"], received
    end

    it "logs fatal messages for level 2" do
      log_queue = Rdkafka::Config.log_queue
      received = nil
      log_queue.stub(:<<, ->(val) { received = val }) do
        Rdkafka::Bindings::LogCallback.call(nil, 2, nil, "log line")
      end

      assert_equal [Logger::FATAL, "rdkafka: log line"], received
    end

    it "logs error messages" do
      log_queue = Rdkafka::Config.log_queue
      received = nil
      log_queue.stub(:<<, ->(val) { received = val }) do
        Rdkafka::Bindings::LogCallback.call(nil, 3, nil, "log line")
      end

      assert_equal [Logger::ERROR, "rdkafka: log line"], received
    end

    it "logs warning messages" do
      log_queue = Rdkafka::Config.log_queue
      received = nil
      log_queue.stub(:<<, ->(val) { received = val }) do
        Rdkafka::Bindings::LogCallback.call(nil, 4, nil, "log line")
      end

      assert_equal [Logger::WARN, "rdkafka: log line"], received
    end

    it "logs info messages for level 5" do
      log_queue = Rdkafka::Config.log_queue
      received = nil
      log_queue.stub(:<<, ->(val) { received = val }) do
        Rdkafka::Bindings::LogCallback.call(nil, 5, nil, "log line")
      end

      assert_equal [Logger::INFO, "rdkafka: log line"], received
    end

    it "logs info messages for level 6" do
      log_queue = Rdkafka::Config.log_queue
      received = nil
      log_queue.stub(:<<, ->(val) { received = val }) do
        Rdkafka::Bindings::LogCallback.call(nil, 6, nil, "log line")
      end

      assert_equal [Logger::INFO, "rdkafka: log line"], received
    end

    it "logs debug messages" do
      log_queue = Rdkafka::Config.log_queue
      received = nil
      log_queue.stub(:<<, ->(val) { received = val }) do
        Rdkafka::Bindings::LogCallback.call(nil, 7, nil, "log line")
      end

      assert_equal [Logger::DEBUG, "rdkafka: log line"], received
    end

    it "logs unknown messages" do
      log_queue = Rdkafka::Config.log_queue
      received = nil
      log_queue.stub(:<<, ->(val) { received = val }) do
        Rdkafka::Bindings::LogCallback.call(nil, 100, nil, "log line")
      end

      assert_equal [Logger::UNKNOWN, "rdkafka: log line"], received
    end
  end

  describe "stats callback" do
    describe "without a stats callback" do
      it "does nothing" do
        Rdkafka::Bindings::StatsCallback.call(nil, "{}", 2, nil)
      end
    end

    describe "with a stats callback" do
      it "calls the stats callback with a stats hash" do
        Rdkafka::Config.statistics_callback = lambda do |stats|
          $received_stats = stats
        end
        Rdkafka::Bindings::StatsCallback.call(nil, '{"received":1}', 13, nil)

        assert_equal({ "received" => 1 }, $received_stats)
      end
    end
  end

  describe "error callback" do
    describe "without an error callback" do
      it "does nothing" do
        Rdkafka::Bindings::ErrorCallback.call(nil, 1, "error", nil)
      end
    end

    describe "with an error callback" do
      it "calls the error callback with an Rdkafka::Error" do
        Rdkafka::Config.error_callback = lambda do |error|
          $received_error = error
        end
        Rdkafka::Bindings::ErrorCallback.call(nil, 8, "Broker not available", nil)

        assert_equal :broker_not_available, $received_error.code
        assert_equal "Broker not available", $received_error.broker_message
      end

      it "sets nil instance_name for null pointer" do
        Rdkafka::Config.error_callback = lambda do |error|
          $received_error = error
        end
        Rdkafka::Bindings::ErrorCallback.call(nil, 8, "Broker not available", nil)

        assert_nil $received_error.instance_name
      end

      it "sets instance_name from real client" do
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
    end
  end

  describe "oauthbearer set token" do
    describe "with args" do
      it "sets token or captures failure" do
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
    end
  end

  describe "oauthbearer set token failure" do
    describe "without args" do
      it "fails" do
        assert_raises(ArgumentError) do
          Rdkafka::Bindings.rd_kafka_oauthbearer_set_token_failure
        end
      end
    end

    describe "with args" do
      it "succeeds" do
        errstr = "error"
        RdKafkaTestConsumer.with do |consumer_ptr|
          Rdkafka::Bindings.rd_kafka_oauthbearer_set_token_failure(consumer_ptr, errstr)
        end
      end
    end
  end

  describe "oauthbearer callback" do
    describe "without an oauthbearer callback" do
      it "does nothing" do
        Rdkafka::Bindings::OAuthbearerTokenRefreshCallback.call(nil, "", nil)
      end
    end

    describe "with an oauthbearer callback" do
      it "calls the oauth bearer callback and receives config and client name" do
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
  end
end
