# frozen_string_literal: true

require_relative "../../test_helper"
require "zlib"

describe Rdkafka::Bindings do
  it "loads librdkafka" do
    assert_includes described_class.ffi_libraries.map(&:name).first, "librdkafka"
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

      if /GLIBC_[\d.]+['"` ]?\s*not found/i.match?(error_message)
        glibc_version = error_message[/GLIBC_([\d.]+)/, 1] || "unknown"
        assert_equal "unknown", glibc_version
      end

      glibc_version = error_message[/GLIBC_([\d.]+)/, 1] || "unknown"
      assert_equal "unknown", glibc_version
    end
  end

  describe ".lib_extension" do
    it "knows the lib extension for darwin" do
      with_stubbed_const(RbConfig, :CONFIG, "host_os" => "darwin") do
        assert_equal "dylib", described_class.lib_extension
      end
    end

    it "knows the lib extension for linux" do
      with_stubbed_const(RbConfig, :CONFIG, "host_os" => "linux") do
        assert_equal "so", described_class.lib_extension
      end
    end
  end

  it "successfullies call librdkafka" do
    described_class.rd_kafka_conf_new
  end

  describe "non-blocking poll bindings" do
    it "has rd_kafka_poll_nb attached" do
      assert_respond_to described_class, :rd_kafka_poll_nb
    end

    it "has rd_kafka_consumer_poll_nb attached" do
      assert_respond_to described_class, :rd_kafka_consumer_poll_nb
    end
  end

  describe "log callback" do
    it "logs fatal messages at level 0" do
      log_queue = Rdkafka::Config.log_queue
      log_queue.expects(:<<).with([Logger::FATAL, "rdkafka: log line"])
      Rdkafka::Bindings::LogCallback.call(nil, 0, nil, "log line")
    end

    it "logs fatal messages at level 1" do
      log_queue = Rdkafka::Config.log_queue
      log_queue.expects(:<<).with([Logger::FATAL, "rdkafka: log line"])
      Rdkafka::Bindings::LogCallback.call(nil, 1, nil, "log line")
    end

    it "logs fatal messages at level 2" do
      log_queue = Rdkafka::Config.log_queue
      log_queue.expects(:<<).with([Logger::FATAL, "rdkafka: log line"])
      Rdkafka::Bindings::LogCallback.call(nil, 2, nil, "log line")
    end

    it "logs error messages" do
      log_queue = Rdkafka::Config.log_queue
      log_queue.expects(:<<).with([Logger::ERROR, "rdkafka: log line"])
      Rdkafka::Bindings::LogCallback.call(nil, 3, nil, "log line")
    end

    it "logs warning messages" do
      log_queue = Rdkafka::Config.log_queue
      log_queue.expects(:<<).with([Logger::WARN, "rdkafka: log line"])
      Rdkafka::Bindings::LogCallback.call(nil, 4, nil, "log line")
    end

    it "logs info messages at level 5" do
      log_queue = Rdkafka::Config.log_queue
      log_queue.expects(:<<).with([Logger::INFO, "rdkafka: log line"])
      Rdkafka::Bindings::LogCallback.call(nil, 5, nil, "log line")
    end

    it "logs info messages at level 6" do
      log_queue = Rdkafka::Config.log_queue
      log_queue.expects(:<<).with([Logger::INFO, "rdkafka: log line"])
      Rdkafka::Bindings::LogCallback.call(nil, 6, nil, "log line")
    end

    it "logs debug messages" do
      log_queue = Rdkafka::Config.log_queue
      log_queue.expects(:<<).with([Logger::DEBUG, "rdkafka: log line"])
      Rdkafka::Bindings::LogCallback.call(nil, 7, nil, "log line")
    end

    it "logs unknown messages" do
      log_queue = Rdkafka::Config.log_queue
      log_queue.expects(:<<).with([Logger::UNKNOWN, "rdkafka: log line"])
      Rdkafka::Bindings::LogCallback.call(nil, 100, nil, "log line")
    end
  end

  describe "stats callback" do
    context "without a stats callback" do
      it "does nothing" do
        Rdkafka::Bindings::StatsCallback.call(nil, "{}", 2, nil)
      end
    end

    context "with a stats callback" do
      before do
        Rdkafka::Config.statistics_callback = lambda do |stats|
          $received_stats = stats
        end
      end

      it "calls the stats callback with a stats hash" do
        Rdkafka::Bindings::StatsCallback.call(nil, "{\"received\":1}", 13, nil)
        assert_equal({ "received" => 1 }, $received_stats)
      end
    end
  end

  describe "error callback" do
    context "without an error callback" do
      it "does nothing" do
        Rdkafka::Bindings::ErrorCallback.call(nil, 1, "error", nil)
      end
    end

    context "with an error callback" do
      before do
        Rdkafka::Config.error_callback = lambda do |error|
          $received_error = error
        end
      end

      it "calls the error callback with an Rdkafka::Error" do
        Rdkafka::Bindings::ErrorCallback.call(nil, 8, "Broker not available", nil)
        assert_equal :broker_not_available, $received_error.code
        assert_equal "Broker not available", $received_error.broker_message
      end

      it "sets instance_name to nil when client_ptr is null" do
        Rdkafka::Bindings::ErrorCallback.call(nil, 8, "Broker not available", nil)
        assert_nil $received_error.instance_name
      end
    end

    context "with an error callback and a real client" do
      it "sets instance_name from a real client that triggers an error" do
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
    context "with args" do
      before do
        @default_token_expiry_seconds = 900
        @token_value = "token"
        @md_lifetime_ms = Time.now.to_i * 1000 + @default_token_expiry_seconds * 1000
        @md_principal_name = "kafka-cluster"
        @extensions = nil
        @extension_size = 0
        @error_buffer = FFI::MemoryPointer.from_string(" " * 256)
      end

      it "sets token or capture failure" do
        RdKafkaTestConsumer.with do |consumer_ptr|
          response = described_class.rd_kafka_oauthbearer_set_token(consumer_ptr, @token_value, @md_lifetime_ms, @md_principal_name, @extensions, @extension_size, @error_buffer, 256)
          assert_equal Rdkafka::Bindings::RD_KAFKA_RESP_ERR__STATE, response
          assert_equal "SASL/OAUTHBEARER is not the configured authentication mechanism", @error_buffer.read_string
        end
      end
    end
  end

  describe "oauthbearer set token failure" do
    context "without args" do
      it "fails" do
        assert_raises(ArgumentError) do
          described_class.rd_kafka_oauthbearer_set_token_failure
        end
      end
    end

    context "with args" do
      it "succeeds" do
        errstr = "error"
        RdKafkaTestConsumer.with do |consumer_ptr|
          described_class.rd_kafka_oauthbearer_set_token_failure(consumer_ptr, errstr)
        end
      end
    end
  end

  describe "oauthbearer callback" do
    context "without an oauthbearer callback" do
      it "does nothing" do
        Rdkafka::Bindings::OAuthbearerTokenRefreshCallback.call(nil, "", nil)
      end
    end

    context "with an oauthbearer callback" do
      before do
        Rdkafka::Config.oauthbearer_token_refresh_callback = lambda do |config, client_name|
          $received_config = config
          $received_client_name = client_name
        end
      end

      it "calls the oauth bearer callback and receive config and client name" do
        RdKafkaTestConsumer.with do |consumer_ptr|
          Rdkafka::Bindings::OAuthbearerTokenRefreshCallback.call(consumer_ptr, "{}", nil)
          assert_equal "{}", $received_config
          assert_match(/consumer/, $received_client_name)
        end
      end
    end
  end
end
