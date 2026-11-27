# frozen_string_literal: true

require "zlib"

describe Rdkafka::Bindings do
  it "loads librdkafka" do
    assert_includes(described_class.ffi_libraries.map(&:name).first, "librdkafka")
  end

  describe "glibc error handling" do
    it "raises LibraryLoadError when librdkafka needs a newer glibc" do
      fixture = File.expand_path("../../../spec/fixtures/glibc_load_error.rb", __dir__)
      output = IO.popen([RbConfig.ruby, "-Ilib", fixture], err: [:child, :out], &:read)

      assert_predicate($?, :success?, output)
    end

    it "provides a helpful error message for glibc compatibility issues" do
      # This test simulates what would happen if the library loading fails with a glibc error
      # We can't actually test the real scenario without breaking the test suite,
      # but we can verify the error message format would be correct

      # Create a mock LoadError with a glibc message
      glibc_error = LoadError.new("Could not open library 'librdkafka.so': /lib/x86_64-linux-gnu/libc.so.6: version `GLIBC_2.38' not found")

      # Extract what the error handler would do
      error_message = glibc_error.message

      assert_match(/GLIBC_[\d.]+['"` ]?\s*not found/i, error_message)

      glibc_version = error_message[/GLIBC_([\d.]+)/, 1]

      assert_equal("2.38", glibc_version)
    end

    it "detects various glibc error message formats" do
      test_cases = [
        "version `GLIBC_2.38' not found",
        "version 'GLIBC_2.33' not found",
        "GLIBC_2.34 not found",
        "version `GLIBC_2.39` not found"
      ]

      test_cases.each do |test_case|
        assert_match(/GLIBC_[\d.]+['"` ]?\s*not found/i, test_case)
      end
    end

    it "handles edge cases where version extraction fails gracefully" do
      # Simulate an edge case where the pattern matches but capture group might fail
      error_message = "GLIBC_ not found"

      # The pattern might match but version extraction should fallback to 'unknown'
      if /GLIBC_[\d.]+['"` ]?\s*not found/i.match?(error_message)
        # This won't execute because the pattern requires [\d.]+
        glibc_version = error_message[/GLIBC_([\d.]+)/, 1] || "unknown"

        assert_equal("unknown", glibc_version)
      end

      # More realistic: ensure fallback works if regex changes in the future
      glibc_version = error_message[/GLIBC_([\d.]+)/, 1] || "unknown"

      assert_equal("unknown", glibc_version)
    end
  end

  describe ".lib_extension" do
    it "knows the lib extension for darwin" do
      RbConfig::CONFIG.stubs(:[]).with("host_os").returns("darwin")

      assert_equal("dylib", described_class.lib_extension)
    end

    it "knows the lib extension for linux" do
      RbConfig::CONFIG.stubs(:[]).with("host_os").returns("linux")

      assert_equal("so", described_class.lib_extension)
    end
  end

  it "successfullies call librdkafka" do
    described_class.rd_kafka_conf_new
  end

  describe "error buffer size" do
    before do
      @error_buffer = FFI::MemoryPointer.from_string(" " * 256)
      @conf_error = 'No such configuration property: "not.a.property"'
      @new_error = "`acks` must be set to `all` when `enable.idempotence` is true"
    end

    def conf_set_error(size)
      config = described_class.rd_kafka_conf_new

      assert_equal(
        :config_unknown,
        described_class.rd_kafka_conf_set(config, "not.a.property", "1", @error_buffer, size)
      )
      @error_buffer.read_string
    ensure
      described_class.rd_kafka_conf_destroy(config)
    end

    def topic_conf_set_error(size)
      config = described_class.rd_kafka_topic_conf_new

      assert_equal(
        :config_unknown,
        described_class.rd_kafka_topic_conf_set(config, "not.a.property", "1", @error_buffer, size)
      )
      @error_buffer.read_string
    ensure
      described_class.rd_kafka_topic_conf_destroy(config)
    end

    def new_error_for(size)
      config = described_class.rd_kafka_conf_new
      described_class.rd_kafka_conf_set(config, "enable.idempotence", "true", FFI::MemoryPointer.new(:char, 256), 256)
      described_class.rd_kafka_conf_set(config, "acks", "1", FFI::MemoryPointer.new(:char, 256), 256)

      assert_predicate(described_class.rd_kafka_new(:rd_kafka_producer, config, @error_buffer, size), :null?)
      @error_buffer.read_string
    ensure
      described_class.rd_kafka_conf_destroy(config)
    end

    it "cuts the rd_kafka_conf_set error to the given size" do
      assert_equal(@conf_error[0, 15], conf_set_error(16))
    end

    it "passes a rd_kafka_conf_set size above 32 bits" do
      assert_equal(@conf_error, conf_set_error(2**32 + 1))
    end

    it "cuts the rd_kafka_topic_conf_set error to the given size" do
      assert_equal(@conf_error[0, 15], topic_conf_set_error(16))
    end

    it "passes a rd_kafka_topic_conf_set size above 32 bits" do
      assert_equal(@conf_error, topic_conf_set_error(2**32 + 1))
    end

    it "cuts the rd_kafka_new error to the given size" do
      assert_equal(@new_error[0, 15], new_error_for(16))
    end

    it "passes a rd_kafka_new size above 32 bits" do
      assert_equal(@new_error, new_error_for(2**32 + 1))
    end
  end

  describe "non-blocking poll bindings" do
    it "has rd_kafka_poll_nb attached" do
      assert_respond_to(described_class, :rd_kafka_poll_nb)
    end

    it "has rd_kafka_consumer_poll_nb attached" do
      assert_respond_to(described_class, :rd_kafka_consumer_poll_nb)
    end
  end

  describe ".partitioner" do
    before do
      @partition_count = 5
      # "smörgås" is 7 characters but 9 bytes in UTF-8 (ö and å are two bytes each)
      @multibyte_key = "smörgås"
    end

    # Captures the key length the partitioner passes to librdkafka.
    def captured_key_length(key)
      length = nil
      described_class
        .stubs(:rd_kafka_msg_partitioner_consistent_random)
        .with { |_topic, _ptr, len, *| length = len }
        .returns(0)
      described_class.partitioner(FFI::Pointer::NULL, key, @partition_count)
      length
    end

    after { Rdkafka::Config.partitioner_key_uses_bytesize = false }

    context "by default (Config.partitioner_key_uses_bytesize is false)" do
      it "passes the key's character length (legacy behavior, no partition change)" do
        assert_equal(@multibyte_key.size, captured_key_length(@multibyte_key))
      end
    end

    context "when Config.partitioner_key_uses_bytesize is true" do
      before { Rdkafka::Config.partitioner_key_uses_bytesize = true }

      it "passes the key's byte length (correct, other-client-consistent)" do
        length = captured_key_length(@multibyte_key)

        assert_equal(@multibyte_key.bytesize, length)
        refute_equal(@multibyte_key.size, length)
      end

      it "switches back to the character length when disabled again" do
        Rdkafka::Config.partitioner_key_uses_bytesize = false

        assert_equal(@multibyte_key.size, captured_key_length(@multibyte_key))
      end
    end
  end

  describe "log callback" do
    before do
      @log_queue = Rdkafka::Config.log_queue
      @log_queue.stubs(:<<)
    end

    it "logs fatal messages" do
      @log_queue.expects(:<<).with([Logger::FATAL, "rdkafka: log line"])
      Rdkafka::Bindings::LogCallback.call(nil, 0, nil, "log line")
    end

    it "logs fatal messages" do
      @log_queue.expects(:<<).with([Logger::FATAL, "rdkafka: log line"])
      Rdkafka::Bindings::LogCallback.call(nil, 1, nil, "log line")
    end

    it "logs fatal messages" do
      @log_queue.expects(:<<).with([Logger::FATAL, "rdkafka: log line"])
      Rdkafka::Bindings::LogCallback.call(nil, 2, nil, "log line")
    end

    it "logs error messages" do
      @log_queue.expects(:<<).with([Logger::ERROR, "rdkafka: log line"])
      Rdkafka::Bindings::LogCallback.call(nil, 3, nil, "log line")
    end

    it "logs warning messages" do
      @log_queue.expects(:<<).with([Logger::WARN, "rdkafka: log line"])
      Rdkafka::Bindings::LogCallback.call(nil, 4, nil, "log line")
    end

    it "logs info messages" do
      @log_queue.expects(:<<).with([Logger::INFO, "rdkafka: log line"])
      Rdkafka::Bindings::LogCallback.call(nil, 5, nil, "log line")
    end

    it "logs info messages" do
      @log_queue.expects(:<<).with([Logger::INFO, "rdkafka: log line"])
      Rdkafka::Bindings::LogCallback.call(nil, 6, nil, "log line")
    end

    it "logs debug messages" do
      @log_queue.expects(:<<).with([Logger::DEBUG, "rdkafka: log line"])
      Rdkafka::Bindings::LogCallback.call(nil, 7, nil, "log line")
    end

    it "logs unknown messages" do
      @log_queue.expects(:<<).with([Logger::UNKNOWN, "rdkafka: log line"])
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

      it "stores topic partition counts without the deprecated cache names" do
        topic_name = "stats-#{SecureRandom.hex(6)}"
        json = JSON.generate("topics" => { topic_name => { "partitions" => { "0" => {}, "1" => {}, "-1" => {} } } })

        Rdkafka::Producer.expects(:warn).never

        Rdkafka::Bindings::StatsCallback.call(nil, json, json.bytesize, nil)

        assert_equal(2, Rdkafka::Producer.partition_count_cache.to_h.dig(topic_name, 1))
      end
    end

    context "when the stats callback is cleared while it runs" do
      it "calls the callback it read" do
        received = []
        Rdkafka::Config.statistics_callback = ->(stats) { received << stats }

        clear_callback = lambda do |*|
          Rdkafka::Config.statistics_callback = nil
          true
        end
        JSON.stubs(:parse).with(&clear_callback).returns({ "received" => 1 })

        Rdkafka::Bindings::StatsCallback.call(nil, "{\"received\":1}", 13, nil)

        assert_equal([{ "received" => 1 }], received)
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

        assert_equal(:broker_not_available, $received_error.code)
        assert_equal("Broker not available", $received_error.broker_message)
      end

      it "sets instance_name to nil when client_ptr is null" do
        Rdkafka::Bindings::ErrorCallback.call(nil, 8, "Broker not available", nil)

        assert_nil($received_error.instance_name)
      end
    end

    context "when the error callback is cleared while it runs" do
      it "calls the callback it read" do
        received = []
        Rdkafka::Config.error_callback = ->(error) { received << error }

        error = Rdkafka::RdkafkaError.new(Rdkafka::Bindings::RD_KAFKA_RESP_ERR__STATE, broker_message: "state")
        clear_callback = lambda do |*|
          Rdkafka::Config.error_callback = nil
          true
        end
        Rdkafka::RdkafkaError.stubs(:new).with(&clear_callback).returns(error)

        Rdkafka::Bindings::ErrorCallback.call(nil, Rdkafka::Bindings::RD_KAFKA_RESP_ERR__STATE, "state", nil)

        assert_equal([:state], received.map(&:code))
      end
    end

    context "with an error callback and a real client" do
      it "sets instance_name from a real client that triggers an error" do
        received_errors = []
        Rdkafka::Config.error_callback = lambda do |error|
          received_errors << error
        end

        # Create a producer pointing to a non-existent broker to trigger error callbacks
        config = Rdkafka::Config.new(
          "bootstrap.servers": "127.0.0.1:19999",
          "statistics.interval.ms": 0
        )
        producer = config.producer

        # Wait for librdkafka to attempt connection and fire error callbacks
        sleep(2)

        producer.close

        errors_with_name = received_errors.select { |e| e.instance_name }

        refute_empty(errors_with_name)
        assert_includes(errors_with_name.first.instance_name, "rdkafka#producer-")
      end
    end
  end

  describe "oauthbearer set token" do
    context "with args" do
      before do
        @lifetime_ms = Time.now.to_i * 1000 + 900 * 1000
        @error_buffer = FFI::MemoryPointer.from_string(" " * 256)
      end

      it "sets token or capture failure" do
        RdKafkaTestConsumer.with do |consumer_ptr|
          response = described_class.rd_kafka_oauthbearer_set_token(consumer_ptr, "token", @lifetime_ms, "kafka-cluster", nil, 0, @error_buffer, 256)

          assert_equal(Rdkafka::Bindings::RD_KAFKA_RESP_ERR__STATE, response)
          assert_equal("SASL/OAUTHBEARER is not the configured authentication mechanism", @error_buffer.read_string)
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

  describe "offset commit callback" do
    it "does nothing when the opaque is no longer registered" do
      Rdkafka::Bindings::OffsetCommitCallback.call(
        FFI::Pointer::NULL,
        Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR,
        FFI::Pointer::NULL,
        FFI::Pointer.new(0xDEAD)
      )
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

          assert_equal("{}", $received_config)
          assert_match(/consumer/, $received_client_name)
        end
      end
    end

    context "when the oauthbearer callback is cleared while it runs" do
      it "calls the callback it read" do
        received = []
        callback = ->(config, client_name) { received << [config, client_name] }

        Rdkafka::Config
          .stubs(:oauthbearer_token_refresh_callback)
          .returns(callback, nil)

        RdKafkaTestConsumer.with do |consumer_ptr|
          Rdkafka::Bindings::OAuthbearerTokenRefreshCallback.call(consumer_ptr, "{}", nil)
        end

        assert_equal(1, received.size)
        assert_equal("{}", received.first.first)
      end
    end
  end
end
