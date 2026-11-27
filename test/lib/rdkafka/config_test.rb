# frozen_string_literal: true

describe Rdkafka::Config do
  context "logger" do
    it "has a default logger" do
      assert_kind_of(Logger, described_class.logger)
    end

    it "sets the logger" do
      logger = Logger.new($stdout)

      refute_equal(logger, described_class.logger)
      described_class.logger = logger

      assert_equal(logger, described_class.logger)
    end

    it "does not accept a nil logger" do
      assert_raises(Rdkafka::Config::NoLoggerError) do
        described_class.logger = nil
      end
    end

    it "supports logging queue" do
      log = StringIO.new
      described_class.logger = Logger.new(log)
      described_class.ensure_log_thread

      described_class.log_queue << [Logger::FATAL, "I love testing"]
      20.times do
        break if log.string != ""

        sleep 0.05
      end

      assert_includes(log.string, "FATAL -- : I love testing")
    end

    unless RUBY_PLATFORM == "java"
      it "expect to start new logger thread after fork and work" do
        reader, writer = IO.pipe

        pid = fork do
          $stdout.reopen(writer)
          described_class.logger = Logger.new($stdout)
          reader.close
          producer = rdkafka_producer_config(debug: "all").producer
          producer.close
          writer.close
          sleep(1)
        end

        writer.close
        Process.wait(pid)
        output = reader.read

        assert_operator(output.split("\n").size, :>=, 20)
      end
    end
  end

  context "partition count cache ttl" do
    before do
      @cache = Rdkafka::Producer::PartitionCountCache.new
      @original_cache = Rdkafka::Producer.partition_count_cache
      Rdkafka::Producer.partition_count_cache = @cache
    end

    after { Rdkafka::Producer.partition_count_cache = @original_cache }

    it "defaults to the cache default" do
      assert_equal(Rdkafka::Defaults::PARTITION_COUNT_CACHE_TTL_MS, described_class.partition_count_cache_ttl_ms)
    end

    it "sets the ttl of the process-wide cache and keeps cached counts" do
      @cache.set("topic", 5)
      described_class.partition_count_cache_ttl_ms = 60_000

      assert_equal(60_000, @cache.ttl_ms)
      assert_equal(60_000, described_class.partition_count_cache_ttl_ms)
      assert_equal(5, @cache.to_h["topic"][1])
    end

    it "rejects a non positive ttl" do
      assert_raises(ArgumentError) { described_class.partition_count_cache_ttl_ms = 0 }
    end
  end

  context "statistics callback" do
    context "with a proc/lambda" do
      it "sets the callback" do
        described_class.statistics_callback = lambda do |stats|
        end

        assert_respond_to(described_class.statistics_callback, :call)
      end
    end

    context "with a callable object" do
      it "sets the callback" do
        callback = Class.new do
          def call(stats)
          end
        end
        described_class.statistics_callback = callback.new

        assert_respond_to(described_class.statistics_callback, :call)
      end
    end

    it "does not accept a callback that's not callable" do
      assert_raises(TypeError) do
        described_class.statistics_callback = "a string"
      end
    end
  end

  context "error callback" do
    context "with a proc/lambda" do
      it "sets the callback" do
        described_class.error_callback = lambda do |error|
        end

        assert_respond_to(described_class.error_callback, :call)
      end
    end

    context "with a callable object" do
      it "sets the callback" do
        callback = Class.new do
          def call(stats)
          end
        end
        described_class.error_callback = callback.new

        assert_respond_to(described_class.error_callback, :call)
      end
    end

    it "does not accept a callback that's not callable" do
      assert_raises(TypeError) do
        described_class.error_callback = "a string"
      end
    end

    it "accepts nil to clear the callback" do
      described_class.error_callback = nil

      assert_nil(described_class.error_callback)
    end
  end

  context "oauthbearer calllback" do
    context "with a proc/lambda" do
      it "sets the callback" do
        described_class.oauthbearer_token_refresh_callback = lambda do |config, client_name|
        end

        assert_respond_to(described_class.oauthbearer_token_refresh_callback, :call)
      end
    end

    context "with a callable object" do
      it "sets the callback" do
        callback = Class.new do
          def call(config, client_name)
          end
        end
        described_class.oauthbearer_token_refresh_callback = callback.new

        assert_respond_to(described_class.oauthbearer_token_refresh_callback, :call)
      end
    end

    it "does not accept a callback that's not callable" do
      assert_raises(TypeError) do
        described_class.oauthbearer_token_refresh_callback = "not a callback"
      end
    end
  end

  describe "#describe_properties" do
    it "returns a Hash of property names to values" do
      config = described_class.new
      properties = config.describe_properties

      assert_kind_of(Hash, properties)
      refute_empty(properties)
    end

    it "contains known default properties" do
      config = described_class.new
      properties = config.describe_properties

      assert_includes(properties, :"client.id")
    end

    it "reflects user-set values" do
      config = described_class.new("client.id": "test-client")
      properties = config.describe_properties

      assert_equal("test-client", properties[:"client.id"])
    end
  end

  context "configuration" do
    # Expects Rdkafka::Producer.new to receive the native kafka and the given partitioner. Mocha
    # cannot call through to the original constructor, so a stand-in producer is returned and the
    # native kafka it got is closed afterwards. The stand-in is a plain object, not a mocha stub:
    # `Rdkafka::Clients` keeps it registered and may call `closed?` on it in a later test.
    def expect_producer_with_partitioner(partitioner)
      Rdkafka::Producer
        .expects(:new)
        .with { |kafka, name| (@native_kafka = kafka).is_a?(Rdkafka::NativeKafka) && name == partitioner }
        .returns(producer_stand_in)
    end

    def producer_stand_in
      Object.new.tap do |producer|
        def producer.close = nil

        def producer.closed? = true
      end
    end

    it "stores configuration" do
      config = described_class.new
      config[:key] = "value"

      assert_equal("value", config[:key])
    end

    it "uses default configuration" do
      config = described_class.new

      assert_nil(config[:"api.version.request"])
    end

    it "creates a consumer with valid config" do
      consumer = rdkafka_consumer_config.consumer

      assert_kind_of(Rdkafka::Consumer, consumer)
      consumer.close
    end

    it "creates a consumer with consumer_poll_set set to false" do
      config = rdkafka_consumer_config
      config.consumer_poll_set = false
      consumer = config.consumer

      assert_kind_of(Rdkafka::Consumer, consumer)
      consumer.close
    end

    it "raises an error when creating a consumer with invalid config" do
      config = described_class.new("invalid.key" => "value")
      error = assert_raises(Rdkafka::Config::ConfigError) do
        config.consumer
      end

      assert_equal("No such configuration property: \"invalid.key\"", error.message)
    end

    it "raises an error when creating a consumer with a nil key in the config" do
      config = described_class.new(nil => "value")
      error = assert_raises(Rdkafka::Config::ConfigError) do
        config.consumer
      end

      assert_equal("No such configuration property: \"\"", error.message)
    end

    it "treats a nil value as blank" do
      config = described_class.new("security.protocol" => nil)
      error = assert_raises(Rdkafka::Config::ConfigError) do
        config.consumer
      end

      assert_equal("Configuration property \"security.protocol\" cannot be set to empty value", error.message)
    end

    it "creates a producer with valid config" do
      producer = rdkafka_consumer_config.producer

      assert_kind_of(Rdkafka::Producer, producer)
      producer.close
    end

    it "raises an error when creating a producer with invalid config" do
      config = described_class.new("invalid.key" => "value")
      error = assert_raises(Rdkafka::Config::ConfigError) do
        config.producer
      end

      assert_equal("No such configuration property: \"invalid.key\"", error.message)
    end

    it "allows string partitioner key" do
      expect_producer_with_partitioner("murmur2")
      config = described_class.new("partitioner" => "murmur2")
      config.producer.close
    ensure
      @native_kafka&.close
    end

    it "allows symbol partitioner key" do
      expect_producer_with_partitioner("murmur2")
      config = described_class.new(partitioner: "murmur2")
      config.producer.close
    ensure
      @native_kafka&.close
    end

    it "allows configuring zstd compression" do
      config = described_class.new("compression.codec" => "zstd")
      begin
        producer = config.producer

        assert_kind_of(Rdkafka::Producer, producer)
        producer.close
      rescue Rdkafka::Config::ConfigError
        skip "Zstd compression not supported on this machine"
      end
    end

    it "raises an error when client creation fails for a consumer" do
      config = described_class.new(
        "security.protocol" => "SSL",
        "ssl.ca.location" => "/nonsense"
      )
      error = assert_raises(Rdkafka::Config::ClientCreationError) do
        config.consumer
      end

      assert_match(/ssl.ca.location failed(.*)/, error.message)
    end

    it "raises an error when client creation fails for a producer" do
      config = described_class.new(
        "security.protocol" => "SSL",
        "ssl.ca.location" => "/nonsense"
      )
      error = assert_raises(Rdkafka::Config::ClientCreationError) do
        config.producer
      end

      assert_match(/ssl.ca.location failed(.*)/, error.message)
    end

    it "destroys the native handle when setup after client creation fails" do
      handle = nil
      real_destroy = Rdkafka::Bindings.method(:rd_kafka_destroy)
      Rdkafka::Bindings.stubs(:rd_kafka_queue_get_main).raises(RuntimeError, "setup failed")
      Rdkafka::Bindings.expects(:rd_kafka_destroy).once.with { |kafka| handle = kafka }

      error = assert_raises(RuntimeError) { described_class.new.producer }

      assert_equal("setup failed", error.message)
    ensure
      real_destroy&.call(handle) if handle
    end

    it "destroys the native handle when wrapping it fails" do
      handle = nil
      real_destroy = Rdkafka::Bindings.method(:rd_kafka_destroy)
      Rdkafka::NativeKafka.stubs(:new).raises(RuntimeError, "wrapper failed")
      Rdkafka::Bindings.expects(:rd_kafka_destroy).once.with { |kafka| handle = kafka }

      error = assert_raises(RuntimeError) { described_class.new.producer }

      assert_equal("wrapper failed", error.message)
    ensure
      real_destroy&.call(handle) if handle
    end

    it "raises the original error when destroying the native handle also fails" do
      handle = nil
      real_destroy = Rdkafka::Bindings.method(:rd_kafka_destroy)
      Rdkafka::NativeKafka.stubs(:new).raises(RuntimeError, "wrapper failed")
      Rdkafka::Bindings.stubs(:rd_kafka_destroy).with { |kafka| handle = kafka }.raises(RuntimeError, "destroy failed")
      described_class.logger.stubs(:error)
      described_class.logger.expects(:error).with(regexp_matches(/destroy failed/))

      error = assert_raises(RuntimeError) { described_class.new.producer }

      assert_equal("wrapper failed", error.message)
    ensure
      real_destroy&.call(handle) if handle
    end

    %i[consumer producer admin].each do |client_type|
      it "closes the #{client_type} when starting it fails" do
        handle = nil
        real_destroy = Rdkafka::Bindings.method(:rd_kafka_destroy)
        Rdkafka::NativeKafka.any_instance.stubs(:start).raises(RuntimeError, "start failed")
        Rdkafka::Bindings.expects(:rd_kafka_destroy).once.with { |kafka| handle = kafka }

        config = (client_type == :consumer) ? rdkafka_consumer_config : described_class.new
        error = assert_raises(RuntimeError) { config.public_send(client_type) }

        assert_equal("start failed", error.message)
      ensure
        real_destroy&.call(handle) if handle
      end
    end

    it "destroys the native config when an invalid option aborts native_config" do
      real_conf_destroy = Rdkafka::Bindings.method(:rd_kafka_conf_destroy)
      created_conf = Rdkafka::Bindings.rd_kafka_conf_new
      Rdkafka::Bindings.stubs(:rd_kafka_conf_new).returns(created_conf)
      Rdkafka::Bindings
        .expects(:rd_kafka_conf_destroy)
        .with { |conf| conf.address == created_conf.address }

      config = described_class.new("invalid.key" => "value")

      assert_raises(Rdkafka::Config::ConfigError) { config.consumer }
    ensure
      real_conf_destroy&.call(created_conf) if created_conf
    end

    it "destroys the native config when client creation fails" do
      real_conf_destroy = Rdkafka::Bindings.method(:rd_kafka_conf_destroy)
      created_conf = Rdkafka::Bindings.rd_kafka_conf_new
      Rdkafka::Bindings.stubs(:rd_kafka_conf_new).returns(created_conf)
      Rdkafka::Bindings
        .expects(:rd_kafka_conf_destroy)
        .with { |conf| conf.address == created_conf.address }

      config = described_class.new(
        "security.protocol" => "SSL",
        "ssl.ca.location" => "/nonsense"
      )

      assert_raises(Rdkafka::Config::ClientCreationError) { config.consumer }
    ensure
      real_conf_destroy&.call(created_conf) if created_conf
    end
  end
end
