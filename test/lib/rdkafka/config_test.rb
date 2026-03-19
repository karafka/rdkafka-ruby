# frozen_string_literal: true

require_relative "../../test_helper"

describe Rdkafka::Config do
  context "logger" do
    it "has a default logger" do
      assert_kind_of Logger, described_class.logger
    end

    it "sets the logger" do
      logger = Logger.new($stdout)
      refute_equal logger, described_class.logger
      described_class.logger = logger
      assert_equal logger, described_class.logger
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

      assert_includes log.string, "FATAL -- : I love testing"
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
        assert_operator output.split("\n").size, :>=, 20
      end
    end
  end

  context "statistics callback" do
    context "with a proc/lambda" do
      it "sets the callback" do
        described_class.statistics_callback = lambda do |stats|
        end
        assert_respond_to described_class.statistics_callback, :call
      end
    end

    context "with a callable object" do
      it "sets the callback" do
        callback = Class.new do
          def call(stats)
          end
        end
        described_class.statistics_callback = callback.new
        assert_respond_to described_class.statistics_callback, :call
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
        assert_respond_to described_class.error_callback, :call
      end
    end

    context "with a callable object" do
      it "sets the callback" do
        callback = Class.new do
          def call(stats)
          end
        end
        described_class.error_callback = callback.new
        assert_respond_to described_class.error_callback, :call
      end
    end

    it "does not accept a callback that's not callable" do
      assert_raises(TypeError) do
        described_class.error_callback = "a string"
      end
    end

    it "accepts nil to clear the callback" do
      described_class.error_callback = nil
    end
  end

  context "oauthbearer calllback" do
    context "with a proc/lambda" do
      it "sets the callback" do
        described_class.oauthbearer_token_refresh_callback = lambda do |config, client_name|
        end
        assert_respond_to described_class.oauthbearer_token_refresh_callback, :call
      end
    end

    context "with a callable object" do
      it "sets the callback" do
        callback = Class.new do
          def call(config, client_name)
          end
        end

        described_class.oauthbearer_token_refresh_callback = callback.new
        assert_respond_to described_class.oauthbearer_token_refresh_callback, :call
      end
    end

    it "does not accept a callback that's not callable" do
      assert_raises(TypeError) do
        described_class.oauthbearer_token_refresh_callback = "not a callback"
      end
    end
  end

  context "configuration" do
    it "stores configuration" do
      config = described_class.new
      config[:key] = "value"
      assert_equal "value", config[:key]
    end

    it "uses default configuration" do
      config = described_class.new
      assert_nil config[:"api.version.request"]
    end

    it "creates a consumer with valid config" do
      consumer = rdkafka_consumer_config.consumer
      assert_kind_of Rdkafka::Consumer, consumer
      consumer.close
    end

    it "creates a consumer with consumer_poll_set set to false" do
      config = rdkafka_consumer_config
      config.consumer_poll_set = false
      consumer = config.consumer
      assert_kind_of Rdkafka::Consumer, consumer
      consumer.close
    end

    it "raises an error when creating a consumer with invalid config" do
      config = described_class.new("invalid.key" => "value")
      e = assert_raises(Rdkafka::Config::ConfigError) do
        config.consumer
      end
      assert_equal "No such configuration property: \"invalid.key\"", e.message
    end

    it "raises an error when creating a consumer with a nil key in the config" do
      config = described_class.new(nil => "value")
      e = assert_raises(Rdkafka::Config::ConfigError) do
        config.consumer
      end
      assert_equal "No such configuration property: \"\"", e.message
    end

    it "treats a nil value as blank" do
      config = described_class.new("security.protocol" => nil)
      e = assert_raises(Rdkafka::Config::ConfigError) do
        config.consumer
        config.producer
      end
      assert_equal "Configuration property \"security.protocol\" cannot be set to empty value", e.message
    end

    it "creates a producer with valid config" do
      producer = rdkafka_consumer_config.producer
      assert_kind_of Rdkafka::Producer, producer
      producer.close
    end

    it "raises an error when creating a producer with invalid config" do
      config = described_class.new("invalid.key" => "value")
      e = assert_raises(Rdkafka::Config::ConfigError) do
        config.producer
      end
      assert_equal "No such configuration property: \"invalid.key\"", e.message
    end

    it "allows string partitioner key" do
      config = described_class.new("partitioner" => "murmur2")
      producer = config.producer
      assert_kind_of Rdkafka::Producer, producer
      producer.close
    end

    it "allows symbol partitioner key" do
      config = described_class.new(partitioner: "murmur2")
      producer = config.producer
      assert_kind_of Rdkafka::Producer, producer
      producer.close
    end

    it "allows configuring zstd compression" do
      config = described_class.new("compression.codec" => "zstd")
      begin
        producer = config.producer
        assert_kind_of Rdkafka::Producer, producer
        producer.close
      rescue Rdkafka::Config::ConfigError => ex
        skip "Zstd compression not supported on this machine"
        raise ex
      end
    end

    it "raises an error when client creation fails for a consumer" do
      config = described_class.new(
        "security.protocol" => "SSL",
        "ssl.ca.location" => "/nonsense"
      )
      e = assert_raises(Rdkafka::Config::ClientCreationError) do
        config.consumer
      end
      assert_match(/ssl.ca.location failed(.*)/, e.message)
    end

    it "raises an error when client creation fails for a producer" do
      config = described_class.new(
        "security.protocol" => "SSL",
        "ssl.ca.location" => "/nonsense"
      )
      e = assert_raises(Rdkafka::Config::ClientCreationError) do
        config.producer
      end
      assert_match(/ssl.ca.location failed(.*)/, e.message)
    end
  end
end
