# frozen_string_literal: true

RSpec.describe Rdkafka::Config do
  context "logger" do
    it "has a default logger" do
      expect(described_class.logger).to be_a Logger
    end

    it "sets the logger" do
      logger = Logger.new($stdout)
      expect(described_class.logger).not_to eq logger
      described_class.logger = logger
      expect(described_class.logger).to eq logger
    end

    it "does not accept a nil logger" do
      expect {
        described_class.logger = nil
      }.to raise_error(Rdkafka::Config::NoLoggerError)
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

      expect(log.string).to include "FATAL -- : I love testing"
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
        expect(output.split("\n").size).to be >= 20
      end
    end
  end

  context "statistics callback" do
    context "with a proc/lambda" do
      it "sets the callback" do
        expect {
          described_class.statistics_callback = lambda do |stats|
          end
        }.not_to raise_error
        expect(described_class.statistics_callback).to respond_to :call
      end
    end

    context "with a callable object" do
      it "sets the callback" do
        callback = Class.new do
          def call(stats)
          end
        end
        expect {
          described_class.statistics_callback = callback.new
        }.not_to raise_error
        expect(described_class.statistics_callback).to respond_to :call
      end
    end

    it "does not accept a callback that's not callable" do
      expect {
        described_class.statistics_callback = "a string"
      }.to raise_error(TypeError)
    end
  end

  context "error callback" do
    context "with a proc/lambda" do
      it "sets the callback" do
        expect {
          described_class.error_callback = lambda do |error|
          end
        }.not_to raise_error
        expect(described_class.error_callback).to respond_to :call
      end
    end

    context "with a callable object" do
      it "sets the callback" do
        callback = Class.new do
          def call(stats)
          end
        end
        expect {
          described_class.error_callback = callback.new
        }.not_to raise_error
        expect(described_class.error_callback).to respond_to :call
      end
    end

    it "does not accept a callback that's not callable" do
      expect {
        described_class.error_callback = "a string"
      }.to raise_error(TypeError)
    end
  end

  context "oauthbearer calllback" do
    context "with a proc/lambda" do
      it "sets the callback" do
        expect {
          described_class.oauthbearer_token_refresh_callback = lambda do |config, client_name|
          end
        }.not_to raise_error
        expect(described_class.oauthbearer_token_refresh_callback).to respond_to :call
      end
    end

    context "with a callable object" do
      it "sets the callback" do
        callback = Class.new do
          def call(config, client_name)
          end
        end

        expect {
          described_class.oauthbearer_token_refresh_callback = callback.new
        }.not_to raise_error
        expect(described_class.oauthbearer_token_refresh_callback).to respond_to :call
      end
    end

    it "does not accept a callback that's not callable" do
      expect {
        described_class.oauthbearer_token_refresh_callback = "not a callback"
      }.to raise_error(TypeError)
    end
  end

  context "configuration" do
    it "stores configuration" do
      config = described_class.new
      config[:key] = "value"
      expect(config[:key]).to eq "value"
    end

    it "uses default configuration" do
      config = described_class.new
      expect(config[:"api.version.request"]).to be_nil
    end

    it "creates a consumer with valid config" do
      consumer = rdkafka_consumer_config.consumer
      expect(consumer).to be_a Rdkafka::Consumer
      consumer.close
    end

    it "creates a consumer with consumer_poll_set set to false" do
      config = rdkafka_consumer_config
      config.consumer_poll_set = false
      consumer = config.consumer
      expect(consumer).to be_a Rdkafka::Consumer
      consumer.close
    end

    it "raises an error when creating a consumer with invalid config" do
      config = described_class.new("invalid.key" => "value")
      expect {
        config.consumer
      }.to raise_error(Rdkafka::Config::ConfigError, "No such configuration property: \"invalid.key\"")
    end

    it "raises an error when creating a consumer with a nil key in the config" do
      config = described_class.new(nil => "value")
      expect {
        config.consumer
      }.to raise_error(Rdkafka::Config::ConfigError, "No such configuration property: \"\"")
    end

    it "treats a nil value as blank" do
      config = described_class.new("security.protocol" => nil)
      expect {
        config.consumer
        config.producer
      }.to raise_error(Rdkafka::Config::ConfigError, "Configuration property \"security.protocol\" cannot be set to empty value")
    end

    it "creates a producer with valid config" do
      producer = rdkafka_consumer_config.producer
      expect(producer).to be_a Rdkafka::Producer
      producer.close
    end

    it "raises an error when creating a producer with invalid config" do
      config = described_class.new("invalid.key" => "value")
      expect {
        config.producer
      }.to raise_error(Rdkafka::Config::ConfigError, "No such configuration property: \"invalid.key\"")
    end

    it "allows string partitioner key" do
      expect(Rdkafka::Producer).to receive(:new).with(kind_of(Rdkafka::NativeKafka), "murmur2").and_call_original
      config = described_class.new("partitioner" => "murmur2")
      config.producer.close
    end

    it "allows symbol partitioner key" do
      expect(Rdkafka::Producer).to receive(:new).with(kind_of(Rdkafka::NativeKafka), "murmur2").and_call_original
      config = described_class.new(partitioner: "murmur2")
      config.producer.close
    end

    it "allows configuring zstd compression" do
      config = described_class.new("compression.codec" => "zstd")
      begin
        producer = config.producer
        expect(producer).to be_a Rdkafka::Producer
        producer.close
      rescue Rdkafka::Config::ConfigError => ex
        pending "Zstd compression not supported on this machine"
        raise ex
      end
    end

    it "raises an error when client creation fails for a consumer" do
      config = described_class.new(
        "security.protocol" => "SSL",
        "ssl.ca.location" => "/nonsense"
      )
      expect {
        config.consumer
      }.to raise_error(Rdkafka::Config::ClientCreationError, /ssl.ca.location failed(.*)/)
    end

    it "raises an error when client creation fails for a producer" do
      config = described_class.new(
        "security.protocol" => "SSL",
        "ssl.ca.location" => "/nonsense"
      )
      expect {
        config.producer
      }.to raise_error(Rdkafka::Config::ClientCreationError, /ssl.ca.location failed(.*)/)
    end
  end
end
