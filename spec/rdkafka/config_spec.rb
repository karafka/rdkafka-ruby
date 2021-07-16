require "spec_helper"

describe Rdkafka::Config do
  context "logger" do
    it "should have a default logger" do
      expect(Rdkafka::Config.logger).to be_a Logger
    end

    it "should set the logger" do
      logger = Logger.new(STDOUT)
      expect(Rdkafka::Config.logger).not_to eq logger
      Rdkafka::Config.logger = logger
      expect(Rdkafka::Config.logger).to eq logger
    end

    it "should not accept a nil logger" do
      expect {
        Rdkafka::Config.logger = nil
      }.to raise_error(Rdkafka::Config::NoLoggerError)
    end

    it "supports logging queue" do
      log = StringIO.new
      Rdkafka::Config.logger = Logger.new(log)

      Rdkafka::Config.log_queue << [Logger::FATAL, "I love testing"]
      20.times do
        break if log.string != ""
        sleep 0.05
      end

      expect(log.string).to include "FATAL -- : I love testing"
    end
  end

  context "statistics callback" do
    context "with a proc/lambda" do
      it "should set the callback" do
        expect {
          Rdkafka::Config.statistics_callback = lambda do |stats|
            puts stats
          end
        }.not_to raise_error
        expect(Rdkafka::Config.statistics_callback).to respond_to :call
      end
    end

    context "with a callable object" do
      it "should set the callback" do
        callback = Class.new do
          def call(stats); end
        end
        expect {
          Rdkafka::Config.statistics_callback = callback.new
        }.not_to raise_error
        expect(Rdkafka::Config.statistics_callback).to respond_to :call
      end
    end

    it "should not accept a callback that's not callable" do
      expect {
        Rdkafka::Config.statistics_callback = 'a string'
      }.to raise_error(TypeError)
    end
  end

  context "error callback" do
    context "with a proc/lambda" do
      it "should set the callback" do
        expect {
          Rdkafka::Config.error_callback = lambda do |error|
            puts error
          end
        }.not_to raise_error
        expect(Rdkafka::Config.error_callback).to respond_to :call
      end
    end

    context "with a callable object" do
      it "should set the callback" do
        callback = Class.new do
          def call(stats); end
        end
        expect {
          Rdkafka::Config.error_callback = callback.new
        }.not_to raise_error
        expect(Rdkafka::Config.error_callback).to respond_to :call
      end
    end

    it "should not accept a callback that's not callable" do
      expect {
        Rdkafka::Config.error_callback = 'a string'
      }.to raise_error(TypeError)
    end
  end

  context "configuration" do
    it "should store configuration" do
      config = Rdkafka::Config.new
      config[:"key"] = 'value'
      expect(config[:"key"]).to eq 'value'
    end

    it "should use default configuration" do
      config = Rdkafka::Config.new
      expect(config[:"api.version.request"]).to eq true
    end

    it "should create a consumer with valid config" do
      consumer = rdkafka_config.consumer
      expect(consumer).to be_a Rdkafka::Consumer
      consumer.close
    end

    it "should raise an error when creating a consumer with invalid config" do
      config = Rdkafka::Config.new('invalid.key' => 'value')
      expect {
        config.consumer
      }.to raise_error(Rdkafka::Config::ConfigError, "No such configuration property: \"invalid.key\"")
    end

    it "should raise an error when creating a consumer with a nil key in the config" do
      config = Rdkafka::Config.new(nil => 'value')
      expect {
        config.consumer
      }.to raise_error(Rdkafka::Config::ConfigError, "No such configuration property: \"\"")
    end

    it "should treat a nil value as blank" do
      config = Rdkafka::Config.new('security.protocol' => nil)
      expect {
        config.consumer
        config.producer
      }.to raise_error(Rdkafka::Config::ConfigError, "Configuration property \"security.protocol\" cannot be set to empty value")
    end

    it "should create a producer with valid config" do
      producer = rdkafka_config.producer
      expect(producer).to be_a Rdkafka::Producer
      producer.close
    end

    it "should raise an error when creating a producer with invalid config" do
      config = Rdkafka::Config.new('invalid.key' => 'value')
      expect {
        config.producer
      }.to raise_error(Rdkafka::Config::ConfigError, "No such configuration property: \"invalid.key\"")
    end

    it "should allow configuring zstd compression" do
      config = Rdkafka::Config.new('compression.codec' => 'zstd')
      begin
        expect(config.producer).to be_a Rdkafka::Producer
        config.producer.close
      rescue Rdkafka::Config::ConfigError => ex
        pending "Zstd compression not supported on this machine"
        raise ex
      end
    end

    it "should raise an error when client creation fails for a consumer" do
      config = Rdkafka::Config.new(
        "security.protocol" => "SSL",
        "ssl.ca.location" => "/nonsense"
      )
      expect {
        config.consumer
      }.to raise_error(Rdkafka::Config::ClientCreationError, /ssl.ca.location failed(.*)/)
    end

    it "should raise an error when client creation fails for a producer" do
      config = Rdkafka::Config.new(
        "security.protocol" => "SSL",
        "ssl.ca.location" => "/nonsense"
      )
      expect {
        config.producer
      }.to raise_error(Rdkafka::Config::ClientCreationError, /ssl.ca.location failed(.*)/)
    end
  end
end
