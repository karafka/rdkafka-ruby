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
      expect(rdkafka_config.consumer).to be_a Rdkafka::Consumer
    end

    it "should raise an error when creating a consumer with invalid config" do
      config = Rdkafka::Config.new('invalid.key' => 'value')
      expect {
        config.consumer
      }.to raise_error(Rdkafka::Config::ConfigError, "No such configuration property: \"invalid.key\"")
    end

    it "should create a producer with valid config" do
      expect(rdkafka_config.producer).to be_a Rdkafka::Producer
    end

    it "should raise an error when creating a producer with invalid config" do
      config = Rdkafka::Config.new('invalid.key' => 'value')
      expect {
        config.producer
      }.to raise_error(Rdkafka::Config::ConfigError, "No such configuration property: \"invalid.key\"")
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
