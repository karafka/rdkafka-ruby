require "spec_helper"

describe Rdkafka::Config do
  it "should store configuration" do
    config = Rdkafka::Config.new
    config[:"key"] = 'value'
    expect(config[:"key"]).to eq 'value'
  end

  it "should use default configuration" do
    config = Rdkafka::Config.new
    expect(config[:"api.version.request"]).to eq true
    expect(config[:"log.queue"]).to eq true
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
end
