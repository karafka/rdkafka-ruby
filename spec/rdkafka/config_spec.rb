require "spec_helper"

describe Rdkafka::Config do
  it "should store configuration" do
    config = Rdkafka::Config.new
    config['key'] = 'value'
    expect(config['key']).to eq 'value'
  end

  it "should create a consumer with valid config" do
    config = Rdkafka::Config.new('metadata.broker.list' => 'localhost')
    config.consumer
    # TODO test consumer
  end

  it "should raise an error when creating a consumer with invalid config" do
    config = Rdkafka::Config.new('invalid.key' => 'value')
    expect {
      config.consumer
    }.to raise_error(Rdkafka::Config::ConfigError, "No such configuration property: \"invalid.key\"")
  end

  it "should create a producer with valid config" do
    config = Rdkafka::Config.new('metadata.broker.list' => 'localhost')
    config.producer
    # TODO test producer
  end

  it "should raise an error when creating a producer with invalid config" do
    config = Rdkafka::Config.new('invalid.key' => 'value')
    expect {
      config.producer
    }.to raise_error(Rdkafka::Config::ConfigError, "No such configuration property: \"invalid.key\"")
  end
end
