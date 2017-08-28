require "spec_helper"

describe Rdkafka::Producer do
  let(:producer) do
    rdkafka_config.producer
  end

  it "should require a topic" do
    expect {
      producer.produce(
        nil,
        "payload",
        "key"
     )
    }.to raise_error ArgumentError, "Topic cannot be nil"
  end

  it "should produce messages" do
    producer.produce(
      "produce_test_topic",
      "payload 1",
      "key 1"
    )
  end
end
