require "spec_helper"

describe Rdkafka::Producer do
  let(:producer) do
    rdkafka_config.producer
  end

  it "should require a topic" do
    expect {
      producer.produce(
        payload: "payload",
        key: "key"
     )
    }.to raise_error ArgumentError, "missing keyword: topic"
  end

  it "should produce messages" do
    producer.produce(
      topic: "produce_test_topic",
      payload: "payload 1",
      key: "key 1"
    )
  end
end
