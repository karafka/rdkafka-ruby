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

  it "should produce a message" do
    handle = producer.produce(
      topic: "produce_test_topic",
      payload: "payload 1",
      key: "key 1"
    )
    expect(handle.pending?).to be true

    report = handle.wait
    expect(handle.pending?).to be false
    expect(report).not_to be_nil
    expect(report.partition).to eq 0
    expect(report.offset).to be > 0
  end
end
