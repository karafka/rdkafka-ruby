require "spec_helper"

describe Rdkafka::Producer do
  let(:producer) { rdkafka_config.producer }
  let(:consumer) { rdkafka_config.consumer }

  it "should require a topic" do
    expect {
      producer.produce(
        payload: "payload",
        key:     "key"
     )
    }.to raise_error ArgumentError, "missing keyword: topic"
  end

  it "should produce a message" do
    consumer.subscribe("produce_test_topic")
    consumer.poll(100)

    handle = producer.produce(
      topic:   "produce_test_topic",
      payload: "payload 1",
      key:     "key 1"
    )
    expect(handle.pending?).to be true

    # Check delivery handle and report
    report = handle.wait(5)
    expect(handle.pending?).to be false
    expect(report).not_to be_nil
    expect(report.partition).to eq 0
    expect(report.offset).to be > 0

    # Consume message and verify it's content
    message = consumer.poll(5000)
    expect(message).not_to be_nil
    expect(message.partition).to eq 0
    expect(message.offset).to eq report.offset
    expect(message.payload).to eq "payload 1"
    expect(message.key).to eq "key 1"
  end

  it "should raise a timeout error when waiting too long" do
    handle = producer.produce(
      topic:   "produce_test_topic",
      payload: "payload 1",
      key:     "key 1"
    )
    expect {
      handle.wait(0)
    }.to raise_error Rdkafka::WaitTimeoutError
  end
end
