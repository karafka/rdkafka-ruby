require "spec_helper"

describe Rdkafka::Producer do
  let(:producer) { rdkafka_config.producer }

  it "should require a topic" do
    expect {
      producer.produce(
        payload: "payload",
        key:     "key"
     )
    }.to raise_error ArgumentError, "missing keyword: topic"
  end

  it "should produce a message" do
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

    # Close producer
    producer.close

    # Consume message and verify it's content
    message = wait_for_message(
      topic: "produce_test_topic",
      delivery_report: report
    )
    expect(message.partition).to eq 0
    expect(message.payload).to eq "payload 1"
    expect(message.key).to eq "key 1"
    # Since api.version.request is on by default we will get
    # the message creation timestamp if it's not set.
    expect(message.timestamp).to be > 1505069891557
  end

  it "should produce a message with utf-8 encoding" do
    handle = producer.produce(
      topic:   "produce_test_topic",
      payload: "Τη γλώσσα μου έδωσαν ελληνική",
      key:     "key utf8"
    )
    expect(handle.pending?).to be true

    # Check delivery handle and report
    report = handle.wait(5)

    # Close producer
    producer.close

    # Consume message and verify it's content
    message = wait_for_message(
      topic: "produce_test_topic",
      delivery_report: report
    )

    expect(message.partition).to eq 1
    expect(message.payload.force_encoding("utf-8")).to eq "Τη γλώσσα μου έδωσαν ελληνική"
    expect(message.key).to eq "key utf8"
  end

  it "should produce a message with a timestamp" do
    handle = producer.produce(
      topic:     "produce_test_topic",
      payload:   "Payload timestamp",
      key:       "key timestamp",
      timestamp: 1505069646000
    )
    expect(handle.pending?).to be true

    # Check delivery handle and report
    report = handle.wait(5)

    # Close producer
    producer.close

    # Consume message and verify it's content
    message = wait_for_message(
      topic: "produce_test_topic",
      delivery_report: report
    )

    expect(message.partition).to eq 2
    expect(message.key).to eq "key timestamp"
    expect(message.timestamp).to eq 1505069646000
  end

  it "should raise a timeout error when waiting too long" do
    handle = producer.produce(
      topic:   "produce_test_topic",
      payload: "payload 1",
      key:     "key 1"
    )
    expect {
      handle.wait(0)
    }.to raise_error Rdkafka::Producer::DeliveryHandle::WaitTimeoutError
  end
end
