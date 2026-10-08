# frozen_string_literal: true

RSpec.describe Rdkafka::Producer::DeliveryReport do
  let(:report) { described_class.new(2, 100, topic_name, -1) }

  let(:topic_name) { TestTopics.unique }

  it "gets the partition" do
    expect(report.partition).to eq 2
  end

  it "gets the offset" do
    expect(report.offset).to eq 100
  end

  it "gets the topic_name" do
    expect(report.topic_name).to eq topic_name
  end

  it "gets the same topic name under topic alias" do
    expect(report.topic).to eq topic_name
  end

  it "gets the error" do
    expect(report.error).to eq(-1)
  end

  it "has no status, latency or broker id by default" do
    expect(report.status).to be_nil
    expect(report.latency).to be_nil
    expect(report.broker_id).to be_nil
    expect(report).not_to be_persisted
    expect(report).not_to be_possibly_persisted
    expect(report).not_to be_not_persisted
  end

  context "with status, latency and broker id" do
    let(:report) do
      described_class.new(
        2,
        100,
        topic_name,
        nil,
        nil,
        status: Rdkafka::Bindings::RD_KAFKA_MSG_STATUS_POSSIBLY_PERSISTED,
        latency: 1_500,
        broker_id: 3
      )
    end

    it "gets them" do
      expect(report.status).to eq(Rdkafka::Bindings::RD_KAFKA_MSG_STATUS_POSSIBLY_PERSISTED)
      expect(report.latency).to eq(1_500)
      expect(report.broker_id).to eq(3)
    end

    it "answers only the matching status predicate" do
      expect(report).to be_possibly_persisted
      expect(report).not_to be_persisted
      expect(report).not_to be_not_persisted
    end
  end

  context "when librdkafka reports latency and broker id as unavailable" do
    let(:report) { described_class.new(2, 100, topic_name, nil, nil, latency: -1, broker_id: -1) }

    it "returns nil for both" do
      expect(report.latency).to be_nil
      expect(report.broker_id).to be_nil
    end
  end
end
