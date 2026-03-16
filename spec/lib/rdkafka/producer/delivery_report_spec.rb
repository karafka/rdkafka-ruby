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
end
