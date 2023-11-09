# frozen_string_literal: true

describe Rdkafka::Producer::DeliveryReport do
  subject { Rdkafka::Producer::DeliveryReport.new(2, 100, "topic", -1) }

  it "should get the partition" do
    expect(subject.partition).to eq 2
  end

  it "should get the offset" do
    expect(subject.offset).to eq 100
  end

  it "should get the topic_name" do
    expect(subject.topic_name).to eq "topic"
  end

  it "should get the error" do
    expect(subject.error).to eq -1
  end
end
