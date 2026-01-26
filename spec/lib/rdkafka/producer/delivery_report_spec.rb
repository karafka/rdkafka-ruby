# frozen_string_literal: true

RSpec.describe Rdkafka::Producer::DeliveryReport do
  subject { described_class.new(2, 100, topic_name, -1) }

  let(:topic_name) { TestTopics.unique }

  it "gets the partition" do
    expect(subject.partition).to eq 2
  end

  it "gets the offset" do
    expect(subject.offset).to eq 100
  end

  it "gets the topic_name" do
    expect(subject.topic_name).to eq topic_name
  end

  it "gets the same topic name under topic alias" do
    expect(subject.topic).to eq topic_name
  end

  it "gets the error" do
    expect(subject.error).to eq(-1)
  end
end
