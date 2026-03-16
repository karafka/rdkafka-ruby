# frozen_string_literal: true

RSpec.describe Rdkafka::Producer::DeliveryReport do
  def topic_name
    @topic_name ||= TestTopics.unique
  end

  def build_report
    Rdkafka::Producer::DeliveryReport.new(2, 100, topic_name, -1)
  end

  it "gets the partition" do
    expect(build_report.partition).to eq 2
  end

  it "gets the offset" do
    expect(build_report.offset).to eq 100
  end

  it "gets the topic_name" do
    expect(build_report.topic_name).to eq topic_name
  end

  it "gets the same topic name under topic alias" do
    expect(build_report.topic).to eq topic_name
  end

  it "gets the error" do
    expect(build_report.error).to eq(-1)
  end
end
