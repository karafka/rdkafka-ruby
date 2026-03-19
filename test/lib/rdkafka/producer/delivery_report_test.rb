# frozen_string_literal: true

require_relative "../../../test_helper"

describe Rdkafka::Producer::DeliveryReport do
  before do
    @topic_name = TestTopics.unique
    @report = described_class.new(2, 100, @topic_name, -1)
  end

  it "gets the partition" do
    assert_equal 2, @report.partition
  end

  it "gets the offset" do
    assert_equal 100, @report.offset
  end

  it "gets the topic_name" do
    assert_equal @topic_name, @report.topic_name
  end

  it "gets the same topic name under topic alias" do
    assert_equal @topic_name, @report.topic
  end

  it "gets the error" do
    assert_equal(-1, @report.error)
  end
end
