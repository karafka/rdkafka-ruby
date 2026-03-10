# frozen_string_literal: true

describe Rdkafka::Producer::DeliveryReport do
  subject { Rdkafka::Producer::DeliveryReport.new(2, 100, topic_name, -1) }

  let(:topic_name) { TestTopics.unique }

  it "gets the partition" do
    assert_equal 2, subject.partition
  end

  it "gets the offset" do
    assert_equal 100, subject.offset
  end

  it "gets the topic_name" do
    assert_equal topic_name, subject.topic_name
  end

  it "gets the same topic name under topic alias" do
    assert_equal topic_name, subject.topic
  end

  it "gets the error" do
    assert_equal(-1, subject.error)
  end
end
