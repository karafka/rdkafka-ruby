# frozen_string_literal: true

require "test_helper"

class DeliveryReportTest < Minitest::Test
  def setup
    super
    @topic_name = TestTopics.unique
    @subject = Rdkafka::Producer::DeliveryReport.new(2, 100, @topic_name, -1)
  end

  def test_gets_partition
    assert_equal 2, @subject.partition
  end

  def test_gets_offset
    assert_equal 100, @subject.offset
  end

  def test_gets_topic_name
    assert_equal @topic_name, @subject.topic_name
  end

  def test_gets_topic_alias
    assert_equal @topic_name, @subject.topic
  end

  def test_gets_error
    assert_equal(-1, @subject.error)
  end
end
