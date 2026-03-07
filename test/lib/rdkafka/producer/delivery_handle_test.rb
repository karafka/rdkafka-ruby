# frozen_string_literal: true

require "test_helper"

class DeliveryHandleTest < Minitest::Test
  def new_subject(pending_handle:, response: 0)
    Rdkafka::Producer::DeliveryHandle.new.tap do |handle|
      handle[:pending] = pending_handle
      handle[:response] = response
      handle[:partition] = 2
      handle[:offset] = 100
      handle.topic = TestTopics.produce_test_topic
    end
  end

  def test_wait_raises_timeout
    subject = new_subject(pending_handle: true)
    error = assert_raises(Rdkafka::Producer::DeliveryHandle::WaitTimeoutError) do
      subject.wait(max_wait_timeout_ms: 100)
    end
    assert_match(/delivery/, error.message)
  end

  def test_wait_returns_report_when_not_pending
    subject = new_subject(pending_handle: false)
    report = subject.wait

    assert_equal 2, report.partition
    assert_equal 100, report.offset
    assert_equal TestTopics.produce_test_topic, report.topic_name
  end

  def test_wait_without_timeout
    subject = new_subject(pending_handle: false)
    report = subject.wait(max_wait_timeout_ms: nil)

    assert_equal 2, report.partition
    assert_equal 100, report.offset
    assert_equal TestTopics.produce_test_topic, report.topic_name
  end

  def test_create_result_no_error
    subject = new_subject(pending_handle: false, response: 0)
    report = subject.create_result

    assert_nil report.error
  end

  def test_create_result_with_error
    subject = new_subject(pending_handle: false, response: 1)
    report = subject.create_result

    assert_equal Rdkafka::RdkafkaError.new(1), report.error
  end
end
