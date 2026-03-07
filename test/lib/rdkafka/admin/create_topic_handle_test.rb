# frozen_string_literal: true

require "test_helper"

class CreateTopicHandleTest < Minitest::Test
  def new_subject(pending_handle:, topic_name: nil)
    topic_name ||= TestTopics.unique
    Rdkafka::Admin::CreateTopicHandle.new.tap do |handle|
      handle[:pending] = pending_handle
      handle[:response] = 0
      handle[:error_string] = FFI::Pointer::NULL
      handle[:result_name] = FFI::MemoryPointer.from_string(topic_name)
    end
  end

  def test_wait_raises_timeout
    subject = new_subject(pending_handle: true)
    error = assert_raises(Rdkafka::Admin::CreateTopicHandle::WaitTimeoutError) do
      subject.wait(max_wait_timeout_ms: 100)
    end
    assert_match(/create topic/, error.message)
  end

  def test_wait_returns_report_when_not_pending
    topic_name = TestTopics.unique
    subject = new_subject(pending_handle: false, topic_name: topic_name)
    report = subject.wait

    assert_nil report.error_string
    assert_equal topic_name, report.result_name
  end

  def test_wait_without_timeout
    topic_name = TestTopics.unique
    subject = new_subject(pending_handle: false, topic_name: topic_name)
    report = subject.wait(max_wait_timeout_ms: nil)

    assert_nil report.error_string
    assert_equal topic_name, report.result_name
  end

  def test_raise_error
    subject = new_subject(pending_handle: false)
    error = assert_raises(Rdkafka::RdkafkaError) { subject.raise_error }
    assert_match(/Success \(no_error\)/, error.message)
  end
end
