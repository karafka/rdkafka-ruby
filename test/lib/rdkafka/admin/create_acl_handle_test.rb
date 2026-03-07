# frozen_string_literal: true

require "test_helper"

class CreateAclHandleTest < Minitest::Test
  def new_subject(pending_handle:)
    Rdkafka::Admin::CreateAclHandle.new.tap do |handle|
      handle[:pending] = pending_handle
      handle[:response] = Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR
      handle[:response_string] = FFI::MemoryPointer.from_string("")
    end
  end

  def test_wait_raises_timeout
    subject = new_subject(pending_handle: true)
    error = assert_raises(Rdkafka::Admin::CreateAclHandle::WaitTimeoutError) do
      subject.wait(max_wait_timeout_ms: 100)
    end
    assert_match(/create acl/, error.message)
  end

  def test_wait_returns_report_when_not_pending
    subject = new_subject(pending_handle: false)
    report = subject.wait

    assert_equal "", report.rdkafka_response_string
  end

  def test_wait_without_timeout
    subject = new_subject(pending_handle: false)
    report = subject.wait(max_wait_timeout_ms: nil)

    assert_equal "", report.rdkafka_response_string
  end

  def test_raise_error
    subject = new_subject(pending_handle: false)
    error = assert_raises(Rdkafka::RdkafkaError) { subject.raise_error }
    assert_match(/Success \(no_error\)/, error.message)
  end
end
