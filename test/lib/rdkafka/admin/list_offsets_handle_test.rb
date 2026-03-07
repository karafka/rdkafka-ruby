# frozen_string_literal: true

require "test_helper"

class ListOffsetsHandleTest < Minitest::Test
  def new_subject(pending_handle:)
    Rdkafka::Admin::ListOffsetsHandle.new.tap do |handle|
      handle[:pending] = pending_handle
      handle[:response] = Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR
      handle[:response_string] = FFI::MemoryPointer.from_string("")
      handle[:result_infos] = FFI::Pointer::NULL
      handle[:result_count] = 0
    end
  end

  def test_wait_raises_timeout
    subject = new_subject(pending_handle: true)
    error = assert_raises(Rdkafka::Admin::ListOffsetsHandle::WaitTimeoutError) do
      subject.wait(max_wait_timeout_ms: 100)
    end
    assert_match(/list offsets/, error.message)
  end

  def test_wait_returns_report_when_not_pending
    subject = new_subject(pending_handle: false)
    report = subject.wait

    assert_kind_of Rdkafka::Admin::ListOffsetsReport, report
    assert_equal [], report.offsets
  end

  def test_wait_without_timeout
    subject = new_subject(pending_handle: false)
    report = subject.wait(max_wait_timeout_ms: nil)

    assert_equal [], report.offsets
  end

  def test_raise_error
    subject = new_subject(pending_handle: false)
    error = assert_raises(Rdkafka::RdkafkaError) { subject.raise_error }
    assert_match(/Success \(no_error\)/, error.message)
  end
end
