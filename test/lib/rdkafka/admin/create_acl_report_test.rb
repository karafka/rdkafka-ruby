# frozen_string_literal: true

require "test_helper"

class CreateAclReportTest < Minitest::Test
  def subject
    @subject ||= Rdkafka::Admin::CreateAclReport.new(
      rdkafka_response: Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR,
      rdkafka_response_string: FFI::MemoryPointer.from_string("")
    )
  end

  def test_gets_no_error_response
    assert_equal 0, subject.rdkafka_response
  end

  def test_gets_empty_string
    assert_equal "", subject.rdkafka_response_string
  end
end
