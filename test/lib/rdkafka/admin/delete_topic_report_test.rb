# frozen_string_literal: true

require "test_helper"

class DeleteTopicReportTest < Minitest::Test
  def subject
    @subject ||= Rdkafka::Admin::DeleteTopicReport.new(
      FFI::MemoryPointer.from_string("error string"),
      FFI::MemoryPointer.from_string("result name")
    )
  end

  def test_gets_error_string
    assert_equal "error string", subject.error_string
  end

  def test_gets_result_name
    assert_equal "result name", subject.result_name
  end
end
