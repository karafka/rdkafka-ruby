# frozen_string_literal: true

require "test_helper"

class ListOffsetsReportTest < Minitest::Test
  def test_returns_empty_offsets_when_null
    subject = Rdkafka::Admin::ListOffsetsReport.new(result_infos: FFI::Pointer::NULL, result_count: 0)

    assert_equal [], subject.offsets
  end
end
