# frozen_string_literal: true

require_relative "../../../test_helper"

describe Rdkafka::Admin::DeleteTopicReport do
  before do
    @report = described_class.new(
      FFI::MemoryPointer.from_string("error string"),
      FFI::MemoryPointer.from_string("result name")
    )
  end

  it "gets the error string" do
    assert_equal "error string", @report.error_string
  end

  it "gets the result name" do
    assert_equal "result name", @report.result_name
  end
end
