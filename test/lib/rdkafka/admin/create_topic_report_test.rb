# frozen_string_literal: true

describe Rdkafka::Admin::CreateTopicReport do
  subject do
    described_class.new(
      FFI::MemoryPointer.from_string("error string"),
      FFI::MemoryPointer.from_string("result name")
    )
  end

  it "gets the error string" do
    assert_equal "error string", subject.error_string
  end

  it "gets the result name" do
    assert_equal "result name", subject.result_name
  end
end
