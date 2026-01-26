# frozen_string_literal: true

RSpec.describe Rdkafka::Admin::DeleteTopicReport do
  subject {
    described_class.new(
      FFI::MemoryPointer.from_string("error string"),
      FFI::MemoryPointer.from_string("result name")
    )
  }

  it "gets the error string" do
    expect(subject.error_string).to eq("error string")
  end

  it "gets the result name" do
    expect(subject.result_name).to eq("result name")
  end
end
