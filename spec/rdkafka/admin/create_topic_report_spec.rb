require "spec_helper"

describe Rdkafka::Admin::CreateTopicReport do
  subject { Rdkafka::Admin::CreateTopicReport.new(
      FFI::MemoryPointer.from_string("error string"),
      FFI::MemoryPointer.from_string("result name")
  )}

  it "should get the error string" do
    expect(subject.error_string).to eq("error string")
  end

  it "should get the result name" do
    expect(subject.result_name).to eq("result name")
  end
end
