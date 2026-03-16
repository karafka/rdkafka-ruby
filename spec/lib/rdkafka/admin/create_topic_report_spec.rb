# frozen_string_literal: true

RSpec.describe Rdkafka::Admin::CreateTopicReport do
  def build_report
    Rdkafka::Admin::CreateTopicReport.new(
      FFI::MemoryPointer.from_string("error string"),
      FFI::MemoryPointer.from_string("result name")
    )
  end

  it "gets the error string" do
    expect(build_report.error_string).to eq("error string")
  end

  it "gets the result name" do
    expect(build_report.result_name).to eq("result name")
  end
end
