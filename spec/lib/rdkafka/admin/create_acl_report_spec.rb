# frozen_string_literal: true

RSpec.describe Rdkafka::Admin::CreateAclReport do
  def build_report
    Rdkafka::Admin::CreateAclReport.new(
      rdkafka_response: Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR,
      rdkafka_response_string: FFI::MemoryPointer.from_string("")
    )
  end

  it "gets RD_KAFKA_RESP_ERR_NO_ERROR" do
    expect(build_report.rdkafka_response).to eq(0)
  end

  it "gets empty string" do
    expect(build_report.rdkafka_response_string).to eq("")
  end
end
