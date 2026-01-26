# frozen_string_literal: true

RSpec.describe Rdkafka::Admin::CreateAclReport do
  subject {
    described_class.new(
      rdkafka_response: Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR,
      rdkafka_response_string: FFI::MemoryPointer.from_string("")
    )
  }

  it "gets RD_KAFKA_RESP_ERR_NO_ERROR" do
    expect(subject.rdkafka_response).to eq(0)
  end

  it "gets empty string" do
    expect(subject.rdkafka_response_string).to eq("")
  end
end
