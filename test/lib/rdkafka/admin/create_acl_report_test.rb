# frozen_string_literal: true

describe Rdkafka::Admin::CreateAclReport do
  subject do
    described_class.new(
      rdkafka_response: Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR,
      rdkafka_response_string: FFI::MemoryPointer.from_string("")
    )
  end

  it "gets RD_KAFKA_RESP_ERR_NO_ERROR" do
    assert_equal 0, subject.rdkafka_response
  end

  it "gets empty string" do
    assert_equal "", subject.rdkafka_response_string
  end
end
