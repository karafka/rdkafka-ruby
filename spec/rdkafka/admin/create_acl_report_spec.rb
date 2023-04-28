# frozen_string_literal: true

require "spec_helper"

describe Rdkafka::Admin::CreateAclReport do
  subject { Rdkafka::Admin::CreateAclReport.new(
      rdkafka_response: Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR,
      rdkafka_response_string: FFI::MemoryPointer.from_string("")
  )}

  it "should get RD_KAFKA_RESP_ERR_NO_ERROR " do
    expect(subject.rdkafka_response).to eq(0)
  end

  it "should get empty string" do
    expect(subject.rdkafka_response_string).to eq("")
  end
end
