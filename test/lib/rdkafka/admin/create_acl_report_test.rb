# frozen_string_literal: true

require_relative "../../../test_helper"

describe Rdkafka::Admin::CreateAclReport do
  before do
    @report = described_class.new(
      rdkafka_response: Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR,
      rdkafka_response_string: FFI::MemoryPointer.from_string("")
    )
  end

  it "gets RD_KAFKA_RESP_ERR_NO_ERROR" do
    assert_equal 0, @report.rdkafka_response
  end

  it "gets empty string" do
    assert_equal "", @report.rdkafka_response_string
  end
end
