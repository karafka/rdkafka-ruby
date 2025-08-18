# frozen_string_literal: true

require "spec_helper"

describe Rdkafka::Admin::CreateAclHandle do
  # If create acl was successful there is no error object
  # the error code is set to RD_KAFKA_RESP_ERR_NO_ERRORa
  # https://github.com/confluentinc/librdkafka/blob/1f9f245ac409f50f724695c628c7a0d54a763b9a/src/rdkafka_error.c#L169
  let(:response) { Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR }

  subject do
    Rdkafka::Admin::CreateAclHandle.new.tap do |handle|
      handle[:pending] = pending_handle
      handle[:response] = response
      # If create acl was successful there is no error object and the error_string is set to ""
      # https://github.com/confluentinc/librdkafka/blob/1f9f245ac409f50f724695c628c7a0d54a763b9a/src/rdkafka_error.c#L178
      handle[:response_string] = FFI::MemoryPointer.from_string("")
    end
  end

  describe "#wait" do
    let(:pending_handle) { true }

    it "should wait until the timeout and then raise an error" do
      expect {
        subject.wait(max_wait_timeout: 0.1)
      }.to raise_error Rdkafka::Admin::CreateAclHandle::WaitTimeoutError, /create acl/
    end

    context "when not pending anymore and no error" do
      let(:pending_handle) { false }

      it "should return a create acl report" do
        report = subject.wait

        expect(report.rdkafka_response_string).to eq("")
      end

      it "should wait without a timeout" do
        report = subject.wait(max_wait_timeout: nil)

        expect(report.rdkafka_response_string).to eq("")
      end
    end
  end

  describe "#raise_error" do
    let(:pending_handle) { false }

    it "should raise the appropriate error" do
      expect {
        subject.raise_error
      }.to raise_exception(Rdkafka::RdkafkaError, /Success \(no_error\)/)
    end
  end
end
