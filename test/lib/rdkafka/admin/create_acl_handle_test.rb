# frozen_string_literal: true

require_relative "../../../test_helper"

describe Rdkafka::Admin::CreateAclHandle do
  before do
    @response = Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR
  end

  describe "#wait" do
    context "when pending" do
      before do
        @handle = described_class.new.tap do |handle|
          handle[:pending] = true
          handle[:response] = @response
          handle[:response_string] = FFI::MemoryPointer.from_string("")
        end
      end

      it "waits until the timeout and then raise an error" do
        e = assert_raises(Rdkafka::Admin::CreateAclHandle::WaitTimeoutError) {
          @handle.wait(max_wait_timeout_ms: 100)
        }
        assert_match(/create acl/, e.message)
      end
    end

    context "when not pending anymore and no error" do
      before do
        @handle = described_class.new.tap do |handle|
          handle[:pending] = false
          handle[:response] = @response
          handle[:response_string] = FFI::MemoryPointer.from_string("")
        end
      end

      it "returns a create acl report" do
        report = @handle.wait

        assert_equal "", report.rdkafka_response_string
      end

      it "waits without a timeout" do
        report = @handle.wait(max_wait_timeout_ms: nil)

        assert_equal "", report.rdkafka_response_string
      end
    end
  end

  describe "#raise_error" do
    before do
      @handle = described_class.new.tap do |handle|
        handle[:pending] = false
        handle[:response] = @response
        handle[:response_string] = FFI::MemoryPointer.from_string("")
      end
    end

    it "raises the appropriate error" do
      e = assert_raises(Rdkafka::RdkafkaError) { @handle.raise_error }
      assert_match(/Success \(no_error\)/, e.message)
    end
  end
end
