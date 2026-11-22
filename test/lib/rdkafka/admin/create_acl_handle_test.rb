# frozen_string_literal: true

describe Rdkafka::Admin::CreateAclHandle do
  # If create acl was successful there is no error object
  # the error code is set to RD_KAFKA_RESP_ERR_NO_ERRORa
  # https://github.com/confluentinc/librdkafka/blob/1f9f245ac409f50f724695c628c7a0d54a763b9a/src/rdkafka_error.c#L169
  before { @response = Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR }

  def build_handle(pending_handle)
    described_class.new.tap do |handle|
      handle[:pending] = pending_handle
      handle[:response] = @response
      # If create acl was successful there is no error object and the error_string is set to ""
      # https://github.com/confluentinc/librdkafka/blob/1f9f245ac409f50f724695c628c7a0d54a763b9a/src/rdkafka_error.c#L178
      handle.result = Rdkafka::Admin::CreateAclReport.new(
        rdkafka_response: @response,
        rdkafka_response_string: FFI::MemoryPointer.from_string("")
      )
      handle.broker_message = ""
    end
  end

  describe "#wait" do
    before { @handle = build_handle(true) }

    it "waits until the timeout and then raise an error" do
      error = assert_raises(Rdkafka::Admin::CreateAclHandle::WaitTimeoutError) do
        @handle.wait(max_wait_timeout_ms: 100)
      end

      assert_match(/create acl/, error.message)
    end

    context "when not pending anymore and no error" do
      before { @handle = build_handle(false) }

      it "returns a create acl report" do
        report = @handle.wait

        assert_equal("", report.rdkafka_response_string)
      end

      it "waits without a timeout" do
        report = @handle.wait(max_wait_timeout_ms: nil)

        assert_equal("", report.rdkafka_response_string)
      end
    end
  end

  describe "#raise_error" do
    before { @handle = build_handle(false) }

    it "raises the appropriate error" do
      error = assert_raises(Rdkafka::RdkafkaError) { @handle.raise_error }

      assert_match(/Success \(no_error\)/, error.message)
    end
  end
end
