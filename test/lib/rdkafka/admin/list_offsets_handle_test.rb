# frozen_string_literal: true

describe Rdkafka::Admin::ListOffsetsHandle do
  before { @response = Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR }

  def build_handle(pending_handle)
    described_class.new.tap do |handle|
      handle[:pending] = pending_handle
      handle[:response] = @response
      handle.broker_message = ""
      handle.result = Rdkafka::Admin::ListOffsetsReport.new(
        result_infos: FFI::Pointer::NULL,
        result_count: 0
      )
    end
  end

  describe "#wait" do
    before { @handle = build_handle(true) }

    it "waits until the timeout and then raises an error" do
      error = assert_raises(Rdkafka::Admin::ListOffsetsHandle::WaitTimeoutError) do
        @handle.wait(max_wait_timeout_ms: 100)
      end

      assert_match(/list offsets/, error.message)
    end

    context "when not pending anymore and no error" do
      before { @handle = build_handle(false) }

      it "returns a list offsets report" do
        report = @handle.wait

        assert_kind_of(Rdkafka::Admin::ListOffsetsReport, report)
        assert_equal([], report.offsets)
      end

      it "waits without a timeout" do
        report = @handle.wait(max_wait_timeout_ms: nil)

        assert_equal([], report.offsets)
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
