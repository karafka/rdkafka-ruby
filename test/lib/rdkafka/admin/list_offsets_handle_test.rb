# frozen_string_literal: true

describe Rdkafka::Admin::ListOffsetsHandle do
  let(:response) { Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR }

  subject do
    Rdkafka::Admin::ListOffsetsHandle.new.tap do |handle|
      handle[:pending] = pending_handle
      handle[:response] = response
      handle[:response_string] = FFI::MemoryPointer.from_string("")
      handle[:result_infos] = FFI::Pointer::NULL
      handle[:result_count] = 0
    end
  end

  describe "#wait" do
    let(:pending_handle) { true }

    it "waits until the timeout and then raises an error" do
      error = assert_raises(Rdkafka::Admin::ListOffsetsHandle::WaitTimeoutError) do
        subject.wait(max_wait_timeout_ms: 100)
      end
      assert_match(/list offsets/, error.message)
    end

    describe "when not pending anymore and no error" do
      let(:pending_handle) { false }

      it "returns a list offsets report" do
        report = subject.wait

        assert_kind_of Rdkafka::Admin::ListOffsetsReport, report
        assert_equal [], report.offsets
      end

      it "waits without a timeout" do
        report = subject.wait(max_wait_timeout_ms: nil)

        assert_equal [], report.offsets
      end
    end
  end

  describe "#raise_error" do
    let(:pending_handle) { false }

    it "raises the appropriate error" do
      error = assert_raises(Rdkafka::RdkafkaError) { subject.raise_error }
      assert_match(/Success \(no_error\)/, error.message)
    end
  end
end
