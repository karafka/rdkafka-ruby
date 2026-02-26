# frozen_string_literal: true

RSpec.describe Rdkafka::Admin::ListOffsetsHandle do
  subject do
    described_class.new.tap do |handle|
      handle[:pending] = pending_handle
      handle[:response] = response
      handle[:response_string] = FFI::MemoryPointer.from_string("")
      handle[:result_infos] = FFI::Pointer::NULL
      handle[:result_count] = 0
    end
  end

  let(:response) { Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR }

  describe "#wait" do
    let(:pending_handle) { true }

    it "waits until the timeout and then raises an error" do
      expect {
        subject.wait(max_wait_timeout_ms: 100)
      }.to raise_error Rdkafka::Admin::ListOffsetsHandle::WaitTimeoutError, /list offsets/
    end

    context "when not pending anymore and no error" do
      let(:pending_handle) { false }

      it "returns a list offsets report" do
        report = subject.wait

        expect(report).to be_a(Rdkafka::Admin::ListOffsetsReport)
        expect(report.offsets).to eq([])
      end

      it "waits without a timeout" do
        report = subject.wait(max_wait_timeout_ms: nil)

        expect(report.offsets).to eq([])
      end
    end
  end

  describe "#raise_error" do
    let(:pending_handle) { false }

    it "raises the appropriate error" do
      expect {
        subject.raise_error
      }.to raise_exception(Rdkafka::RdkafkaError, /Success \(no_error\)/)
    end
  end
end
