require "spec_helper"

describe Rdkafka::Admin::DeleteTopicHandle do
  let(:response) { 0 }

  subject do
    Rdkafka::Admin::DeleteTopicHandle.new.tap do |handle|
      handle[:pending] = pending_handle
      handle[:response] = response
      handle[:error_string] = FFI::Pointer::NULL
      handle[:result_name] = FFI::MemoryPointer.from_string("my-test-topic")
    end
  end

  describe "#wait" do
    let(:pending_handle) { true }

    it "should wait until the timeout and then raise an error" do
      expect {
        subject.wait(max_wait_timeout: 0.1)
      }.to raise_error Rdkafka::Admin::DeleteTopicHandle::WaitTimeoutError, /delete topic/
    end

    context "when not pending anymore and no error" do
      let(:pending_handle) { false }

      it "should return a delete topic report" do
        report = subject.wait

        expect(report.error_string).to eq(nil)
        expect(report.result_name).to eq("my-test-topic")
      end

      it "should wait without a timeout" do
        report = subject.wait(max_wait_timeout: nil)

        expect(report.error_string).to eq(nil)
        expect(report.result_name).to eq("my-test-topic")
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
