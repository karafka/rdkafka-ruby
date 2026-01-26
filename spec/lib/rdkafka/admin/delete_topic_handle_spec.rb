# frozen_string_literal: true

RSpec.describe Rdkafka::Admin::DeleteTopicHandle do
  subject do
    described_class.new.tap do |handle|
      handle[:pending] = pending_handle
      handle[:response] = response
      handle[:error_string] = FFI::Pointer::NULL
      handle[:result_name] = FFI::MemoryPointer.from_string(topic_name)
    end
  end

  let(:response) { 0 }
  let(:topic_name) { TestTopics.unique }

  describe "#wait" do
    let(:pending_handle) { true }

    it "waits until the timeout and then raise an error" do
      expect {
        subject.wait(max_wait_timeout_ms: 100)
      }.to raise_error Rdkafka::Admin::DeleteTopicHandle::WaitTimeoutError, /delete topic/
    end

    context "when not pending anymore and no error" do
      let(:pending_handle) { false }

      it "returns a delete topic report" do
        report = subject.wait

        expect(report.error_string).to be_nil
        expect(report.result_name).to eq(topic_name)
      end

      it "waits without a timeout" do
        report = subject.wait(max_wait_timeout_ms: nil)

        expect(report.error_string).to be_nil
        expect(report.result_name).to eq(topic_name)
      end
    end
  end

  describe "#raise_error" do
    let(:pending_handle) { false }

    before { subject[:response] = -1 }

    it "raises the appropriate error" do
      expect {
        subject.raise_error
      }.to raise_exception(Rdkafka::RdkafkaError, /Unknown broker error \(unknown\)/)
    end
  end
end
