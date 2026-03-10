# frozen_string_literal: true

describe Rdkafka::Admin::DeleteTopicHandle do
  let(:response) { 0 }
  let(:topic_name) { TestTopics.unique }

  subject do
    Rdkafka::Admin::DeleteTopicHandle.new.tap do |handle|
      handle[:pending] = pending_handle
      handle[:response] = response
      handle[:error_string] = FFI::Pointer::NULL
      handle[:result_name] = FFI::MemoryPointer.from_string(topic_name)
    end
  end

  describe "#wait" do
    let(:pending_handle) { true }

    it "waits until the timeout and then raises an error" do
      error = assert_raises(Rdkafka::Admin::DeleteTopicHandle::WaitTimeoutError) do
        subject.wait(max_wait_timeout_ms: 100)
      end
      assert_match(/delete topic/, error.message)
    end

    describe "when not pending anymore and no error" do
      let(:pending_handle) { false }

      it "returns a delete topic report" do
        report = subject.wait

        assert_nil report.error_string
        assert_equal topic_name, report.result_name
      end

      it "waits without a timeout" do
        report = subject.wait(max_wait_timeout_ms: nil)

        assert_nil report.error_string
        assert_equal topic_name, report.result_name
      end
    end
  end

  describe "#raise_error" do
    let(:pending_handle) { false }

    before { subject[:response] = -1 }

    it "raises the appropriate error" do
      error = assert_raises(Rdkafka::RdkafkaError) { subject.raise_error }
      assert_match(/Unknown broker error \(unknown\)/, error.message)
    end
  end
end
