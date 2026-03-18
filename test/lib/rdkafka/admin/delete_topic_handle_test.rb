# frozen_string_literal: true

require_relative "../../../test_helper"

describe Rdkafka::Admin::DeleteTopicHandle do
  before do
    @topic_name = TestTopics.unique
  end

  describe "#wait" do
    context "when pending" do
      before do
        @handle = described_class.new.tap do |handle|
          handle[:pending] = true
          handle[:response] = 0
          handle[:error_string] = FFI::Pointer::NULL
          handle[:result_name] = FFI::MemoryPointer.from_string(@topic_name)
        end
      end

      it "waits until the timeout and then raise an error" do
        e = assert_raises(Rdkafka::Admin::DeleteTopicHandle::WaitTimeoutError) {
          @handle.wait(max_wait_timeout_ms: 100)
        }
        assert_match(/delete topic/, e.message)
      end
    end

    context "when not pending anymore and no error" do
      before do
        @handle = described_class.new.tap do |handle|
          handle[:pending] = false
          handle[:response] = 0
          handle[:error_string] = FFI::Pointer::NULL
          handle[:result_name] = FFI::MemoryPointer.from_string(@topic_name)
        end
      end

      it "returns a delete topic report" do
        report = @handle.wait

        assert_nil report.error_string
        assert_equal @topic_name, report.result_name
      end

      it "waits without a timeout" do
        report = @handle.wait(max_wait_timeout_ms: nil)

        assert_nil report.error_string
        assert_equal @topic_name, report.result_name
      end
    end
  end

  describe "#raise_error" do
    before do
      @handle = described_class.new.tap do |handle|
        handle[:pending] = false
        handle[:response] = -1
        handle[:error_string] = FFI::Pointer::NULL
        handle[:result_name] = FFI::MemoryPointer.from_string(@topic_name)
      end
    end

    it "raises the appropriate error" do
      e = assert_raises(Rdkafka::RdkafkaError) { @handle.raise_error }
      assert_match(/Unknown broker error \(unknown\)/, e.message)
    end
  end
end
