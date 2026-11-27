# frozen_string_literal: true

describe Rdkafka::Admin::DeleteTopicHandle do
  before do
    @response = 0
    @topic_name = TestTopics.unique
  end

  def build_handle(pending_handle)
    described_class.new.tap do |handle|
      handle[:pending] = pending_handle
      handle[:response] = @response
      handle.result = Rdkafka::Admin::DeleteTopicReport.new(
        FFI::Pointer::NULL,
        FFI::MemoryPointer.from_string(@topic_name)
      )
    end
  end

  describe "#wait" do
    before { @handle = build_handle(true) }

    it "waits until the timeout and then raise an error" do
      error = assert_raises(Rdkafka::Admin::DeleteTopicHandle::WaitTimeoutError) do
        @handle.wait(max_wait_timeout_ms: 100)
      end

      assert_match(/delete topic/, error.message)
    end

    context "when not pending anymore and no error" do
      before { @handle = build_handle(false) }

      it "returns a delete topic report" do
        report = @handle.wait

        assert_nil(report.error_string)
        assert_equal(@topic_name, report.result_name)
      end

      it "waits without a timeout" do
        report = @handle.wait(max_wait_timeout_ms: nil)

        assert_nil(report.error_string)
        assert_equal(@topic_name, report.result_name)
      end
    end
  end

  describe "#raise_error" do
    before do
      @handle = build_handle(false)
      @handle[:response] = -1
    end

    it "raises the appropriate error" do
      error = assert_raises(Rdkafka::RdkafkaError) { @handle.raise_error }

      assert_match(/Unknown broker error \(unknown\)/, error.message)
    end
  end
end
