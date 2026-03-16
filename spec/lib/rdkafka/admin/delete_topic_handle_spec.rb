# frozen_string_literal: true

RSpec.describe Rdkafka::Admin::DeleteTopicHandle do
  def topic_name
    @topic_name ||= TestTopics.unique
  end

  def build_handle(pending:, response: 0)
    build_topic_handle(Rdkafka::Admin::DeleteTopicHandle, pending: pending, response: response, topic_name: topic_name)
  end

  describe "#wait" do
    it "waits until the timeout and then raise an error" do
      handle = build_handle(pending: true)

      expect {
        handle.wait(max_wait_timeout_ms: 100)
      }.to raise_error Rdkafka::Admin::DeleteTopicHandle::WaitTimeoutError, /delete topic/
    end

    context "when not pending anymore and no error" do
      it "returns a delete topic report" do
        handle = build_handle(pending: false)
        report = handle.wait

        expect(report.error_string).to be_nil
        expect(report.result_name).to eq(topic_name)
      end

      it "waits without a timeout" do
        handle = build_handle(pending: false)
        report = handle.wait(max_wait_timeout_ms: nil)

        expect(report.error_string).to be_nil
        expect(report.result_name).to eq(topic_name)
      end
    end
  end

  describe "#raise_error" do
    it "raises the appropriate error" do
      handle = build_handle(pending: false, response: -1)

      expect {
        handle.raise_error
      }.to raise_exception(Rdkafka::RdkafkaError, /Unknown broker error \(unknown\)/)
    end
  end
end
