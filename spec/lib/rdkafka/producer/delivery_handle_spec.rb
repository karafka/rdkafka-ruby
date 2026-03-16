# frozen_string_literal: true

RSpec.describe Rdkafka::Producer::DeliveryHandle do
  def topic
    @topic ||= TestTopics.unique
  end

  def build_handle(pending:, response: 0)
    build_delivery_handle(pending: pending, response: response, topic: topic)
  end

  describe "#wait" do
    it "waits until the timeout and then raise an error" do
      handle = build_handle(pending: true)

      expect {
        handle.wait(max_wait_timeout_ms: 100)
      }.to raise_error Rdkafka::Producer::DeliveryHandle::WaitTimeoutError, /delivery/
    end

    context "when not pending anymore and no error" do
      it "returns a delivery report" do
        handle = build_handle(pending: false)
        report = handle.wait

        expect(report.partition).to eq(2)
        expect(report.offset).to eq(100)
        expect(report.topic_name).to eq(topic)
      end

      it "waits without a timeout" do
        handle = build_handle(pending: false)
        report = handle.wait(max_wait_timeout_ms: nil)

        expect(report.partition).to eq(2)
        expect(report.offset).to eq(100)
        expect(report.topic_name).to eq(topic)
      end
    end
  end

  describe "#create_result" do
    context "when response is 0" do
      it "has no error" do
        handle = build_handle(pending: false)
        report = handle.create_result

        expect(report.error).to be_nil
      end
    end

    context "when response is not 0" do
      it "has an error" do
        handle = build_handle(pending: false, response: 1)
        report = handle.create_result

        expect(report.error).to eq(Rdkafka::RdkafkaError.new(1))
      end
    end
  end
end
