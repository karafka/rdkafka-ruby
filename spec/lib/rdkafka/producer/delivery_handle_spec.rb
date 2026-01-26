# frozen_string_literal: true

RSpec.describe Rdkafka::Producer::DeliveryHandle do
  subject do
    described_class.new.tap do |handle|
      handle[:pending] = pending_handle
      handle[:response] = response
      handle[:partition] = 2
      handle[:offset] = 100
      handle.topic = TestTopics.produce_test_topic
    end
  end

  let(:response) { 0 }

  describe "#wait" do
    let(:pending_handle) { true }

    it "waits until the timeout and then raise an error" do
      expect {
        subject.wait(max_wait_timeout_ms: 100)
      }.to raise_error Rdkafka::Producer::DeliveryHandle::WaitTimeoutError, /delivery/
    end

    context "when not pending anymore and no error" do
      let(:pending_handle) { false }

      it "returns a delivery report" do
        report = subject.wait

        expect(report.partition).to eq(2)
        expect(report.offset).to eq(100)
        expect(report.topic_name).to eq(TestTopics.produce_test_topic)
      end

      it "waits without a timeout" do
        report = subject.wait(max_wait_timeout_ms: nil)

        expect(report.partition).to eq(2)
        expect(report.offset).to eq(100)
        expect(report.topic_name).to eq(TestTopics.produce_test_topic)
      end
    end
  end

  describe "#create_result" do
    let(:pending_handle) { false }
    let(:report) { subject.create_result }

    context "when response is 0" do
      it { expect(report.error).to be_nil }
    end

    context "when response is not 0" do
      let(:response) { 1 }

      it { expect(report.error).to eq(Rdkafka::RdkafkaError.new(response)) }
    end
  end
end
