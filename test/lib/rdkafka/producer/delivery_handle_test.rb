# frozen_string_literal: true

describe Rdkafka::Producer::DeliveryHandle do
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

    it "waits until the timeout and then raises an error" do
      error = assert_raises(Rdkafka::Producer::DeliveryHandle::WaitTimeoutError) do
        subject.wait(max_wait_timeout_ms: 100)
      end
      assert_match(/delivery/, error.message)
    end

    describe "when not pending anymore and no error" do
      let(:pending_handle) { false }

      it "returns a delivery report" do
        report = subject.wait

        assert_equal 2, report.partition
        assert_equal 100, report.offset
        assert_equal TestTopics.produce_test_topic, report.topic_name
      end

      it "waits without a timeout" do
        report = subject.wait(max_wait_timeout_ms: nil)

        assert_equal 2, report.partition
        assert_equal 100, report.offset
        assert_equal TestTopics.produce_test_topic, report.topic_name
      end
    end
  end

  describe "#create_result" do
    let(:pending_handle) { false }

    describe "when response is 0" do
      it "has no error" do
        report = subject.create_result

        assert_nil report.error
      end
    end

    describe "when response is not 0" do
      let(:response) { 1 }

      it "has an error" do
        report = subject.create_result

        assert_equal Rdkafka::RdkafkaError.new(response), report.error
      end
    end
  end
end
