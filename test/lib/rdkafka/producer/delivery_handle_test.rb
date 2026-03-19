# frozen_string_literal: true

require_relative "../../../test_helper"

describe Rdkafka::Producer::DeliveryHandle do
  describe "#wait" do
    before do
      @handle = described_class.new.tap do |handle|
        handle[:pending] = true
        handle[:response] = 0
        handle[:partition] = 2
        handle[:offset] = 100
        handle.topic = TestTopics.unique
      end
    end

    it "waits until the timeout and then raise an error" do
      e = assert_raises(Rdkafka::Producer::DeliveryHandle::WaitTimeoutError) {
        @handle.wait(max_wait_timeout_ms: 100)
      }
      assert_match(/delivery/, e.message)
    end

    context "when not pending anymore and no error" do
      before do
        @handle[:pending] = false
      end

      it "returns a delivery report" do
        report = @handle.wait

        assert_equal 2, report.partition
        assert_equal 100, report.offset
        assert_equal @handle.topic, report.topic_name
      end

      it "waits without a timeout" do
        report = @handle.wait(max_wait_timeout_ms: nil)

        assert_equal 2, report.partition
        assert_equal 100, report.offset
        assert_equal @handle.topic, report.topic_name
      end
    end
  end

  describe "#create_result" do
    before do
      @response = 0
      @handle = described_class.new.tap do |handle|
        handle[:pending] = false
        handle[:response] = 0
        handle[:partition] = 2
        handle[:offset] = 100
        handle.topic = TestTopics.unique
      end
    end

    context "when response is 0" do
      it "has nil error" do
        report = @handle.create_result
        assert_nil report.error
      end
    end

    context "when response is not 0" do
      before do
        @handle[:response] = 1
      end

      it "has the appropriate error" do
        report = @handle.create_result
        assert_equal Rdkafka::RdkafkaError.new(1), report.error
      end
    end
  end
end
