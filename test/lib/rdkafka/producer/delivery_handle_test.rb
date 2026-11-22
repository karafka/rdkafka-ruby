# frozen_string_literal: true

describe Rdkafka::Producer::DeliveryHandle do
  before { @response = 0 }

  def build_handle(pending_handle)
    described_class.new.tap do |handle|
      handle[:pending] = pending_handle
      handle[:response] = @response
      handle[:partition] = 2
      handle[:offset] = 100
      handle.topic = TestTopics.unique
    end
  end

  describe "#wait" do
    before { @handle = build_handle(true) }

    it "waits until the timeout and then raise an error" do
      error = assert_raises(Rdkafka::Producer::DeliveryHandle::WaitTimeoutError) do
        @handle.wait(max_wait_timeout_ms: 100)
      end

      assert_match(/delivery/, error.message)
    end

    context "when not pending anymore and no error" do
      before { @handle = build_handle(false) }

      it "returns a delivery report" do
        report = @handle.wait

        assert_equal(2, report.partition)
        assert_equal(100, report.offset)
        assert_equal(@handle.topic, report.topic_name)
      end

      it "waits without a timeout" do
        report = @handle.wait(max_wait_timeout_ms: nil)

        assert_equal(2, report.partition)
        assert_equal(100, report.offset)
        assert_equal(@handle.topic, report.topic_name)
      end
    end
  end

  describe "#create_result" do
    context "when response is 0" do
      before { @handle = build_handle(false) }

      it "has no error" do
        assert_nil(@handle.create_result.error)
      end
    end

    context "when response is not 0" do
      before do
        @response = 1
        @handle = build_handle(false)
      end

      it "has an error matching the response" do
        assert_equal(Rdkafka::RdkafkaError.new(@response), @handle.create_result.error)
      end
    end

    context "when the delivery callback set status, latency and broker id" do
      before do
        @handle = build_handle(false)
        @handle[:status] = Rdkafka::Bindings::RD_KAFKA_MSG_STATUS_PERSISTED
        @handle[:latency] = 2_000
        @handle[:broker_id] = 1
      end

      it "passes them to the report" do
        report = @handle.create_result

        assert_equal(Rdkafka::Bindings::RD_KAFKA_MSG_STATUS_PERSISTED, report.status)
        assert_equal(2_000, report.latency)
        assert_equal(1, report.broker_id)
      end
    end
  end
end
