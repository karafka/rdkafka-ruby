# frozen_string_literal: true

describe Rdkafka::Producer::DeliveryReport do
  before do
    @topic_name = TestTopics.unique
    @report = described_class.new(2, 100, @topic_name, -1)
  end

  it "gets the partition" do
    assert_equal(2, @report.partition)
  end

  it "gets the offset" do
    assert_equal(100, @report.offset)
  end

  it "gets the topic_name" do
    assert_equal(@topic_name, @report.topic_name)
  end

  it "gets the same topic name under topic alias" do
    assert_equal(@topic_name, @report.topic)
  end

  it "gets the error" do
    assert_equal(-1, @report.error)
  end

  it "has no status, latency or broker id by default" do
    assert_nil(@report.status)
    assert_nil(@report.latency)
    assert_nil(@report.broker_id)
    refute_predicate(@report, :persisted?)
    refute_predicate(@report, :possibly_persisted?)
    refute_predicate(@report, :not_persisted?)
  end

  context "with status, latency and broker id" do
    before do
      @report = described_class.new(
        2,
        100,
        @topic_name,
        nil,
        nil,
        status: Rdkafka::Bindings::RD_KAFKA_MSG_STATUS_POSSIBLY_PERSISTED,
        latency: 1_500,
        broker_id: 3
      )
    end

    it "gets them" do
      assert_equal(Rdkafka::Bindings::RD_KAFKA_MSG_STATUS_POSSIBLY_PERSISTED, @report.status)
      assert_equal(1_500, @report.latency)
      assert_equal(3, @report.broker_id)
    end

    it "answers only the matching status predicate" do
      assert_predicate(@report, :possibly_persisted?)
      refute_predicate(@report, :persisted?)
      refute_predicate(@report, :not_persisted?)
    end
  end

  context "when librdkafka reports latency and broker id as unavailable" do
    before do
      @report = described_class.new(2, 100, @topic_name, nil, nil, latency: -1, broker_id: -1)
    end

    it "returns nil for both" do
      assert_nil(@report.latency)
      assert_nil(@report.broker_id)
    end
  end
end
