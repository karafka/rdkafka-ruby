# frozen_string_literal: true

require_relative "../../test_helper"
require "securerandom"

describe Rdkafka::Metadata do
  before do
    @config = rdkafka_consumer_config
    @native_config = @config.send(:native_config)
    @native_kafka = @config.send(:native_kafka, @native_config, :rd_kafka_consumer)
  end

  after do
    Rdkafka::Bindings.rd_kafka_consumer_close(@native_kafka)
    Rdkafka::Bindings.rd_kafka_destroy(@native_kafka)
  end

  context "passing in a topic name" do
    context "that is non-existent topic" do
      it "raises an appropriate exception" do
        topic_name = SecureRandom.uuid.to_s
        e = assert_raises(Rdkafka::RdkafkaError) do
          described_class.new(@native_kafka, topic_name)
        end
        assert_match(/Broker: Unknown topic or partition \(unknown_topic_or_part\)/, e.message)
      end
    end

    context "that is one of our test topics" do
      before do
        @topic_name = TestTopics.create(partitions: 25)
        @metadata = described_class.new(@native_kafka, @topic_name)
      end

      it "#brokers returns our single broker" do
        assert_equal 1, @metadata.brokers.length
        assert_equal 1, @metadata.brokers[0][:broker_id]
        assert_includes %w[127.0.0.1 localhost], @metadata.brokers[0][:broker_name]
        assert_equal rdkafka_base_config[:"bootstrap.servers"].split(":").last.to_i, @metadata.brokers[0][:broker_port]
      end

      it "#topics returns data on our test topic" do
        assert_equal 1, @metadata.topics.length
        assert_equal 25, @metadata.topics[0][:partition_count]
        assert_equal 25, @metadata.topics[0][:partitions].length
        assert_equal @topic_name, @metadata.topics[0][:topic_name]
      end
    end
  end

  context "not passing in a topic name" do
    it "#brokers returns our single broker" do
      metadata = described_class.new(@native_kafka, nil)
      assert_equal 1, metadata.brokers.length
      assert_equal 1, metadata.brokers[0][:broker_id]
      assert_includes %w[127.0.0.1 localhost], metadata.brokers[0][:broker_name]
      assert_equal rdkafka_base_config[:"bootstrap.servers"].split(":").last.to_i, metadata.brokers[0][:broker_port]
    end

    it "#topics returns data about existing topics" do
      # Force topic creation before querying metadata
      test_topic = TestTopics.create
      metadata = described_class.new(@native_kafka, nil)
      result = metadata.topics.map { |topic| topic[:topic_name] }
      assert_includes result, test_topic
    end
  end

  context "when a non-zero error code is returned" do
    it "creating the instance raises an exception" do
      topic_name = SecureRandom.uuid.to_s
      Rdkafka::Bindings.stubs(:rd_kafka_metadata).returns(-165)

      e = assert_raises(Rdkafka::RdkafkaError) do
        described_class.new(@native_kafka, topic_name)
      end
      assert_match(/Local: Required feature not supported by broker \(unsupported_feature\)/, e.message)
    end
  end
end
