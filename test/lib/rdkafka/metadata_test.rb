# frozen_string_literal: true

require "test_helper"
require "securerandom"

class MetadataTest < Minitest::Test
  def setup
    super
    @config = rdkafka_consumer_config
    @native_config = @config.send(:native_config)
    @native_kafka = @config.send(:native_kafka, @native_config, :rd_kafka_consumer)
  end

  def teardown
    Rdkafka::Bindings.rd_kafka_consumer_close(@native_kafka)
    Rdkafka::Bindings.rd_kafka_destroy(@native_kafka)
    super
  end

  def test_raises_for_non_existent_topic
    topic_name = SecureRandom.uuid.to_s
    error = assert_raises(Rdkafka::RdkafkaError) do
      Rdkafka::Metadata.new(@native_kafka, topic_name)
    end
    assert_equal "Broker: Unknown topic or partition (unknown_topic_or_part)", error.message
  end

  def test_brokers_for_specific_topic
    topic_name = TestTopics.partitioner_test_topic
    metadata = Rdkafka::Metadata.new(@native_kafka, topic_name)

    assert_equal 1, metadata.brokers.length
    assert_equal 1, metadata.brokers[0][:broker_id]
    assert_includes %w[127.0.0.1 localhost], metadata.brokers[0][:broker_name]
    assert_equal rdkafka_base_config[:"bootstrap.servers"].split(":").last.to_i, metadata.brokers[0][:broker_port]
  end

  def test_topics_for_specific_topic
    topic_name = TestTopics.partitioner_test_topic
    metadata = Rdkafka::Metadata.new(@native_kafka, topic_name)

    assert_equal 1, metadata.topics.length
    assert_equal 25, metadata.topics[0][:partition_count]
    assert_equal 25, metadata.topics[0][:partitions].length
    assert_equal topic_name, metadata.topics[0][:topic_name]
  end

  def test_brokers_without_topic_name
    metadata = Rdkafka::Metadata.new(@native_kafka, nil)

    assert_equal 1, metadata.brokers.length
    assert_equal 1, metadata.brokers[0][:broker_id]
    assert_includes %w[127.0.0.1 localhost], metadata.brokers[0][:broker_name]
    assert_equal rdkafka_base_config[:"bootstrap.servers"].split(":").last.to_i, metadata.brokers[0][:broker_port]
  end

  def test_topics_returns_all_test_topics
    metadata = Rdkafka::Metadata.new(@native_kafka, nil)
    test_topics = [
      TestTopics.consume_test_topic, TestTopics.empty_test_topic, TestTopics.load_test_topic,
      TestTopics.produce_test_topic, TestTopics.rake_test_topic, TestTopics.watermarks_test_topic,
      TestTopics.partitioner_test_topic
    ]
    result = metadata.topics.map { |topic| topic[:topic_name] }

    test_topics.each do |t|
      assert_includes result, t
    end
  end

  def test_raises_on_non_zero_error_code
    topic_name = SecureRandom.uuid.to_s
    Rdkafka::Bindings.stub(:rd_kafka_metadata, -165) do
      error = assert_raises(Rdkafka::RdkafkaError) do
        Rdkafka::Metadata.new(@native_kafka, topic_name)
      end
      assert_match(/Local: Required feature not supported by broker \(unsupported_feature\)/, error.message)
    end
  end
end
