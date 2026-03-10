# frozen_string_literal: true

require "securerandom"

describe Rdkafka::Metadata do
  let(:config) { rdkafka_consumer_config }
  let(:native_config) { config.send(:native_config) }
  let(:native_kafka) { config.send(:native_kafka, native_config, :rd_kafka_consumer) }

  after do
    Rdkafka::Bindings.rd_kafka_consumer_close(native_kafka)
    Rdkafka::Bindings.rd_kafka_destroy(native_kafka)
  end

  describe "passing in a topic name" do
    describe "that is non-existent topic" do
      let(:topic_name) { SecureRandom.uuid.to_s }

      it "raises an appropriate exception" do
        error = assert_raises(Rdkafka::RdkafkaError) do
          Rdkafka::Metadata.new(native_kafka, topic_name)
        end
        assert_equal "Broker: Unknown topic or partition (unknown_topic_or_part)", error.message
      end
    end

    describe "that is one of our test topics" do
      subject { Rdkafka::Metadata.new(native_kafka, topic_name) }

      let(:topic_name) { create_topic_for_test(partitions: 25) }

      it "#brokers returns our single broker" do
        assert_equal 1, subject.brokers.length
        assert_equal 1, subject.brokers[0][:broker_id]
        assert_includes %w[127.0.0.1 localhost], subject.brokers[0][:broker_name]
        assert_equal rdkafka_base_config[:"bootstrap.servers"].split(":").last.to_i, subject.brokers[0][:broker_port]
      end

      it "#topics returns data on our test topic" do
        assert_equal 1, subject.topics.length
        assert_equal 25, subject.topics[0][:partition_count]
        assert_equal 25, subject.topics[0][:partitions].length
        assert_equal topic_name, subject.topics[0][:topic_name]
      end
    end
  end

  describe "not passing in a topic name" do
    subject { Rdkafka::Metadata.new(native_kafka, topic_name) }

    let(:topic_name) { nil }

    it "#brokers returns our single broker" do
      assert_equal 1, subject.brokers.length
      assert_equal 1, subject.brokers[0][:broker_id]
      assert_includes %w[127.0.0.1 localhost], subject.brokers[0][:broker_name]
      assert_equal rdkafka_base_config[:"bootstrap.servers"].split(":").last.to_i, subject.brokers[0][:broker_port]
    end

    it "#topics returns data about existing topics" do
      result = subject.topics.map { |topic| topic[:topic_name] }

      assert_includes result, TestTopics.example_topic
    end
  end

  describe "when a non-zero error code is returned" do
    let(:topic_name) { SecureRandom.uuid.to_s }

    it "creating the instance raises an exception" do
      Rdkafka::Bindings.stub(:rd_kafka_metadata, -165) do
        error = assert_raises(Rdkafka::RdkafkaError) do
          Rdkafka::Metadata.new(native_kafka, topic_name)
        end
        assert_match(/Local: Required feature not supported by broker \(unsupported_feature\)/, error.message)
      end
    end
  end
end
