# frozen_string_literal: true

require "securerandom"

RSpec.describe Rdkafka::Metadata do
  def config
    @config ||= rdkafka_consumer_config
  end

  def native_config
    @native_config ||= config.send(:native_config)
  end

  def native_kafka
    @native_kafka ||= config.send(:native_kafka, native_config, :rd_kafka_consumer)
  end

  after do
    Rdkafka::Bindings.rd_kafka_consumer_close(native_kafka)
    Rdkafka::Bindings.rd_kafka_destroy(native_kafka)
  end

  context "passing in a topic name" do
    context "that is non-existent topic" do
      it "raises an appropriate exception" do
        topic_name = SecureRandom.uuid.to_s

        expect {
          described_class.new(native_kafka, topic_name)
        }.to raise_exception(Rdkafka::RdkafkaError, "Broker: Unknown topic or partition (unknown_topic_or_part)")
      end
    end

    context "that is one of our test topics" do
      it "#brokers returns our single broker" do
        topic_name = TestTopics.create(partitions: 25)
        metadata = described_class.new(native_kafka, topic_name)

        expect(metadata.brokers.length).to eq(1)
        expect(metadata.brokers[0][:broker_id]).to eq(1)
        expect(%w[127.0.0.1 localhost]).to include(metadata.brokers[0][:broker_name])
        expect(metadata.brokers[0][:broker_port]).to eq(rdkafka_base_config[:"bootstrap.servers"].split(":").last.to_i)
      end

      it "#topics returns data on our test topic" do
        topic_name = TestTopics.create(partitions: 25)
        metadata = described_class.new(native_kafka, topic_name)

        expect(metadata.topics.length).to eq(1)
        expect(metadata.topics[0][:partition_count]).to eq(25)
        expect(metadata.topics[0][:partitions].length).to eq(25)
        expect(metadata.topics[0][:topic_name]).to eq(topic_name)
      end
    end
  end

  context "not passing in a topic name" do
    it "#brokers returns our single broker" do
      metadata = described_class.new(native_kafka, nil)

      expect(metadata.brokers.length).to eq(1)
      expect(metadata.brokers[0][:broker_id]).to eq(1)
      expect(%w[127.0.0.1 localhost]).to include(metadata.brokers[0][:broker_name])
      expect(metadata.brokers[0][:broker_port]).to eq(rdkafka_base_config[:"bootstrap.servers"].split(":").last.to_i)
    end

    it "#topics returns data about existing topics" do
      test_topic = TestTopics.create
      # Force topic creation before querying metadata
      metadata = described_class.new(native_kafka, nil)
      result = metadata.topics.map { |topic| topic[:topic_name] }
      expect(result).to include(test_topic)
    end
  end

  context "when a non-zero error code is returned" do
    before do
      stub_binding_return(:rd_kafka_metadata, -165)
    end

    it "creating the instance raises an exception" do
      topic_name = SecureRandom.uuid.to_s

      expect {
        described_class.new(native_kafka, topic_name)
      }.to raise_error(Rdkafka::RdkafkaError, /Local: Required feature not supported by broker \(unsupported_feature\)/)
    end
  end
end
