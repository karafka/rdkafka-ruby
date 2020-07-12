require "spec_helper"
require "securerandom"

describe Rdkafka::Metadata do
  let(:config)        { rdkafka_config }
  let(:native_config) { config.send(:native_config) }
  let(:native_kafka)  { config.send(:native_kafka, native_config, :rd_kafka_consumer) }

  after do
    Rdkafka::Bindings.rd_kafka_consumer_close(native_kafka)
    Rdkafka::Bindings.rd_kafka_destroy(native_kafka)
  end

  subject { described_class.new(native_kafka, topic_name) }

  context "with a topic" do

    context "with a non-existent topic" do
      let(:topic_name) { SecureRandom.uuid.to_s }

      it "#brokers" do
        expect(subject.brokers.length).to eq(1)
        expect(subject.brokers[0][:broker_id]).to eq(1)
        expect(subject.brokers[0][:broker_name]).to eq("localhost")
        expect(subject.brokers[0][:broker_port]).to eq(9092)
      end

      it "#topics" do
        expect(subject.topics[0][:partition_count]).to eq(0)
        expect(subject.topics[0][:partitions]).to eq([])
        expect(subject.topics[0][:topic_name]).to eq(topic_name)
      end
    end

    context "with an existing topic" do
      let(:topic_name) { "partitioner_test_topic" }

      it "#brokers" do
        expect(subject.brokers.length).to eq(1)
        expect(subject.brokers[0][:broker_id]).to eq(1)
        expect(subject.brokers[0][:broker_name]).to eq("localhost")
        expect(subject.brokers[0][:broker_port]).to eq(9092)
      end

      it "#topics" do
        expect(subject.topics[0][:partition_count]).to eq(25)
        expect(subject.topics[0][:partitions].length).to eq(25)
        expect(subject.topics[0][:topic_name]).to eq(topic_name)
      end
    end
  end

  context "without a topic" do
    let(:topic_name) { nil }
    let(:test_topics) {
      %w(consume_test_topic empty_test_topic load_test_topic produce_test_topic rake_test_topic watermarks_test_topic partitioner_test_topic)
    } # Test topics crated in spec_helper.rb

    it "#brokers" do
      expect(subject.brokers.length).to eq(1)
      expect(subject.brokers[0][:broker_id]).to eq(1)
      expect(subject.brokers[0][:broker_name]).to eq("localhost")
      expect(subject.brokers[0][:broker_port]).to eq(9092)
    end

    it "#topics" do
      result = subject.topics.map { |topic| topic[:topic_name] }
      expect(result).to include(*test_topics)
    end
  end

  context "when a non-zero error code is returned" do
    let(:topic_name) { SecureRandom.uuid.to_s }

    before do
      allow(Rdkafka::Bindings).to receive(:rd_kafka_metadata).and_return(-165)
    end

    it "#brokers" do
      expect {
        described_class.new(native_kafka, topic_name)
      }.to raise_error(Rdkafka::RdkafkaError, /Local: Required feature not supported by broker \(unsupported_feature\)/)
    end
  end
end
