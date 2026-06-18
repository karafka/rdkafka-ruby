# frozen_string_literal: true

require "securerandom"

RSpec.describe Rdkafka::Metadata do
  let(:config) { rdkafka_consumer_config }
  let(:native_config) { config.send(:native_config) }
  let(:native_kafka) { config.send(:native_kafka, native_config, :rd_kafka_consumer) }

  after do
    Rdkafka::Bindings.rd_kafka_consumer_close(native_kafka)
    Rdkafka::Bindings.rd_kafka_destroy(native_kafka)
  end

  context "passing in a topic name" do
    context "that is non-existent topic" do
      let(:topic_name) { TestTopics.non_existing }

      it "raises an appropriate exception" do
        expect {
          described_class.new(native_kafka, topic_name)
        }.to raise_exception(Rdkafka::RdkafkaError, "Broker: Unknown topic or partition (unknown_topic_or_part)")
      end
    end

    context "that is one of our test topics" do
      let(:metadata) { described_class.new(native_kafka, topic_name) }

      let(:topic_name) { TestTopics.create(partitions: 25) }

      it "#brokers returns our single broker" do
        expect(metadata.brokers.length).to eq(1)
        expect(metadata.brokers[0][:broker_id]).to eq(1)
        expect(%w[127.0.0.1 localhost]).to include(metadata.brokers[0][:broker_name])
        expect(metadata.brokers[0][:broker_port]).to eq(rdkafka_base_config[:"bootstrap.servers"].split(":").last.to_i)
      end

      it "#topics returns data on our test topic" do
        expect(metadata.topics.length).to eq(1)
        expect(metadata.topics[0][:partition_count]).to eq(25)
        expect(metadata.topics[0][:partitions].length).to eq(25)
        expect(metadata.topics[0][:topic_name]).to eq(topic_name)
      end

      it "#topics exposes the replica and in-sync replica broker ids per partition" do
        metadata.topics[0][:partitions].each do |partition|
          expect(partition[:replicas]).to eq([1])
          expect(partition[:isrs]).to eq([1])
          expect(partition[:replica_count]).to eq(1)
        end
      end
    end
  end

  context "not passing in a topic name" do
    let(:metadata) { described_class.new(native_kafka, topic_name) }

    let(:topic_name) { nil }
    let(:test_topic) { TestTopics.create }

    it "#brokers returns our single broker" do
      expect(metadata.brokers.length).to eq(1)
      expect(metadata.brokers[0][:broker_id]).to eq(1)
      expect(%w[127.0.0.1 localhost]).to include(metadata.brokers[0][:broker_name])
      expect(metadata.brokers[0][:broker_port]).to eq(rdkafka_base_config[:"bootstrap.servers"].split(":").last.to_i)
    end

    it "#topics returns data about existing topics" do
      # Force topic creation before querying metadata
      test_topic
      result = metadata.topics.map { |topic| topic[:topic_name] }
      expect(result).to include(test_topic)
    end
  end

  context "when a non-zero error code is returned" do
    let(:topic_name) { TestTopics.unique }

    before do
      allow(Rdkafka::Bindings).to receive(:rd_kafka_metadata).and_return(-165)
    end

    it "creating the instance raises an exception" do
      expect {
        described_class.new(native_kafka, topic_name)
      }.to raise_error(Rdkafka::RdkafkaError, /Local: Required feature not supported by broker \(unsupported_feature\)/)
    end
  end

  context "when the fetch is retried" do
    let(:topic_name) { TestTopics.unique }

    # Builds a minimal native metadata struct carrying a single topic with the given response
    # error, so the real parse path raises that error after rd_kafka_metadata "allocated" it.
    # Returns the backing pointers (kept referenced so they are not garbage collected).
    def build_metadata_with_topic_error(resp_err)
      topic_buf = FFI::MemoryPointer.new(Rdkafka::Metadata::TopicMetadata.size)
      topic = Rdkafka::Metadata::TopicMetadata.new(topic_buf)
      topic[:partition_count] = 0
      topic[:rd_kafka_resp_err] = resp_err

      meta_buf = FFI::MemoryPointer.new(Rdkafka::Metadata::Metadata.size)
      meta = Rdkafka::Metadata::Metadata.new(meta_buf)
      meta[:brokers_count] = 0
      meta[:topics_count] = 1
      meta[:topics_metadata] = topic_buf

      [meta_buf, topic_buf]
    end

    before do
      # Make the backoff zero so sleep is instant between retries.
      stub_const("Rdkafka::Defaults::METADATA_RETRY_BACKOFF_BASE_MS", 0)
      allow(Rdkafka::Bindings).to receive(:rd_kafka_topic_new).and_return(FFI::MemoryPointer.new(:int))
      allow(Rdkafka::Bindings).to receive(:rd_kafka_topic_destroy)
      allow(Rdkafka::Bindings).to receive(:rd_kafka_metadata_destroy)
    end

    it "destroys every attempt's native topic handle and metadata struct, not just the last" do
      # leader_not_available (code 5) is retried and surfaces from the parse step, which only runs
      # after rd_kafka_metadata has already allocated the struct.
      err_meta, _err_topic = build_metadata_with_topic_error(5)
      ok_meta = FFI::MemoryPointer.new(Rdkafka::Metadata::Metadata.size) # zeroed => empty success

      # First two attempts return the erroring struct, the third succeeds.
      structs = [err_meta, err_meta, ok_meta]
      fetch = 0
      allow(Rdkafka::Bindings).to receive(:rd_kafka_metadata) do |_client, _flag, _topic, ptr, _timeout|
        ptr.write_pointer(structs[fetch])
        fetch += 1
        0
      end

      described_class.new(native_kafka, topic_name, 10)

      expect(fetch).to eq(3)
      expect(Rdkafka::Bindings).to have_received(:rd_kafka_metadata_destroy).exactly(3).times
      expect(Rdkafka::Bindings).to have_received(:rd_kafka_topic_destroy).exactly(3).times
    end

    it "does not destroy a metadata struct when the fetch itself fails before allocating one" do
      # timed_out (code -185) is retried, but the fetch never succeeds so no struct is allocated.
      allow(Rdkafka::Bindings).to receive(:rd_kafka_metadata).and_return(-185)

      expect {
        described_class.new(native_kafka, topic_name, 10)
      }.to raise_error(Rdkafka::RdkafkaError) { |e| expect(e.code).to eq(:timed_out) }

      expect(Rdkafka::Bindings).not_to have_received(:rd_kafka_metadata_destroy)
    end
  end
end
