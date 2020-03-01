module Rdkafka
  class Metadata
    def initialize(native_client, topic_name = nil)
      native_topic = if topic_name
        Rdkafka::Bindings.rd_kafka_topic_new(native_client, topic_name, nil)
      end

      ptr = FFI::MemoryPointer.new(:pointer)
      # Retrieve metadata flag is 0/1 for single/multiple topics.
      topic_flag = topic_name ? 1 : 0

      # TODO: Handle errors.
      Rdkafka::Bindings.rd_kafka_metadata(native_client, topic_flag, native_topic, ptr, 250)
      @metadata = metadata_from_native(ptr)
    ensure
      Rdkafka::Bindings.rd_kafka_topic_destroy(native_topic) if topic_name
      Rdkafka::Bindings.rd_kafka_metadata_destroy(ptr)
    end

    def brokers
      @metadata[:brokers]
    end

    def topics
      @metadata[:topics]
    end

    private

    def metadata_from_native(ptr)
      metadata_struct = Metadata.new(ptr.read_pointer)

      metadata = {}
      metadata[:brokers] = Array.new(metadata_struct[:brokers_count]) do |i|
        BrokerMetadata.new(metadata_struct[:brokers_metadata] + (i * BrokerMetadata.size)).to_h
      end

      metadata[:topics] = Array.new(metadata_struct[:topics_count]) do |i|
        TopicMetadata.new(metadata_struct[:topics_metadata] + (i * TopicMetadata.size)).to_h
      end

      metadata[:topics].each do |topic|
        topic[:partitions] = Array.new(topic[:partition_count]) do |i|
          PartitionMetadata.new(topic[:partitions_metadata] + (i * PartitionMetadata.size)).to_h
        end
      end
      metadata
    end

    class BrokerMetadata < FFI::Struct
      layout :broker_id, :int32,
             :broker_name, :string,
             :broker_port, :int

      def to_h
        members.each_with_object({}) do |mem, hsh|
          hsh[mem] = self.[](mem)
        end
      end
    end

    class PartitionMetadata < FFI::Struct
      layout :partition_id, :int32,
             :rd_kafka_resp_err, :int,
             :leader, :int32,
             :replica_count, :int,
             :replicas, :int32,
             :isr_broker_count, :int,
             :sync_replica_count, :int32

      def to_h
        members.each_with_object({}) do |mem, hsh|
          hsh[mem] = self.[](mem)
        end
      end
    end

    class TopicMetadata < FFI::Struct
      layout :topic_name, :string,
             :partition_count, :int,
             :partitions_metadata, :pointer,
             :rd_kafka_resp_err, :int

      def to_h
        members.each_with_object({}) do |mem, hsh|
          hsh[mem] = self.[](mem)
        end
      end
    end

    class Metadata < FFI::Struct
      layout :brokers_count, :int,
             :brokers_metadata, :pointer, #BrokersMetadata.ptr, # Array of BrokerMetadata
             :topics_count, :int,
             :topics_metadata, :pointer, #TopicsMetadata.ptr, # Array of TopicMetadata
             :broker_id, :int32,
             :broker_name, :string

      def to_h
        members.each_with_object({}) do |mem, hsh|
          hsh[mem] = self.[](mem)
        end
      end
    end
  end
end
