module Rdkafka
  class Metadata
    attr_reader :brokers, :topics

    def initialize(native_client, topic_name = nil)
      native_topic = if topic_name
        Rdkafka::Bindings.rd_kafka_topic_new(native_client, topic_name, nil)
      end

      ptr = FFI::MemoryPointer.new(:pointer)

      # If topic_flag is 1, we request info about *all* topics in the cluster.  If topic_flag is 0,
      # we only request info about locally known topics (or a single topic if one is passed in).
      topic_flag = topic_name.nil? ? 1 : 0

      # Retrieve the Metadata
      result = Rdkafka::Bindings.rd_kafka_metadata(native_client, topic_flag, native_topic, ptr, 250)

      # Error Handling
      raise Rdkafka::RdkafkaError.new(result) unless result.zero?

      metadata_from_native(ptr.read_pointer)
    ensure
      Rdkafka::Bindings.rd_kafka_topic_destroy(native_topic) if topic_name
      Rdkafka::Bindings.rd_kafka_metadata_destroy(ptr.read_pointer)
    end

    private

    def metadata_from_native(ptr)
      metadata = Metadata.new(ptr)
      @brokers = Array.new(metadata[:brokers_count]) do |i|
        BrokerMetadata.new(metadata[:brokers_metadata] + (i * BrokerMetadata.size)).to_h
      end

      @topics = Array.new(metadata[:topics_count]) do |i|
        topic = TopicMetadata.new(metadata[:topics_metadata] + (i * TopicMetadata.size))
        raise Rdkafka::RdkafkaError.new(topic[:rd_kafka_resp_err]) unless topic[:rd_kafka_resp_err].zero?

        partitions = Array.new(topic[:partition_count]) do |j|
          partition = PartitionMetadata.new(topic[:partitions_metadata] + (j * PartitionMetadata.size))
          raise Rdkafka::RdkafkaError.new(partition[:rd_kafka_resp_err]) unless partition[:rd_kafka_resp_err].zero?
          partition.to_h
        end
        topic.to_h.merge!(partitions: partitions)
      end
    end

    class CustomFFIStruct < FFI::Struct
      def to_h
        members.each_with_object({}) do |mem, hsh|
          val = self.[](mem)
          next if val.is_a?(FFI::Pointer) || mem == :rd_kafka_resp_err

          hsh[mem] = self.[](mem)
        end
      end
    end

    class Metadata < CustomFFIStruct
      layout :brokers_count, :int,
             :brokers_metadata, :pointer,
             :topics_count, :int,
             :topics_metadata, :pointer,
             :broker_id, :int32,
             :broker_name, :string
    end

    class BrokerMetadata < CustomFFIStruct
      layout :broker_id, :int32,
             :broker_name, :string,
             :broker_port, :int
    end

    class TopicMetadata < CustomFFIStruct
      layout :topic_name, :string,
             :partition_count, :int,
             :partitions_metadata, :pointer,
             :rd_kafka_resp_err, :int
    end

    class PartitionMetadata < CustomFFIStruct
      layout :partition_id, :int32,
             :rd_kafka_resp_err, :int,
             :leader, :int32,
             :replica_count, :int,
             :replicas, :pointer,
             :in_sync_replica_brokers, :int,
             :isrs, :pointer
    end
  end
end
