# frozen_string_literal: true

module Rdkafka
  module Bindings
    # Producer
    RD_KAFKA_VTYPE_END = 0
    RD_KAFKA_VTYPE_TOPIC = 1
    RD_KAFKA_VTYPE_RKT = 2
    RD_KAFKA_VTYPE_PARTITION = 3
    RD_KAFKA_VTYPE_VALUE = 4
    RD_KAFKA_VTYPE_KEY = 5
    RD_KAFKA_VTYPE_OPAQUE = 6
    RD_KAFKA_VTYPE_MSGFLAGS = 7
    RD_KAFKA_VTYPE_TIMESTAMP = 8
    RD_KAFKA_VTYPE_HEADER = 9
    RD_KAFKA_VTYPE_HEADERS = 10
    RD_KAFKA_PURGE_F_QUEUE = 1
    RD_KAFKA_PURGE_F_INFLIGHT = 2

    RD_KAFKA_MSG_F_COPY = 0x2

    attach_function :rd_kafka_producev, [:pointer, :varargs], :int, blocking: true
    attach_function :rd_kafka_purge, [:pointer, :int], :int, blocking: true
    callback :delivery_cb, [:pointer, :pointer, :pointer], :void
    attach_function :rd_kafka_conf_set_dr_msg_cb, [:pointer, :delivery_cb], :void

    # Hash mapping partitioner names to their FFI function symbols
    # @return [Hash{String => Symbol}]
    PARTITIONERS = %w[random consistent consistent_random murmur2 murmur2_random fnv1a fnv1a_random].each_with_object({}) do |name, hsh|
      method_name = :"rd_kafka_msg_partitioner_#{name}"
      attach_function method_name, [:pointer, :pointer, :size_t, :int32, :pointer, :pointer], :int32
      hsh[name] = method_name
    end

    # Calculates the partition for a message based on the partitioner
    #
    # @param topic_ptr [FFI::Pointer] pointer to the topic handle
    # @param str [String] the partition key string
    # @param partition_count [Integer, nil] number of partitions
    # @param partitioner [String] name of the partitioner to use
    # @return [Integer] partition number or RD_KAFKA_PARTITION_UA if unassigned
    # @raise [Rdkafka::Config::ConfigError] when an unknown partitioner is specified
    def self.partitioner(topic_ptr, str, partition_count, partitioner = "consistent_random")
      # Return RD_KAFKA_PARTITION_UA(unassigned partition) when partition count is nil/zero.
      return RD_KAFKA_PARTITION_UA unless partition_count&.nonzero?

      str_ptr = str.empty? ? FFI::MemoryPointer::NULL : FFI::MemoryPointer.from_string(str)
      method_name = PARTITIONERS.fetch(partitioner) do
        raise Rdkafka::Config::ConfigError.new("Unknown partitioner: #{partitioner}")
      end

      public_send(method_name, topic_ptr, str_ptr, partition_key_length(str), partition_count, nil, nil)
    end

    # Partition key length as the character count (legacy default).
    #
    # @param str [String] the partition key string
    # @return [Integer]
    def self.partition_key_size(str)
      str.size
    end

    # Partition key length as the byte count, which is what librdkafka hashes (UTF-8 bytes copied
    # into the key pointer) and what other Kafka clients use.
    #
    # @param str [String] the partition key string
    # @return [Integer]
    def self.partition_key_bytesize(str)
      str.bytesize
    end

    # Length of the partition key passed to librdkafka. Aliased to one of the methods above by
    # `Rdkafka::Config.partitioner_key_uses_bytesize=`, so the per-message path does not check the
    # setting on every call.
    singleton_class.alias_method :partition_key_length, :partition_key_size
  end
end
