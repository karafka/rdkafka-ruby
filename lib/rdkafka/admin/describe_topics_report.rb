# frozen_string_literal: true

module Rdkafka
  class Admin
    # Report for describe topics operation result
    class DescribeTopicsReport
      include DescribeResultParsing

      # Described topics, one per requested topic name. Each entry is a hash with:
      #   - `:name` [String] the topic name
      #   - `:topic_id` [String, nil] the topic id (KIP-516), base64 encoded
      #   - `:is_internal` [Boolean] whether the topic is internal (e.g. `__consumer_offsets`)
      #   - `:partitions` [Array<Hash>] each with `:partition` [Integer], `:leader` [Hash, nil] and
      #     `:replicas` / `:isr` [Array<Hash>]; nodes have `:id`, `:host`, `:port` and `:rack`
      #   - `:authorized_operations` [Array<Integer>, nil] `Bindings::RD_KAFKA_ACL_OPERATION_*`
      #     codes, `nil` unless requested with `include_authorized_operations: true`
      #   - `:error` [RdkafkaError, nil] per-topic error (e.g. `unknown_topic_or_part`), `nil`
      #     on success
      # @return [Array<Hash>]
      attr_reader :topics

      # @param result_ptr [FFI::Pointer] pointer to the `rd_kafka_DescribeTopics_result_t`
      def initialize(result_ptr)
        @topics = []

        return if result_ptr.null?

        count_ptr = FFI::MemoryPointer.new(:size_t)
        array_ptr = Bindings.rd_kafka_DescribeTopics_result_topics(result_ptr, count_ptr)

        return if array_ptr.null?

        array_ptr.read_array_of_pointer(count_ptr.read(:size_t)).each do |topic_ptr|
          @topics << extract_topic(topic_ptr)
        end
      end

      private

      # @param topic_ptr [FFI::Pointer] pointer to a `rd_kafka_TopicDescription_t`
      # @return [Hash]
      def extract_topic(topic_ptr)
        name_ptr = Bindings.rd_kafka_TopicDescription_name(topic_ptr)
        operations_count_ptr = FFI::MemoryPointer.new(:size_t)

        {
          name: name_ptr.null? ? nil : name_ptr.read_string,
          topic_id: extract_topic_id(topic_ptr),
          is_internal: Bindings.rd_kafka_TopicDescription_is_internal(topic_ptr) != 0,
          partitions: extract_partitions(topic_ptr),
          authorized_operations: extract_authorized_operations(
            Bindings.rd_kafka_TopicDescription_authorized_operations(topic_ptr, operations_count_ptr),
            operations_count_ptr
          ),
          error: extract_error(topic_ptr)
        }
      end

      # @param topic_ptr [FFI::Pointer] pointer to a `rd_kafka_TopicDescription_t`
      # @return [String, nil]
      def extract_topic_id(topic_ptr)
        uuid_ptr = Bindings.rd_kafka_TopicDescription_topic_id(topic_ptr)

        return nil if uuid_ptr.null?

        string_ptr = Bindings.rd_kafka_Uuid_base64str(uuid_ptr)
        string_ptr.null? ? nil : string_ptr.read_string
      end

      # @param topic_ptr [FFI::Pointer] pointer to a `rd_kafka_TopicDescription_t`
      # @return [Array<Hash>]
      def extract_partitions(topic_ptr)
        count_ptr = FFI::MemoryPointer.new(:size_t)
        array_ptr = Bindings.rd_kafka_TopicDescription_partitions(topic_ptr, count_ptr)

        return [] if array_ptr.null?

        array_ptr.read_array_of_pointer(count_ptr.read(:size_t)).map do |partition_ptr|
          replicas_count_ptr = FFI::MemoryPointer.new(:size_t)
          isr_count_ptr = FFI::MemoryPointer.new(:size_t)

          {
            partition: Bindings.rd_kafka_TopicPartitionInfo_partition(partition_ptr),
            leader: extract_node(Bindings.rd_kafka_TopicPartitionInfo_leader(partition_ptr)),
            replicas: extract_nodes(
              Bindings.rd_kafka_TopicPartitionInfo_replicas(partition_ptr, replicas_count_ptr),
              replicas_count_ptr
            ),
            isr: extract_nodes(
              Bindings.rd_kafka_TopicPartitionInfo_isr(partition_ptr, isr_count_ptr),
              isr_count_ptr
            )
          }
        end
      end

      # The error is owned by the result, so it is copied and not destroyed here.
      # @param topic_ptr [FFI::Pointer] pointer to a `rd_kafka_TopicDescription_t`
      # @return [RdkafkaError, nil]
      def extract_error(topic_ptr)
        error_ptr = Bindings.rd_kafka_TopicDescription_error(topic_ptr)

        return nil if error_ptr.null?

        code = Bindings.rd_kafka_error_code(error_ptr)

        return nil if code == Bindings::RD_KAFKA_RESP_ERR_NO_ERROR

        string_ptr = Bindings.rd_kafka_error_string(error_ptr)
        RdkafkaError.new(code, broker_message: string_ptr.null? ? nil : string_ptr.read_string)
      end
    end
  end
end
