# frozen_string_literal: true

module Rdkafka
  class Admin
    # Report for list consumer group offsets operation result
    class ListConsumerGroupOffsetsReport
      # @return [String, nil] the group the operation ran against
      attr_reader :group_name

      # @return [String, nil] group level error message, if any
      attr_reader :error_string

      # Committed offsets per partition, each with `:topic`, `:partition`, `:offset` (nil when
      # the group has no committed offset for it or the partition has an `:error`), `:metadata`
      # (nil when none was committed) and `:error` (an `RdkafkaError` or nil).
      # @return [Array<Hash>]
      attr_reader :partitions

      # @param error_string [FFI::Pointer] pointer to the group level error string
      # @param result_name [FFI::Pointer] pointer to the group name
      # @param partitions_ptr [FFI::Pointer] pointer to the per partition result list
      def initialize(error_string, result_name, partitions_ptr = FFI::Pointer::NULL)
        @error_string = error_string.null? ? nil : error_string.read_string
        @group_name = result_name.null? ? nil : result_name.read_string
        @partitions = extract_partitions(partitions_ptr)
      end

      private

      # @param partitions_ptr [FFI::Pointer] pointer to a native topic partition list
      # @return [Array<Hash>]
      def extract_partitions(partitions_ptr)
        return [] if partitions_ptr.nil? || partitions_ptr.null?

        native = Bindings::TopicPartitionList.new(partitions_ptr)

        Array.new(native[:cnt]) do |i|
          tp = Bindings::TopicPartition.new(native[:elems] + (i * Bindings::TopicPartition.size))
          offset = tp[:offset]

          {
            topic: tp[:topic],
            partition: tp[:partition],
            offset: (offset == Bindings::RD_KAFKA_OFFSET_INVALID) ? nil : offset,
            metadata: tp[:metadata].null? ? nil : tp[:metadata].read_string(tp[:metadata_size]),
            error: tp[:err].zero? ? nil : RdkafkaError.new(tp[:err])
          }
        end
      end
    end
  end
end
