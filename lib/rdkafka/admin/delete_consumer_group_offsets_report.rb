# frozen_string_literal: true

module Rdkafka
  class Admin
    # Report for delete consumer group offsets operation result
    class DeleteConsumerGroupOffsetsReport
      # @return [String, nil] the group the operation ran against
      attr_reader :group_name

      # @return [String, nil] group level error message, if any
      attr_reader :error_string

      # Per partition outcomes, each with `:topic`, `:partition`, `:offset` and `:error`
      # (an `RdkafkaError` or nil). Partitions are reported individually because Kafka can
      # fail some and accept others within a single request.
      # @return [Array<Hash>]
      attr_reader :partitions

      # @param error_string [FFI::Pointer] pointer to the group level error string
      # @param result_name [FFI::Pointer] pointer to the group name
      # @param partitions_ptr [FFI::Pointer] pointer to the per partition result list
      def initialize(error_string, result_name, partitions_ptr = FFI::Pointer::NULL)
        @error_string = error_string.read_string if error_string != FFI::Pointer::NULL
        @group_name = result_name.read_string if result_name != FFI::Pointer::NULL
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

          {
            topic: tp[:topic],
            partition: tp[:partition],
            offset: tp[:offset],
            error: tp[:err].zero? ? nil : RdkafkaError.new(tp[:err])
          }
        end
      end
    end
  end
end
