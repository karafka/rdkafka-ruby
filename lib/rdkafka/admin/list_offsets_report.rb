# frozen_string_literal: true

module Rdkafka
  class Admin
    # Report for list offsets operation result
    class ListOffsetsReport
      attr_reader :offsets

      # @param result_infos [FFI::Pointer] pointer to result info array
      # @param result_count [Integer] number of result info entries
      def initialize(result_infos:, result_count:)
        @offsets = []

        return if result_infos.null?

        result_infos
          .read_array_of_pointer(result_count)
          .each { |result_info_ptr| validate!(result_info_ptr) }
          .each do |result_info_ptr|
            tp_ptr = Bindings.rd_kafka_ListOffsetsResultInfo_topic_partition(result_info_ptr)
            tp = Bindings::TopicPartition.new(tp_ptr)
            timestamp = Bindings.rd_kafka_ListOffsetsResultInfo_timestamp(result_info_ptr)
            leader_epoch = Bindings.rd_kafka_topic_partition_get_leader_epoch(tp_ptr)

            @offsets << {
              topic: tp[:topic],
              partition: tp[:partition],
              offset: tp[:offset],
              timestamp: timestamp,
              leader_epoch: (leader_epoch == -1) ? nil : leader_epoch
            }
          end
      end

      private

      # Validates the partition result and raises an error if invalid
      # @param result_info_ptr [FFI::Pointer] pointer to the result info
      # @raise [RdkafkaError] when the partition has an error
      def validate!(result_info_ptr)
        tp_ptr = Bindings.rd_kafka_ListOffsetsResultInfo_topic_partition(result_info_ptr)
        tp = Bindings::TopicPartition.new(tp_ptr)
        code = tp[:err]

        return if code.zero?

        raise RdkafkaError.new(code)
      end
    end
  end
end
