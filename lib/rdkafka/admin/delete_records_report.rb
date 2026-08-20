# frozen_string_literal: true

module Rdkafka
  class Admin
    # Report for delete records operation result
    class DeleteRecordsReport
      # Per-partition results. Each partition's `offset` is the post-deletion low-watermark (the
      # smallest available offset of all live replicas) and its `err` carries the per-partition
      # error code, if deletion failed for that partition.
      # @return [Rdkafka::Consumer::TopicPartitionList]
      attr_reader :offsets

      # @param result_ptr [FFI::Pointer] pointer to the `rd_kafka_DeleteRecords_result_t`
      def initialize(result_ptr)
        @offsets = Rdkafka::Consumer::TopicPartitionList.new

        return if result_ptr.null?

        native_tpl = Bindings.rd_kafka_DeleteRecords_result_offsets(result_ptr)

        @offsets = Rdkafka::Consumer::TopicPartitionList.from_native_tpl(native_tpl)
      end
    end
  end
end
