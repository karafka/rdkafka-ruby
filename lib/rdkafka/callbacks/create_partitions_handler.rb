# frozen_string_literal: true

module Rdkafka
  module Callbacks
    # Handles `RD_KAFKA_ADMIN_OP_CREATEPARTITIONS_RESULT` events
    # @private
    class CreatePartitionsHandler < BaseHandler
      class << self
        # Resolves the create-partitions handle from its result event
        # @param event_ptr [FFI::Pointer] pointer to the event
        # @return [void]
        def call(event_ptr)
          create_partitionss_result = Rdkafka::Bindings.rd_kafka_event_CreatePartitions_result(event_ptr)

          # Get the number of create topic results
          pointer_to_size_t = FFI::MemoryPointer.new(:int32)
          create_partitions_result_array = Rdkafka::Bindings.rd_kafka_CreatePartitions_result_topics(create_partitionss_result, pointer_to_size_t)
          create_partitions_results = TopicResult.create_topic_results_from_array(pointer_to_size_t.read_int, create_partitions_result_array)
          create_partitions_handle_ptr = Rdkafka::Bindings.rd_kafka_event_opaque(event_ptr)

          if create_partitions_handle = Rdkafka::Admin::CreatePartitionsHandle.remove(create_partitions_handle_ptr.address)
            create_partitions_handle[:response] = create_partitions_results[0].result_error
            create_partitions_handle.result = Rdkafka::Admin::CreatePartitionsReport.new(
              create_partitions_results[0].error_string,
              create_partitions_results[0].result_name
            )
            create_partitions_handle.broker_message = create_partitions_handle.result.error_string

            create_partitions_handle.unlock
          end
        end
      end
    end
  end
end
