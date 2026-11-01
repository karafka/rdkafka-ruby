# frozen_string_literal: true

module Rdkafka
  module Callbacks
    # Handles `RD_KAFKA_EVENT_LISTCONSUMERGROUPOFFSETS_RESULT` events
    # @private
    class ListConsumerGroupOffsetsHandler < BaseHandler
      class << self
        # Resolves the list consumer group offsets handle from its result event
        # @param event_ptr [FFI::Pointer] pointer to the event
        # @return [void]
        def call(event_ptr)
          result = Rdkafka::Bindings.rd_kafka_event_ListConsumerGroupOffsets_result(event_ptr)

          pointer_to_size_t = FFI::MemoryPointer.new(:size_t)
          group_result_array = Rdkafka::Bindings.rd_kafka_ListConsumerGroupOffsets_result_groups(result, pointer_to_size_t)
          group_results = GroupResult.create_group_results_from_array(pointer_to_size_t.read_int, group_result_array)
          handle_ptr = Rdkafka::Bindings.rd_kafka_event_opaque(event_ptr)

          if (handle = Rdkafka::Admin::ListConsumerGroupOffsetsHandle.remove(handle_ptr.address))
            unless resolve_operation_error(event_ptr, handle)
              group_result = group_results[0]

              # librdkafka only supports one group per invocation
              if group_result.nil?
                handle[:response] = Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR
                handle.result = Rdkafka::Admin::ListConsumerGroupOffsetsReport.new(
                  FFI::Pointer::NULL,
                  FFI::Pointer::NULL
                )
              else
                report = Rdkafka::Admin::ListConsumerGroupOffsetsReport.new(
                  group_result.error_string,
                  group_result.result_name,
                  Rdkafka::Bindings.rd_kafka_group_result_partitions(group_result_array.read_pointer)
                )
                handle.result = report

                # The group level error can be clean while a partition failed (e.g.
                # `unstable_offset_commit` with `require_stable_offsets`), which must not look
                # like a committed offset of nil.
                failed = report.partitions.find { |partition| partition[:error] }

                if !group_result.result_error.zero?
                  handle[:response] = group_result.result_error
                  handle.broker_message = report.error_string
                elsif failed
                  handle[:response] = failed[:error].rdkafka_response
                  handle.broker_message = "Failed for partition #{failed[:partition]} of '#{failed[:topic]}'"
                else
                  handle[:response] = Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR
                end
              end

              handle.unlock
            end
          end
        end
      end
    end
  end
end
