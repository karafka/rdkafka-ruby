# frozen_string_literal: true

module Rdkafka
  module Callbacks
    # Handles `RD_KAFKA_EVENT_DELETEGROUPS_RESULT` events
    # @private
    class DeleteGroupsHandler < BaseHandler
      class << self
        # Resolves the delete-groups handle from its result event
        # @param event_ptr [FFI::Pointer] pointer to the event
        # @return [void]
        def call(event_ptr)
          delete_groups_result = Rdkafka::Bindings.rd_kafka_event_DeleteGroups_result(event_ptr)

          # Get the number of delete group results
          pointer_to_size_t = FFI::MemoryPointer.new(:size_t)
          delete_group_result_array = Rdkafka::Bindings.rd_kafka_DeleteGroups_result_groups(delete_groups_result, pointer_to_size_t)
          delete_group_results = GroupResult.create_group_results_from_array(pointer_to_size_t.read_int, delete_group_result_array)
          delete_group_handle_ptr = Rdkafka::Bindings.rd_kafka_event_opaque(event_ptr)

          if (delete_group_handle = Rdkafka::Admin::DeleteGroupsHandle.remove(delete_group_handle_ptr.address))
            unless resolve_operation_error(event_ptr, delete_group_handle)
              delete_group_handle[:response] = delete_group_results[0].result_error
              delete_group_handle.result = Rdkafka::Admin::DeleteGroupsReport.new(
                delete_group_results[0].error_string,
                delete_group_results[0].result_name
              )
              delete_group_handle.broker_message = delete_group_handle.result.error_string

              delete_group_handle.unlock
            end
          end
        end
      end
    end
  end
end
