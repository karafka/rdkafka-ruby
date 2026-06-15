# frozen_string_literal: true

module Rdkafka
  module Callbacks
    # Handles `RD_KAFKA_EVENT_DELETEACLS_RESULT` events
    # @private
    class DeleteAclHandler < BaseHandler
      class << self
        # Resolves the delete-ACL handle from its result event
        # @param event_ptr [FFI::Pointer] pointer to the event
        # @return [void]
        def call(event_ptr)
          delete_acls_result = Rdkafka::Bindings.rd_kafka_event_DeleteAcls_result(event_ptr)

          # Get the number of acl results
          pointer_to_size_t = FFI::MemoryPointer.new(:int32)
          delete_acl_result_responses = Rdkafka::Bindings.rd_kafka_DeleteAcls_result_responses(delete_acls_result, pointer_to_size_t)
          delete_acl_results = DeleteAclResult.delete_acl_results_from_array(pointer_to_size_t.read_int, delete_acl_result_responses)
          delete_acl_handle_ptr = Rdkafka::Bindings.rd_kafka_event_opaque(event_ptr)

          if delete_acl_handle = Rdkafka::Admin::DeleteAclHandle.remove(delete_acl_handle_ptr.address)
            unless resolve_operation_error(event_ptr, delete_acl_handle)
              delete_acl_handle[:response] = delete_acl_results[0].result_error
              delete_acl_handle.broker_message = read_event_string(delete_acl_results[0].error_string)

              delete_acl_handle.result = if delete_acl_results[0].result_error == Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR
                Rdkafka::Admin::DeleteAclReport.new(
                  matching_acls: delete_acl_results[0].matching_acls,
                  matching_acls_count: delete_acl_results[0].matching_acls_count
                )
              else
                Rdkafka::Admin::DeleteAclReport.new(matching_acls: FFI::Pointer::NULL, matching_acls_count: 0)
              end

              delete_acl_handle.unlock
            end
          end
        end
      end
    end
  end
end
