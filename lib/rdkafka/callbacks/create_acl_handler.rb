# frozen_string_literal: true

module Rdkafka
  module Callbacks
    # Handles `RD_KAFKA_EVENT_CREATEACLS_RESULT` events
    # @private
    class CreateAclHandler < BaseHandler
      class << self
        # Resolves the create-ACL handle from its result event
        # @param event_ptr [FFI::Pointer] pointer to the event
        # @return [void]
        def call(event_ptr)
          create_acls_result = Rdkafka::Bindings.rd_kafka_event_CreateAcls_result(event_ptr)

          # Get the number of acl results
          pointer_to_size_t = FFI::MemoryPointer.new(:size_t)
          create_acl_result_array = Rdkafka::Bindings.rd_kafka_CreateAcls_result_acls(create_acls_result, pointer_to_size_t)
          create_acl_results = CreateAclResult.create_acl_results_from_array(pointer_to_size_t.read_int, create_acl_result_array)
          create_acl_handle_ptr = Rdkafka::Bindings.rd_kafka_event_opaque(event_ptr)

          if create_acl_handle = Rdkafka::Admin::CreateAclHandle.remove(create_acl_handle_ptr.address)
            unless resolve_operation_error(event_ptr, create_acl_handle)
              create_acl_handle[:response] = create_acl_results[0].result_error
              create_acl_handle.result = Rdkafka::Admin::CreateAclReport.new(
                rdkafka_response: create_acl_results[0].result_error,
                rdkafka_response_string: create_acl_results[0].error_string
              )
              create_acl_handle.broker_message = create_acl_handle.result.rdkafka_response_string

              create_acl_handle.unlock
            end
          end
        end
      end
    end
  end
end
