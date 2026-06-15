# frozen_string_literal: true

module Rdkafka
  module Callbacks
    # Handles `RD_KAFKA_EVENT_DESCRIBEACLS_RESULT` events
    # @private
    class DescribeAclHandler < BaseHandler
      class << self
        # Resolves the describe-ACL handle from its result event
        # @param event_ptr [FFI::Pointer] pointer to the event
        # @return [void]
        def call(event_ptr)
          describe_acl = DescribeAclResult.new(event_ptr)
          describe_acl_handle_ptr = Rdkafka::Bindings.rd_kafka_event_opaque(event_ptr)

          if describe_acl_handle = Rdkafka::Admin::DescribeAclHandle.remove(describe_acl_handle_ptr.address)
            describe_acl_handle[:response] = describe_acl.result_error
            describe_acl_handle.broker_message = read_event_string(describe_acl.error_string)

            describe_acl_handle.result = if describe_acl.result_error == Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR
              Rdkafka::Admin::DescribeAclReport.new(
                acls: describe_acl.matching_acls,
                acls_count: describe_acl.matching_acls_count
              )
            else
              Rdkafka::Admin::DescribeAclReport.new(acls: FFI::Pointer::NULL, acls_count: 0)
            end

            describe_acl_handle.unlock
          end
        end
      end
    end
  end
end
