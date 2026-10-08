# frozen_string_literal: true

module Rdkafka
  module Callbacks
    # Handles `RD_KAFKA_EVENT_DESCRIBECONSUMERGROUPS_RESULT` events
    # @private
    class DescribeConsumerGroupsHandler < BaseHandler
      class << self
        # Resolves the describe-consumer-groups handle from its result event
        # @param event_ptr [FFI::Pointer] pointer to the event
        # @return [void]
        def call(event_ptr)
          result_ptr = Rdkafka::Bindings.rd_kafka_event_DescribeConsumerGroups_result(event_ptr)
          handle_ptr = Rdkafka::Bindings.rd_kafka_event_opaque(event_ptr)

          return unless (handle = Rdkafka::Admin::DescribeConsumerGroupsHandle.remove(handle_ptr.address))

          return if resolve_operation_error(event_ptr, handle)

          handle[:response] = Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR

          # Parsing must copy everything out of event-owned memory before the event is destroyed.
          # An exception here is captured and re-raised on the waiting thread, since it cannot
          # unwind through librdkafka native frames.
          handle.result = begin
            Rdkafka::Admin::DescribeConsumerGroupsReport.new(result_ptr)
          rescue => e
            e
          end

          handle.unlock
        end
      end
    end
  end
end
