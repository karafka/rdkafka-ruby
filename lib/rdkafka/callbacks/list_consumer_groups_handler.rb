# frozen_string_literal: true

module Rdkafka
  module Callbacks
    # Handles `RD_KAFKA_EVENT_LISTCONSUMERGROUPS_RESULT` events
    # @private
    class ListConsumerGroupsHandler < BaseHandler
      class << self
        # Resolves the list-consumer-groups handle from its result event
        # @param event_ptr [FFI::Pointer] pointer to the event
        # @return [void]
        def call(event_ptr)
          result_ptr = Rdkafka::Bindings.rd_kafka_event_ListConsumerGroups_result(event_ptr)
          handle_ptr = Rdkafka::Bindings.rd_kafka_event_opaque(event_ptr)

          return unless (handle = Rdkafka::Admin::ListConsumerGroupsHandle.remove(handle_ptr.address))

          # An operation-level error (e.g. timeout or a closed client) is delivered on the event
          # itself; per the source feature's all-or-nothing contract we surface it as the result
          # rather than returning a partial listing.
          return if resolve_operation_error(event_ptr, handle)

          handle[:response] = Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR

          # Parsing must copy everything out of event-owned memory before the event is destroyed.
          # An exception here is captured and re-raised on the waiting thread, since it cannot
          # unwind through librdkafka native frames.
          handle.result = begin
            Rdkafka::Admin::ListConsumerGroupsReport.new(result_ptr)
          rescue => e
            e
          end

          handle.unlock
        end
      end
    end
  end
end
