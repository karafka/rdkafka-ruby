# frozen_string_literal: true

module Rdkafka
  module Callbacks
    # Handles `RD_KAFKA_EVENT_LISTOFFSETS_RESULT` events
    # @private
    class ListOffsetsHandler < BaseHandler
      class << self
        # Resolves the list-offsets handle from its result event
        # @param event_ptr [FFI::Pointer] pointer to the event
        # @return [void]
        def call(event_ptr)
          list_offsets = ListOffsetsResult.new(event_ptr)
          list_offsets_handle_ptr = Rdkafka::Bindings.rd_kafka_event_opaque(event_ptr)

          if list_offsets_handle = Rdkafka::Admin::ListOffsetsHandle.remove(list_offsets_handle_ptr.address)
            list_offsets_handle[:response] = list_offsets.result_error
            list_offsets_handle.broker_message = read_event_string(list_offsets.error_string)

            # Parsing can raise on per-partition errors. The exception is captured and re-raised
            # on the waiting thread, since an exception leaving this callback would unwind through
            # librdkafka native frames.
            list_offsets_handle.result = begin
              if list_offsets.result_error == Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR
                Rdkafka::Admin::ListOffsetsReport.new(
                  result_infos: list_offsets.result_infos,
                  result_count: list_offsets.result_count
                )
              else
                Rdkafka::Admin::ListOffsetsReport.new(result_infos: FFI::Pointer::NULL, result_count: 0)
              end
            rescue => e
              e
            end

            list_offsets_handle.unlock
          end
        end
      end
    end
  end
end
