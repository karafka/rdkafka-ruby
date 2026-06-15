# frozen_string_literal: true

module Rdkafka
  module Callbacks
    # Handles `RD_KAFKA_EVENT_INCREMENTALALTERCONFIGS_RESULT` events
    # @private
    class IncrementalAlterConfigsHandler < BaseHandler
      class << self
        # Resolves the incremental-alter-configs handle from its result event
        # @param event_ptr [FFI::Pointer] pointer to the event
        # @return [void]
        def call(event_ptr)
          incremental_alter = IncrementalAlterConfigsResult.new(event_ptr)
          incremental_alter_handle_ptr = Rdkafka::Bindings.rd_kafka_event_opaque(event_ptr)

          if incremental_alter_handle = Rdkafka::Admin::IncrementalAlterConfigsHandle.remove(incremental_alter_handle_ptr.address)
            incremental_alter_handle[:response] = incremental_alter.result_error
            incremental_alter_handle.broker_message = read_event_string(incremental_alter.error_string)

            # Parsing can raise on per-resource errors. The exception is captured and re-raised on
            # the waiting thread, since an exception leaving this callback would unwind through
            # librdkafka native frames
            incremental_alter_handle.result = begin
              if incremental_alter.result_error == Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR
                Rdkafka::Admin::IncrementalAlterConfigsReport.new(
                  config_entries: incremental_alter.results,
                  entry_count: incremental_alter.results_count
                )
              else
                Rdkafka::Admin::IncrementalAlterConfigsReport.new(config_entries: FFI::Pointer::NULL, entry_count: 0)
              end
            rescue => e
              e
            end

            incremental_alter_handle.unlock
          end
        end
      end
    end
  end
end
