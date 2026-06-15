# frozen_string_literal: true

module Rdkafka
  module Callbacks
    # Handles `RD_KAFKA_EVENT_DESCRIBECONFIGS_RESULT` events
    # @private
    class DescribeConfigsHandler < BaseHandler
      class << self
        # Resolves the describe-configs handle from its result event
        # @param event_ptr [FFI::Pointer] pointer to the event
        # @return [void]
        def call(event_ptr)
          describe_configs = DescribeConfigsResult.new(event_ptr)
          describe_configs_handle_ptr = Rdkafka::Bindings.rd_kafka_event_opaque(event_ptr)

          if describe_configs_handle = Rdkafka::Admin::DescribeConfigsHandle.remove(describe_configs_handle_ptr.address)
            describe_configs_handle[:response] = describe_configs.result_error
            describe_configs_handle.broker_message = read_event_string(describe_configs.error_string)

            # Parsing can raise on per-resource errors. The exception is captured and re-raised on
            # the waiting thread, since an exception leaving this callback would unwind through
            # librdkafka native frames
            describe_configs_handle.result = begin
              if describe_configs.result_error == Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR
                Rdkafka::Admin::DescribeConfigsReport.new(
                  config_entries: describe_configs.results,
                  entry_count: describe_configs.results_count
                )
              else
                Rdkafka::Admin::DescribeConfigsReport.new(config_entries: FFI::Pointer::NULL, entry_count: 0)
              end
            rescue => e
              e
            end

            describe_configs_handle.unlock
          end
        end
      end
    end
  end
end
