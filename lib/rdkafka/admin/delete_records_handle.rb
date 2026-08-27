# frozen_string_literal: true

module Rdkafka
  class Admin
    # Handle for delete records operation
    class DeleteRecordsHandle < AbstractHandle
      layout :pending, :bool,
        :response, :int

      # @return [String] the name of the operation
      def operation_name
        "delete records"
      end

      # @return [DeleteRecordsReport] report prepared by the background event callback, with the
      #   post-deletion low-watermark offsets (or per-partition errors)
      def create_result
        prepared_result
      end

      # Raises an error if the operation failed
      # @raise [RdkafkaError]
      def raise_error
        raise RdkafkaError.new(
          self[:response],
          broker_message: broker_message
        )
      end
    end
  end
end
