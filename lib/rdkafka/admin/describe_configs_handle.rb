# frozen_string_literal: true

module Rdkafka
  class Admin
    # Handle for describe configs operation
    class DescribeConfigsHandle < AbstractHandle
      layout :pending, :bool,
        :response, :int

      # @return [String] the name of the operation.
      def operation_name
        "describe configs"
      end

      # @return [DescribeConfigsReport] report prepared by the background event callback
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
