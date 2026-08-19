# frozen_string_literal: true

module Rdkafka
  class Admin
    # Handle for list consumer groups operation
    class ListConsumerGroupsHandle < AbstractHandle
      layout :pending, :bool,
        :response, :int

      # @return [String] the name of the operation
      def operation_name
        "list consumer groups"
      end

      # @return [ListConsumerGroupsReport] report prepared by the background event callback, with
      #   the listed consumer groups.
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
