# frozen_string_literal: true

module Rdkafka
  class Admin
    # Handle for delete consumer group offsets operation
    class DeleteConsumerGroupOffsetsHandle < AbstractHandle
      layout :pending, :bool,
        :response, :int

      # @return [String] the name of the operation
      def operation_name
        "delete consumer group offsets"
      end

      # Creates the result report
      # @return [DeleteConsumerGroupOffsetsReport]
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
