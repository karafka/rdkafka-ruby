# frozen_string_literal: true

module Rdkafka
  class Admin
    # Handle for alter consumer group offsets operation
    class AlterConsumerGroupOffsetsHandle < AbstractHandle
      layout :pending, :bool,
        :response, :int

      # @return [String] the name of the operation
      def operation_name
        "alter consumer group offsets"
      end

      # Creates the result report
      # @return [AlterConsumerGroupOffsetsReport]
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
