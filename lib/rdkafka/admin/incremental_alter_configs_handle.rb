# frozen_string_literal: true

module Rdkafka
  class Admin
    # Handle for incremental alter configs operation
    class IncrementalAlterConfigsHandle < AbstractHandle
      layout :pending, :bool,
        :response, :int

      # @return [String] the name of the operation.
      def operation_name
        "incremental alter configs"
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
