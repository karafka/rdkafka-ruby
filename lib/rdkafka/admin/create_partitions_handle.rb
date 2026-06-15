module Rdkafka
  class Admin
    # Handle for create partitions operation
    class CreatePartitionsHandle < AbstractHandle
      layout :pending, :bool,
        :response, :int

      # @return [String] the name of the operation
      def operation_name
        "create partitions"
      end

      # @return [CreatePartitionsReport] report prepared by the background event callback
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
