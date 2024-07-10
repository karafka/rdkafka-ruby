module Rdkafka
  class Admin
    class DeleteTopicHandle < AbstractHandle
      layout :pending, :bool,
             :response, :int,
             :error_string, :pointer,
             :result_name, :pointer

      # @return [String] the name of the operation
      def operation_name
        "delete topic"
      end

      # @return [Boolean] whether the delete topic was successful
      def create_result
        DeleteTopicReport.new(self[:error_string], self[:result_name])
      end

      def raise_error
        raise RdkafkaError.new(
            self[:response],
            broker_message: DeleteTopicReport.new(self[:error_string], self[:result_name]).error_string
        )
      end
    end
  end
end
