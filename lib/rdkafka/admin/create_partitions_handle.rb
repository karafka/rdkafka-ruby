module Rdkafka
  class Admin
    class CreatePartitionsHandle < AbstractHandle
      layout :pending, :bool,
             :response, :int,
             :error_string, :pointer,
             :result_name, :pointer

      # @return [String] the name of the operation
      def operation_name
        "create partitions"
      end

      # @return [Boolean] whether the create topic was successful
      def create_result
        CreatePartitionsReport.new(self[:error_string], self[:result_name])
      end

      def raise_error
        raise RdkafkaError.new(
            self[:response],
            broker_message: CreatePartitionsReport.new(self[:error_string], self[:result_name]).error_string
        )
      end
    end
  end
end
