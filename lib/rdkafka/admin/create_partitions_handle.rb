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
    end
  end
end
