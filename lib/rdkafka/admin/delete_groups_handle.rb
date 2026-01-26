# frozen_string_literal: true

module Rdkafka
  class Admin
    # Handle for delete groups operation
    class DeleteGroupsHandle < AbstractHandle
      layout :pending, :bool, # TODO: ???
        :response, :int,
        :error_string, :pointer,
        :result_name, :pointer

      # @return [String] the name of the operation
      def operation_name
        "delete groups"
      end

      # Creates the result report
      # @return [DeleteGroupsReport]
      def create_result
        DeleteGroupsReport.new(self[:error_string], self[:result_name])
      end

      # Raises an error if the operation failed
      # @raise [RdkafkaError]
      def raise_error
        raise RdkafkaError.new(
          self[:response],
          broker_message: create_result.error_string
        )
      end
    end
  end
end
