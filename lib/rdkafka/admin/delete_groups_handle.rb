# frozen_string_literal: true

module Rdkafka
  class Admin
    class DeleteGroupsHandle < AbstractHandle
      layout :pending, :bool, # TODO: ???
             :response, :int,
             :error_string, :pointer,
             :result_name, :pointer

      # @return [String] the name of the operation
      def operation_name
        "delete groups"
      end

      def create_result
        DeleteGroupsReport.new(self[:error_string], self[:result_name])
      end

      def raise_error
        raise RdkafkaError.new(
            self[:response],
            broker_message: create_result.error_string
        )
      end
    end
  end
end
