# frozen_string_literal: true

module Rdkafka
  class Admin
    # Handle for list offsets operation
    class ListOffsetsHandle < AbstractHandle
      layout :pending, :bool,
        :response, :int,
        :response_string, :pointer,
        :result_infos, :pointer,
        :result_count, :int

      # @return [String] the name of the operation.
      def operation_name
        "list offsets"
      end

      # @return [ListOffsetsReport] instance with partition offset information.
      def create_result
        ListOffsetsReport.new(
          result_infos: self[:result_infos],
          result_count: self[:result_count]
        )
      end

      # Raises an error if the operation failed
      # @raise [RdkafkaError]
      def raise_error
        raise RdkafkaError.new(
          self[:response],
          broker_message: self[:response_string].read_string
        )
      end
    end
  end
end
