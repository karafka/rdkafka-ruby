# frozen_string_literal: true

module Rdkafka
  class Admin
    # Handle for describe ACL operation
    class DescribeAclHandle < AbstractHandle
      layout :pending, :bool,
        :response, :int

      # @return [String] the name of the operation.
      def operation_name
        "describe acl"
      end

      # @return [DescribeAclReport] report prepared by the background event callback, with an
      #   array of acls that matches the request filters.
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
