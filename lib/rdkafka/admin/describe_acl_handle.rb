# frozen_string_literal: true

module Rdkafka
  class Admin
    class DescribeAclHandle < AbstractHandle
      layout :pending, :bool,
             :response, :int,
             :response_string, :pointer,
             :acls, :pointer,
             :acls_count, :int

      # @return [String] the name of the operation.
      def operation_name
        "describe acl"
      end

      # @return [DescribeAclReport] instance with an array of acls that matches the request filters.
      def create_result
        DescribeAclReport.new(acls: self[:acls], acls_count: self[:acls_count])
      end

      def raise_error
        raise RdkafkaError.new(
            self[:response],
            broker_message: self[:response_string].read_string
        )
      end
    end
  end
end
