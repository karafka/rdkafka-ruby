# frozen_string_literal: true

module Rdkafka
  class Admin
    class DescribeAclHandle < AbstractHandle
      layout :pending, :bool,
             :response, :int,
             :response_string, :pointer,
             :acls, :pointer

      # @return [String] the name of the operation
      def operation_name
        "describe acl"
      end

      # @return [DescribeAclReport] instance with resource_type, resource_name, resource_pattern_type and an array of acls
      def create_result
        DescribeAclReport.new(acls: self[:acls])
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
