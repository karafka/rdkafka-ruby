# frozen_string_literal: true

module Rdkafka
  class Admin
    class DescribeConfigsHandle < AbstractHandle
      layout :pending, :bool,
             :response, :int,
             :response_string, :pointer,
             :config_entries, :pointer,
             :entry_count, :int

      # @return [String] the name of the operation.
      def operation_name
        "describe configs"
      end

      # @return [DescribeAclReport] instance with an array of acls that matches the request filters.
      def create_result
        DescribeConfigsReport.new(
          config_entries: self[:config_entries],
          entry_count: self[:entry_count]
        )
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
