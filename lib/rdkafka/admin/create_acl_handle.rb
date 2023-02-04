# frozen_string_literal: true

module Rdkafka
  class Admin
    class CreateAclHandle < AbstractHandle
      layout :pending, :bool,
             :response, :int,
             :error_string, :pointer

      # @return [String] the name of the operation
      def operation_name
        "create acl"
      end

      # @return [Boolean] whether the create topic was successful
      def create_result
        CreateAclReport.new(self[:error_string])
      end

      def raise_error
        raise RdkafkaError.new(
            self[:response],
            broker_message: CreateAclReport.new(self[:error_string]).error_string
        )
      end
    end
  end
end
