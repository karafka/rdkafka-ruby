# frozen_string_literal: true

module Rdkafka
  class Admin
    class CreateAclHandle < AbstractHandle
      layout :pending, :bool,
             :response, :int,
             :response_string, :pointer

      # @return [String] the name of the operation
      def operation_name
        "create acl"
      end

      # @return [Boolean] whether the create topic was successful
      def create_result
        CreateAclReport.new(rdkafka_response: self[:response], rdkafka_response_string: self[:response_string])
      end

      def raise_error
        raise RdkafkaError.new(
            self[:response],
            broker_message: self[:error_string].read_string
        )
      end
    end
  end
end
