# frozen_string_literal: true

module Rdkafka
  class Admin
    # Handle for create ACL operation
    class CreateAclHandle < AbstractHandle
      layout :pending, :bool,
        :response, :int

      # @return [String] the name of the operation
      def operation_name
        "create acl"
      end

      # @return [CreateAclReport] report prepared by the background event callback, with
      #   rdkafka_response value as 0 and rdkafka_response_string value as empty string if the
      #   acl creation was successful
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
