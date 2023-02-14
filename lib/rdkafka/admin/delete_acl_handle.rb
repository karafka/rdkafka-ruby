# frozen_string_literal: true

module Rdkafka
  class Admin
    class DeleteAclHandle < AbstractHandle
      layout :pending, :bool,
             :response, :int,
             :error_string, :pointer,
             :matching_acls, :pointer

      # @return [String] the name of the operation
      def operation_name
        "delete acl"
      end

      # @return [Boolean] whether the delete acl was successful
      def create_result
        DeleteAclReport.new(error_string: self[:error_string], matching_acls: self[:matching_acls])
      end

      def raise_error
        raise RdkafkaError.new(
            self[:response],
            broker_message: DeleteAclReport.new(self[:error_string]).error_string
        )
      end
    end
  end
end
