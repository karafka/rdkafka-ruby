# frozen_string_literal: true

module Rdkafka
  class Admin
    class DeleteAclHandle < AbstractHandle
      layout :pending, :bool,
             :response, :int,
             :response_string, :pointer,
             :matching_acls, :pointer

      # @return [String] the name of the operation
      def operation_name
        "delete acl"
      end

      # @return [DeleteAclReport] instance with an array of mathcing_acls
      def create_result
        DeleteAclReport.new(matching_acls: self[:matching_acls])
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
