# frozen_string_literal: true

module Rdkafka
  class Admin
    class CreateACLHandle < AbstractHandle
      layout :pending, :bool,
             :response, :int,
             :error_string, :pointer,
             :error_code, :int

      # @return [String] the name of the operation
      def operation_name
        'create ACL'
      end

      # @return [Boolean] whether the create ACL was successful
      def create_result
        CreateACLReport.new(self[:error_string], self[:error_code])
      end

      def raise_error
        raise RdkafkaError.new(
          self[:response],
          CreateACLReport.new(self[:error_string], self[:error_code]).error_string
        )
      end
    end
  end
end
