# frozen_string_literal: true

module Rdkafka
  class Admin
    class DeleteAclHandle < AbstractHandle
      layout :pending, :bool,
             :response, :int,
             :error_string, :pointer,
             :matching_acls_resource_type, :int,
             :matching_acls_resource_name, :pointer,
             :matching_acls_pattern_type, :int,
             :matching_acls_principal, :pointer,
             :matching_acls_host, :pointer,
             :matching_acls_operation, :int,
             :matching_acls_permission_type, :int

      # @return [String] the name of the operation
      def operation_name
        "delete acl"
      end

      # @return [Boolean] whether the delete acl was successful
      def create_result
        DeleteAclReport.new(error_string: self[:error_string], matching_acls_resource_type: self[:matching_acls_resource_type], matching_acls_resource_name: self[:matching_acls_resource_name], matching_acls_pattern_type: self[:matching_acls_pattern_type], matching_acls_principal: self[:matching_acls_principal], matching_acls_host: self[:matching_acls_host], matching_acls_operation: self[:matching_acls_operation], matching_acls_permission_type: self[:matching_acls_permission_type])
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
