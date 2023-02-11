# frozen_string_literal: true

module Rdkafka
  class Admin
    class DeleteAclReport

      # The error message, or null if the delete Acl filter succeeded.
      # @return [String]
      attr_reader :error_string

      # Deleted Acl resource_type
      # @return [Integer]
      attr_reader :matching_acls_resource_type

      # Deleted Acl resource_name
      # @return [String]
      attr_reader :matching_acls_resource_name

      # Deleted Acl pattern_type
      # @return [Integer]
      attr_reader :matching_acls_pattern_type

      # Deleted Acl principal
      # @return [String]
      attr_reader :matching_acls_principal

      # Deleted Acl host
      # @return [String]
      attr_reader :matching_acls_host

      # Deleted Acl operation
      # @return [Integer]
      attr_reader :matching_acls_operation

      # Deleted Acl permission_type
      # @return [Integer]
      attr_reader :matching_acls_permission_type

      def initialize(error_string:, matching_acls_resource_type:, matching_acls_resource_name:, matching_acls_pattern_type:, matching_acls_principal:, matching_acls_host:, matching_acls_operation:, matching_acls_permission_type:)

        if error_string != FFI::Pointer::NULL
          @error_string = error_string.read_string
        end
        @matching_acls_resource_type = matching_acls_resource_type
        if matching_acls_resource_name != FFI::Pointer::NULL
          @matching_acls_resource_name = matching_acls_resource_name.read_string
        end
        @matching_acls_pattern_type = matching_acls_pattern_type
        if matching_acls_principal != FFI::Pointer::NULL
          @matching_acls_principal = matching_acls_principal.read_string
        end
        if matching_acls_host != FFI::Pointer::NULL
          @matching_acls_host = matching_acls_host.read_string
        end
        @matching_acls_operation = matching_acls_operation
        @matching_acls_permission_type = matching_acls_permission_type
      end
    end
  end
end
