# frozen_string_literal: true

module Rdkafka
  class Admin
    # Report for describe ACL operation result
    class DescribeAclReport
      # acls that exists in the cluster for the resource_type, resource_name and pattern_type filters provided in the request.
      # @return [Rdkafka::Bindings::AclBindingResult] array of matching acls.
      attr_reader :acls

      # @param acls [FFI::Pointer] pointer to ACLs array
      # @param acls_count [Integer] number of ACLs
      def initialize(acls:, acls_count:)
        @acls = []

        if acls != FFI::Pointer::NULL
          acl_binding_result_pointers = acls.read_array_of_pointer(acls_count)
          (1..acls_count).map do |acl_index|
            acl_binding_result = AclBindingResult.new(acl_binding_result_pointers[acl_index - 1])
            @acls << acl_binding_result
          end
        end
      end
    end
  end
end
