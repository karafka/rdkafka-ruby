# frozen_string_literal: true

module Rdkafka
  class Admin
    class DescribeAclReport

      # acls that exists in the cluster for the resource_type, resource_name and pattern_type filters provided in the request.
      # @return [Rdkafka::Bindings::AclBindingResult] array of matching acls.
      attr_reader :acls

      def initialize(acls:, acls_count:)
        @acls=[]
        if acls != FFI::Pointer::NULL
          (1..acls_count).map do |acl_index|
            acl_binding_result_pointer = (acls + (acl_index - 1)).read_pointer
            acl_binding_result = AclBindingResult.new(acl_binding_result_pointer)
            @acls << acl_binding_result
          end
        end
      end
    end
  end
end
