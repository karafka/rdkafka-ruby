# frozen_string_literal: true

module Rdkafka
  class Admin
    class DeleteAclReport

      # matching acls
      # @return [Rdkafka::Bindings::AclBindingResult]
      attr_reader :matching_acls

      def initialize(matching_acls:, matching_acls_count:)
        @matching_acls=[]
        if matching_acls != FFI::Pointer::NULL
          (1..matching_acls_count).map do |matching_acl_index|
            acl_binding_result_pointer = (matching_acls + (matching_acl_index - 1)).read_pointer
            acl_binding_result = AclBindingResult.new(acl_binding_result_pointer)
            @matching_acls << acl_binding_result
          end
        end
      end
    end
  end
end
