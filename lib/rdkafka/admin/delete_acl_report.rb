# frozen_string_literal: true

module Rdkafka
  class Admin
    class DeleteAclReport

      # deleted acls
      # @return [Rdkafka::Bindings::AclBindingResult]
      attr_reader :deleted_acls

      def initialize(matching_acls:, matching_acls_count:)
        @deleted_acls=[]
        if matching_acls != FFI::Pointer::NULL
          acl_binding_result_pointers  = matching_acls.read_array_of_pointer(matching_acls_count)
          (1..matching_acls_count).map do |matching_acl_index|
            acl_binding_result = AclBindingResult.new(acl_binding_result_pointers[matching_acl_index - 1])
            @deleted_acls << acl_binding_result
          end
        end
      end
    end
  end
end
