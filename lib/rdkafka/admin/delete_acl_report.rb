# frozen_string_literal: true

module Rdkafka
  class Admin
    class DeleteAclReport

      # The error message, or null if the delete Acl filter succeeded.
      # @return [String]
      attr_reader :error_string

      # matching acls
      # @return [Rdkafka::Bindings::AclBindingResult]
      attr_reader :matching_acls

      def initialize(error_string:, matching_acls:)

        if error_string != FFI::Pointer::NULL
          @error_string = error_string.read_string
        end
        if matching_acls != FFI::Pointer::NULL
          @matching_acls = matching_acls
        end
      end
    end
  end
end
