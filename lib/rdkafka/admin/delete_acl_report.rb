# frozen_string_literal: true

module Rdkafka
  class Admin
    class DeleteAclReport

      # matching acls
      # @return [Rdkafka::Bindings::AclBindingResult]
      attr_reader :matching_acls

      def initialize(matching_acls:)
        if matching_acls != FFI::Pointer::NULL
          @matching_acls = matching_acls
        end
      end
    end
  end
end
