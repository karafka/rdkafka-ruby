# frozen_string_literal: true

module Rdkafka
  class Admin
    class DescribeAclReport

      # acls that exists in the cluster for the resource_type, resource_name and pattern_type filters provided in the request.
      # @return [Rdkafka::Bindings::AclBindingResult] array of matching acls
      attr_reader :acls

      def initialize(acls:)
        if acls != FFI::Pointer::NULL
          @acls = acls
        end
      end
    end
  end
end
