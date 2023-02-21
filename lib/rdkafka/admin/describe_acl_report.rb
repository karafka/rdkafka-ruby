# frozen_string_literal: true

module Rdkafka
  class Admin
    class DescribeAclReport

      # acls that exists in the cluster for the resource_type, resource_name and pattern_type of this instance.
      # @return [Rdkafka::Bindings::AclBindingResult]
      attr_reader :acls

      def initialize(acls:)
        if acls != FFI::Pointer::NULL
          @acls = acls
        end
      end
    end
  end
end
