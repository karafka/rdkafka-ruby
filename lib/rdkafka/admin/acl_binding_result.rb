# frozen_string_literal: true

module Rdkafka
  class Admin

    # Extracts attributes of rd_kafka_AclBinding_t
    #
    class AclBindingResult
      attr_reader :result_error, :error_string, :matching_acl_resource_type, :matching_acl_resource_name, :matching_acl_pattern_type, :matching_acl_principal, :matching_acl_host, :matching_acl_operation, :matching_acl_permission_type

      def initialize(matching_acl)
          rd_kafka_error_pointer        = Rdkafka::Bindings.rd_kafka_AclBinding_error(matching_acl)
          @result_error                 = Rdkafka::Bindings.rd_kafka_error_code(rd_kafka_error_pointer)
          error_string                 = Rdkafka::Bindings.rd_kafka_error_string(rd_kafka_error_pointer)
          if error_string != FFI::Pointer::NULL
            @error_string = error_string.read_string
          end
          @matching_acl_resource_type   = Rdkafka::Bindings.rd_kafka_AclBinding_restype(matching_acl)
          matching_acl_resource_name    = Rdkafka::Bindings.rd_kafka_AclBinding_name(matching_acl)
          if matching_acl_resource_name != FFI::Pointer::NULL
            @matching_acl_resource_name = matching_acl_resource_name.read_string
          end
          @matching_acl_pattern_type    = Rdkafka::Bindings.rd_kafka_AclBinding_resource_pattern_type(matching_acl)
          matching_acl_principal        = Rdkafka::Bindings.rd_kafka_AclBinding_principal(matching_acl)
          if matching_acl_principal != FFI::Pointer::NULL
            @matching_acl_principal = matching_acl_principal.read_string
          end
          matching_acl_host             = Rdkafka::Bindings.rd_kafka_AclBinding_host(matching_acl)
          if matching_acl_host != FFI::Pointer::NULL
            @matching_acl_host = matching_acl_host.read_string
          end
          @matching_acl_operation       = Rdkafka::Bindings.rd_kafka_AclBinding_operation(matching_acl)
          @matching_acl_permission_type = Rdkafka::Bindings.rd_kafka_AclBinding_permission_type(matching_acl)
        end
      end
  end
end
