# frozen_string_literal: true

module Rdkafka
  module Bindings
    # Create Acls
    RD_KAFKA_ADMIN_OP_CREATEACLS = 9
    RD_KAFKA_EVENT_CREATEACLS_RESULT = 1024

    attach_function :rd_kafka_CreateAcls, [:pointer, :pointer, :size_t, :pointer, :pointer], :void
    attach_function :rd_kafka_event_CreateAcls_result, [:pointer], :pointer
    attach_function :rd_kafka_CreateAcls_result_acls, [:pointer, :pointer], :pointer

    # Delete Acls
    RD_KAFKA_ADMIN_OP_DELETEACLS = 11
    RD_KAFKA_EVENT_DELETEACLS_RESULT = 4096

    attach_function :rd_kafka_DeleteAcls, [:pointer, :pointer, :size_t, :pointer, :pointer], :void
    attach_function :rd_kafka_event_DeleteAcls_result, [:pointer], :pointer
    attach_function :rd_kafka_DeleteAcls_result_responses, [:pointer, :pointer], :pointer
    attach_function :rd_kafka_DeleteAcls_result_response_error, [:pointer], :pointer
    attach_function :rd_kafka_DeleteAcls_result_response_matching_acls, [:pointer, :pointer], :pointer

    # Describe Acls
    RD_KAFKA_ADMIN_OP_DESCRIBEACLS = 10
    RD_KAFKA_EVENT_DESCRIBEACLS_RESULT = 2048

    attach_function :rd_kafka_DescribeAcls, [:pointer, :pointer, :pointer, :pointer], :void
    attach_function :rd_kafka_event_DescribeAcls_result, [:pointer], :pointer
    attach_function :rd_kafka_DescribeAcls_result_acls, [:pointer, :pointer], :pointer

    # Acl Bindings

    attach_function :rd_kafka_AclBinding_restype, [:pointer], :int32
    attach_function :rd_kafka_AclBinding_name, [:pointer], :pointer
    attach_function :rd_kafka_AclBinding_resource_pattern_type, [:pointer], :int32
    attach_function :rd_kafka_AclBinding_principal, [:pointer], :pointer
    attach_function :rd_kafka_AclBinding_host, [:pointer], :pointer
    attach_function :rd_kafka_AclBinding_operation, [:pointer], :int32
    attach_function :rd_kafka_AclBinding_permission_type, [:pointer], :int32
    attach_function :rd_kafka_AclBinding_new, [:int32, :pointer, :int32, :pointer, :pointer, :int32, :int32, :pointer, :size_t], :pointer
    attach_function :rd_kafka_AclBindingFilter_new, [:int32, :pointer, :int32, :pointer, :pointer, :int32, :int32, :pointer, :size_t], :pointer
    attach_function :rd_kafka_AclBinding_destroy, [:pointer], :void

    # rd_kafka_ResourceType_t -
    # https://github.com/confluentinc/librdkafka/blob/292d2a66b992/src/rdkafka.h#L7307
    RD_KAFKA_RESOURCE_ANY = 1
    RD_KAFKA_RESOURCE_TOPIC = 2
    RD_KAFKA_RESOURCE_GROUP = 3
    RD_KAFKA_RESOURCE_BROKER = 4
    RD_KAFKA_RESOURCE_TRANSACTIONAL_ID = 5

    # rd_kafka_ResourcePatternType_t -
    # https://github.com/confluentinc/librdkafka/blob/292d2a66b992/src/rdkafka.h#L7320
    RD_KAFKA_RESOURCE_PATTERN_UNKNOWN = 0
    RD_KAFKA_RESOURCE_PATTERN_ANY = 1
    RD_KAFKA_RESOURCE_PATTERN_MATCH = 2
    RD_KAFKA_RESOURCE_PATTERN_LITERAL = 3
    RD_KAFKA_RESOURCE_PATTERN_PREFIXED = 4

    # rd_kafka_AclOperation_t -
    # https://github.com/confluentinc/librdkafka/blob/292d2a66b992/src/rdkafka.h#L8403
    RD_KAFKA_ACL_OPERATION_ANY = 1
    RD_KAFKA_ACL_OPERATION_ALL = 2
    RD_KAFKA_ACL_OPERATION_READ = 3
    RD_KAFKA_ACL_OPERATION_WRITE = 4
    RD_KAFKA_ACL_OPERATION_CREATE = 5
    RD_KAFKA_ACL_OPERATION_DELETE = 6
    RD_KAFKA_ACL_OPERATION_ALTER = 7
    RD_KAFKA_ACL_OPERATION_DESCRIBE = 8
    RD_KAFKA_ACL_OPERATION_CLUSTER_ACTION = 9
    RD_KAFKA_ACL_OPERATION_DESCRIBE_CONFIGS = 10
    RD_KAFKA_ACL_OPERATION_ALTER_CONFIGS = 11
    RD_KAFKA_ACL_OPERATION_IDEMPOTENT_WRITE = 12

    # rd_kafka_AclPermissionType_t -
    # https://github.com/confluentinc/librdkafka/blob/292d2a66b992/src/rdkafka.h#L8435
    RD_KAFKA_ACL_PERMISSION_TYPE_ANY = 1
    RD_KAFKA_ACL_PERMISSION_TYPE_DENY = 2
    RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW = 3

    # Extracting error details from Acl results
    attach_function :rd_kafka_acl_result_error, [:pointer], :pointer
    attach_function :rd_kafka_error_code, [:pointer], :int32
    attach_function :rd_kafka_error_string, [:pointer], :pointer
    attach_function :rd_kafka_event_error, [:pointer], :int32
    attach_function :rd_kafka_event_error_string, [:pointer], :pointer
    attach_function :rd_kafka_AclBinding_error, [:pointer], :pointer
  end
end
