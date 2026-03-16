# frozen_string_literal: true

# Shared helpers for handle spec tests.
# These are plain Ruby methods that can be reused in minitest without translation.
module HandleTestHelpers
  extend self

  # Build a topic handle (CreateTopicHandle or DeleteTopicHandle) with the given fields.
  def build_topic_handle(klass, pending:, response: 0, topic_name: TestTopics.unique)
    klass.new.tap do |handle|
      handle[:pending] = pending
      handle[:response] = response
      handle[:error_string] = FFI::Pointer::NULL
      handle[:result_name] = FFI::MemoryPointer.from_string(topic_name)
    end
  end

  # Build a CreateAclHandle with the given fields.
  def build_create_acl_handle(pending:, response: Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR)
    Rdkafka::Admin::CreateAclHandle.new.tap do |handle|
      handle[:pending] = pending
      handle[:response] = response
      handle[:response_string] = FFI::MemoryPointer.from_string("")
    end
  end

  # Build a ListOffsetsHandle with the given fields.
  def build_list_offsets_handle(pending:, response: Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR)
    Rdkafka::Admin::ListOffsetsHandle.new.tap do |handle|
      handle[:pending] = pending
      handle[:response] = response
      handle[:response_string] = FFI::MemoryPointer.from_string("")
      handle[:result_infos] = FFI::Pointer::NULL
      handle[:result_count] = 0
    end
  end

  # Build a DeliveryHandle with the given fields.
  def build_delivery_handle(pending:, response: 0, partition: 2, offset: 100, topic: TestTopics.unique)
    Rdkafka::Producer::DeliveryHandle.new.tap do |handle|
      handle[:pending] = pending
      handle[:response] = response
      handle[:partition] = partition
      handle[:offset] = offset
      handle.topic = topic
    end
  end

  # Build an ACL binding pointer for use in DeleteAcl/DescribeAcl handle and report tests.
  def build_acl_pointer(
    resource_name: TestTopics.unique,
    resource_type: Rdkafka::Bindings::RD_KAFKA_RESOURCE_TOPIC,
    resource_pattern_type: Rdkafka::Bindings::RD_KAFKA_RESOURCE_PATTERN_LITERAL,
    principal: "User:anonymous",
    host: "*",
    operation: Rdkafka::Bindings::RD_KAFKA_ACL_OPERATION_READ,
    permission_type: Rdkafka::Bindings::RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW
  )
    error_buffer = FFI::MemoryPointer.from_string(" " * 256)
    ptr = Rdkafka::Bindings.rd_kafka_AclBinding_new(
      resource_type,
      FFI::MemoryPointer.from_string(resource_name),
      resource_pattern_type,
      FFI::MemoryPointer.from_string(principal),
      FFI::MemoryPointer.from_string(host),
      operation,
      permission_type,
      error_buffer,
      256
    )
    raise Rdkafka::Config::ConfigError.new(error_buffer.read_string) if ptr.null?
    ptr
  end

  # Build an ACL pointer array from a single pointer (for DeleteAcl/DescribeAcl handles).
  def build_acl_pointer_array(acl_ptr)
    pointer_array = [acl_ptr]
    array_ptr = FFI::MemoryPointer.new(:pointer)
    array_ptr.write_array_of_pointer(pointer_array)
    array_ptr
  end

  # Build a DeleteAclHandle with the given fields.
  def build_delete_acl_handle(pending:, response: Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR, **acl_opts)
    acl_ptr = build_acl_pointer(**acl_opts)
    acls_array_ptr = build_acl_pointer_array(acl_ptr)

    Rdkafka::Admin::DeleteAclHandle.new.tap do |handle|
      handle[:pending] = pending
      handle[:response] = response
      handle[:response_string] = FFI::MemoryPointer.from_string("")
      handle[:matching_acls] = acls_array_ptr
      handle[:matching_acls_count] = 1
    end
  end

  # Build a DescribeAclHandle with the given fields.
  def build_describe_acl_handle(pending:, response: Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR, **acl_opts)
    acl_ptr = build_acl_pointer(**acl_opts)
    acls_array_ptr = build_acl_pointer_array(acl_ptr)

    Rdkafka::Admin::DescribeAclHandle.new.tap do |handle|
      handle[:pending] = pending
      handle[:response] = response
      handle[:response_string] = FFI::MemoryPointer.from_string("")
      handle[:acls] = acls_array_ptr
      handle[:acls_count] = 1
    end
  end

  # Default ACL attributes for testing.
  def default_acl_attributes
    {
      resource_name: TestTopics.unique,
      resource_type: Rdkafka::Bindings::RD_KAFKA_RESOURCE_TOPIC,
      resource_pattern_type: Rdkafka::Bindings::RD_KAFKA_RESOURCE_PATTERN_LITERAL,
      principal: "User:anonymous",
      host: "*",
      operation: Rdkafka::Bindings::RD_KAFKA_ACL_OPERATION_READ,
      permission_type: Rdkafka::Bindings::RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW
    }
  end
end
