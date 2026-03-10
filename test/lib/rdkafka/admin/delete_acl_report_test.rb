# frozen_string_literal: true

describe Rdkafka::Admin::DeleteAclReport do
  let(:resource_name) { TestTopics.unique }
  let(:resource_type) { Rdkafka::Bindings::RD_KAFKA_RESOURCE_TOPIC }
  let(:resource_pattern_type) { Rdkafka::Bindings::RD_KAFKA_RESOURCE_PATTERN_LITERAL }
  let(:principal) { "User:anonymous" }
  let(:host) { "*" }
  let(:operation) { Rdkafka::Bindings::RD_KAFKA_ACL_OPERATION_READ }
  let(:permission_type) { Rdkafka::Bindings::RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW }

  subject do
    error_buffer = FFI::MemoryPointer.from_string(" " * 256)
    delete_acl_ptr = Rdkafka::Bindings.rd_kafka_AclBinding_new(
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
    raise Rdkafka::Config::ConfigError.new(error_buffer.read_string) if delete_acl_ptr.null?

    pointer_array = [delete_acl_ptr]
    delete_acls_array_ptr = FFI::MemoryPointer.new(:pointer)
    delete_acls_array_ptr.write_array_of_pointer(pointer_array)
    Rdkafka::Admin::DeleteAclReport.new(matching_acls: delete_acls_array_ptr, matching_acls_count: 1)
  end

  it "gets deleted acl resource type as Rdkafka::Bindings::RD_KAFKA_RESOURCE_TOPIC" do
    assert_equal Rdkafka::Bindings::RD_KAFKA_RESOURCE_TOPIC, subject.deleted_acls[0].matching_acl_resource_type
  end

  it "gets deleted acl resource name" do
    assert_equal resource_name, subject.deleted_acls[0].matching_acl_resource_name
  end

  it "gets deleted acl resource pattern type as Rdkafka::Bindings::RD_KAFKA_RESOURCE_PATTERN_LITERAL" do
    assert_equal Rdkafka::Bindings::RD_KAFKA_RESOURCE_PATTERN_LITERAL, subject.deleted_acls[0].matching_acl_resource_pattern_type
    assert_equal Rdkafka::Bindings::RD_KAFKA_RESOURCE_PATTERN_LITERAL, subject.deleted_acls[0].matching_acl_pattern_type
  end

  it "gets deleted acl principal as User:anonymous" do
    assert_equal "User:anonymous", subject.deleted_acls[0].matching_acl_principal
  end

  it "gets deleted acl host as *" do
    assert_equal "*", subject.deleted_acls[0].matching_acl_host
  end

  it "gets deleted acl operation as Rdkafka::Bindings::RD_KAFKA_ACL_OPERATION_READ" do
    assert_equal Rdkafka::Bindings::RD_KAFKA_ACL_OPERATION_READ, subject.deleted_acls[0].matching_acl_operation
  end

  it "gets deleted acl permission_type as Rdkafka::Bindings::RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW" do
    assert_equal Rdkafka::Bindings::RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW, subject.deleted_acls[0].matching_acl_permission_type
  end
end
