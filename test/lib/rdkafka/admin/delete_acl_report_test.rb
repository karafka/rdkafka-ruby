# frozen_string_literal: true

require_relative "../../../test_helper"

describe Rdkafka::Admin::DeleteAclReport do
  before do
    @resource_name = TestTopics.unique
    @resource_type = Rdkafka::Bindings::RD_KAFKA_RESOURCE_TOPIC
    @resource_pattern_type = Rdkafka::Bindings::RD_KAFKA_RESOURCE_PATTERN_LITERAL
    @principal = "User:anonymous"
    @host = "*"
    @operation = Rdkafka::Bindings::RD_KAFKA_ACL_OPERATION_READ
    @permission_type = Rdkafka::Bindings::RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW

    error_buffer = FFI::MemoryPointer.from_string(" " * 256)
    delete_acl_ptr = Rdkafka::Bindings.rd_kafka_AclBinding_new(
      @resource_type,
      FFI::MemoryPointer.from_string(@resource_name),
      @resource_pattern_type,
      FFI::MemoryPointer.from_string(@principal),
      FFI::MemoryPointer.from_string(@host),
      @operation,
      @permission_type,
      error_buffer,
      256
    )
    if delete_acl_ptr.null?
      raise Rdkafka::Config::ConfigError.new(error_buffer.read_string)
    end
    pointer_array = [delete_acl_ptr]
    delete_acls_array_ptr = FFI::MemoryPointer.new(:pointer)
    delete_acls_array_ptr.write_array_of_pointer(pointer_array)
    @report = described_class.new(matching_acls: delete_acls_array_ptr, matching_acls_count: 1)
  end

  it "gets deleted acl resource type as Rdkafka::Bindings::RD_KAFKA_RESOURCE_TOPIC" do
    assert_equal Rdkafka::Bindings::RD_KAFKA_RESOURCE_TOPIC, @report.deleted_acls[0].matching_acl_resource_type
  end

  it "gets deleted acl resource name" do
    assert_equal @resource_name, @report.deleted_acls[0].matching_acl_resource_name
  end

  it "gets deleted acl resource pattern type as Rdkafka::Bindings::RD_KAFKA_RESOURCE_PATTERN_LITERAL" do
    assert_equal Rdkafka::Bindings::RD_KAFKA_RESOURCE_PATTERN_LITERAL, @report.deleted_acls[0].matching_acl_resource_pattern_type
    assert_equal Rdkafka::Bindings::RD_KAFKA_RESOURCE_PATTERN_LITERAL, @report.deleted_acls[0].matching_acl_pattern_type
  end

  it "gets deleted acl principal as User:anonymous" do
    assert_equal "User:anonymous", @report.deleted_acls[0].matching_acl_principal
  end

  it "gets deleted acl host as *" do
    assert_equal "*", @report.deleted_acls[0].matching_acl_host
  end

  it "gets deleted acl operation as Rdkafka::Bindings::RD_KAFKA_ACL_OPERATION_READ" do
    assert_equal Rdkafka::Bindings::RD_KAFKA_ACL_OPERATION_READ, @report.deleted_acls[0].matching_acl_operation
  end

  it "gets deleted acl permission_type as Rdkafka::Bindings::RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW" do
    assert_equal Rdkafka::Bindings::RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW, @report.deleted_acls[0].matching_acl_permission_type
  end
end
