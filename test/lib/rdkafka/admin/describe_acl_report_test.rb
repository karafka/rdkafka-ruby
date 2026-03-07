# frozen_string_literal: true

require "test_helper"

class DescribeAclReportTest < Minitest::Test
  def setup
    super
    @resource_name = TestTopics.unique
    error_buffer = FFI::MemoryPointer.from_string(" " * 256)
    describe_acl_ptr = Rdkafka::Bindings.rd_kafka_AclBinding_new(
      Rdkafka::Bindings::RD_KAFKA_RESOURCE_TOPIC,
      FFI::MemoryPointer.from_string(@resource_name),
      Rdkafka::Bindings::RD_KAFKA_RESOURCE_PATTERN_LITERAL,
      FFI::MemoryPointer.from_string("User:anonymous"),
      FFI::MemoryPointer.from_string("*"),
      Rdkafka::Bindings::RD_KAFKA_ACL_OPERATION_READ,
      Rdkafka::Bindings::RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW,
      error_buffer,
      256
    )
    raise Rdkafka::Config::ConfigError.new(error_buffer.read_string) if describe_acl_ptr.null?

    pointer_array = [describe_acl_ptr]
    describe_acls_array_ptr = FFI::MemoryPointer.new(:pointer)
    describe_acls_array_ptr.write_array_of_pointer(pointer_array)
    @subject = Rdkafka::Admin::DescribeAclReport.new(acls: describe_acls_array_ptr, acls_count: 1)
  end

  def test_gets_matching_acl_resource_type
    assert_equal Rdkafka::Bindings::RD_KAFKA_RESOURCE_TOPIC, @subject.acls[0].matching_acl_resource_type
  end

  def test_gets_matching_acl_resource_name
    assert_equal @resource_name, @subject.acls[0].matching_acl_resource_name
  end

  def test_gets_matching_acl_resource_pattern_type
    assert_equal Rdkafka::Bindings::RD_KAFKA_RESOURCE_PATTERN_LITERAL, @subject.acls[0].matching_acl_resource_pattern_type
    assert_equal Rdkafka::Bindings::RD_KAFKA_RESOURCE_PATTERN_LITERAL, @subject.acls[0].matching_acl_pattern_type
  end

  def test_gets_matching_acl_principal
    assert_equal "User:anonymous", @subject.acls[0].matching_acl_principal
  end

  def test_gets_matching_acl_host
    assert_equal "*", @subject.acls[0].matching_acl_host
  end

  def test_gets_matching_acl_operation
    assert_equal Rdkafka::Bindings::RD_KAFKA_ACL_OPERATION_READ, @subject.acls[0].matching_acl_operation
  end

  def test_gets_matching_acl_permission_type
    assert_equal Rdkafka::Bindings::RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW, @subject.acls[0].matching_acl_permission_type
  end
end
