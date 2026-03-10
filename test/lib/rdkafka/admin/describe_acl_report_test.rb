# frozen_string_literal: true

describe Rdkafka::Admin::DescribeAclReport do
  let(:resource_name) { TestTopics.unique }
  let(:resource_type) { Rdkafka::Bindings::RD_KAFKA_RESOURCE_TOPIC }
  let(:resource_pattern_type) { Rdkafka::Bindings::RD_KAFKA_RESOURCE_PATTERN_LITERAL }
  let(:principal) { "User:anonymous" }
  let(:host) { "*" }
  let(:operation) { Rdkafka::Bindings::RD_KAFKA_ACL_OPERATION_READ }
  let(:permission_type) { Rdkafka::Bindings::RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW }

  subject do
    error_buffer = FFI::MemoryPointer.from_string(" " * 256)
    describe_acl_ptr = Rdkafka::Bindings.rd_kafka_AclBinding_new(
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
    raise Rdkafka::Config::ConfigError.new(error_buffer.read_string) if describe_acl_ptr.null?

    pointer_array = [describe_acl_ptr]
    describe_acls_array_ptr = FFI::MemoryPointer.new(:pointer)
    describe_acls_array_ptr.write_array_of_pointer(pointer_array)
    described_class.new(acls: describe_acls_array_ptr, acls_count: 1)
  end

  it "gets matching acl resource type as Rdkafka::Bindings::RD_KAFKA_RESOURCE_TOPIC" do
    assert_equal Rdkafka::Bindings::RD_KAFKA_RESOURCE_TOPIC, subject.acls[0].matching_acl_resource_type
  end

  it "gets matching acl resource name" do
    assert_equal resource_name, subject.acls[0].matching_acl_resource_name
  end

  it "gets matching acl resource pattern type as Rdkafka::Bindings::RD_KAFKA_RESOURCE_PATTERN_LITERAL" do
    assert_equal Rdkafka::Bindings::RD_KAFKA_RESOURCE_PATTERN_LITERAL, subject.acls[0].matching_acl_resource_pattern_type
    assert_equal Rdkafka::Bindings::RD_KAFKA_RESOURCE_PATTERN_LITERAL, subject.acls[0].matching_acl_pattern_type
  end

  it "gets matching acl principal as User:anonymous" do
    assert_equal "User:anonymous", subject.acls[0].matching_acl_principal
  end

  it "gets matching acl host as *" do
    assert_equal "*", subject.acls[0].matching_acl_host
  end

  it "gets matching acl operation as Rdkafka::Bindings::RD_KAFKA_ACL_OPERATION_READ" do
    assert_equal Rdkafka::Bindings::RD_KAFKA_ACL_OPERATION_READ, subject.acls[0].matching_acl_operation
  end

  it "gets matching acl permission_type as Rdkafka::Bindings::RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW" do
    assert_equal Rdkafka::Bindings::RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW, subject.acls[0].matching_acl_permission_type
  end
end
