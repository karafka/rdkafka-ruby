# frozen_string_literal: true

RSpec.describe Rdkafka::Admin::DeleteAclReport do
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
    if delete_acl_ptr.null?
      raise Rdkafka::Config::ConfigError.new(error_buffer.read_string)
    end
    pointer_array = [delete_acl_ptr]
    delete_acls_array_ptr = FFI::MemoryPointer.new(:pointer)
    delete_acls_array_ptr.write_array_of_pointer(pointer_array)
    described_class.new(matching_acls: delete_acls_array_ptr, matching_acls_count: 1)
  end

  let(:resource_name) { TestTopics.unique }
  let(:resource_type) { Rdkafka::Bindings::RD_KAFKA_RESOURCE_TOPIC }
  let(:resource_pattern_type) { Rdkafka::Bindings::RD_KAFKA_RESOURCE_PATTERN_LITERAL }
  let(:principal) { "User:anonymous" }
  let(:host) { "*" }
  let(:operation) { Rdkafka::Bindings::RD_KAFKA_ACL_OPERATION_READ }
  let(:permission_type) { Rdkafka::Bindings::RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW }
  let(:delete_acl_ptr) { FFI::Pointer::NULL }

  after do
    if delete_acl_ptr != FFI::Pointer::NULL
      Rdkafka::Bindings.rd_kafka_AclBinding_destroy(delete_acl_ptr)
    end
  end

  it "gets deleted acl resource type as Rdkafka::Bindings::RD_KAFKA_RESOURCE_TOPIC" do
    expect(subject.deleted_acls[0].matching_acl_resource_type).to eq(Rdkafka::Bindings::RD_KAFKA_RESOURCE_TOPIC)
  end

  it "gets deleted acl resource name" do
    expect(subject.deleted_acls[0].matching_acl_resource_name).to eq(resource_name)
  end

  it "gets deleted acl resource pattern type as Rdkafka::Bindings::RD_KAFKA_RESOURCE_PATTERN_LITERAL" do
    expect(subject.deleted_acls[0].matching_acl_resource_pattern_type).to eq(Rdkafka::Bindings::RD_KAFKA_RESOURCE_PATTERN_LITERAL)
    expect(subject.deleted_acls[0].matching_acl_pattern_type).to eq(Rdkafka::Bindings::RD_KAFKA_RESOURCE_PATTERN_LITERAL)
  end

  it "gets deleted acl principal as User:anonymous" do
    expect(subject.deleted_acls[0].matching_acl_principal).to eq("User:anonymous")
  end

  it "gets deleted acl host as *" do
    expect(subject.deleted_acls[0].matching_acl_host).to eq("*")
  end

  it "gets deleted acl operation as Rdkafka::Bindings::RD_KAFKA_ACL_OPERATION_READ" do
    expect(subject.deleted_acls[0].matching_acl_operation).to eq(Rdkafka::Bindings::RD_KAFKA_ACL_OPERATION_READ)
  end

  it "gets deleted acl permission_type as Rdkafka::Bindings::RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW" do
    expect(subject.deleted_acls[0].matching_acl_permission_type).to eq(Rdkafka::Bindings::RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW)
  end
end
