# frozen_string_literal: true

require "spec_helper"

describe Rdkafka::Admin::DeleteAclReport do

  let(:resource_name)         {"acl-test-topic"}
  let(:resource_type)         {Rdkafka::Bindings::RD_KAFKA_RESOURCE_TOPIC}
  let(:resource_pattern_type) {Rdkafka::Bindings::RD_KAFKA_RESOURCE_PATTERN_LITERAL}
  let(:principal)             {"User:anonymous"}
  let(:host)                  {"*"}
  let(:operation)             {Rdkafka::Bindings::RD_KAFKA_ACL_OPERATION_READ}
  let(:permission_type)       {Rdkafka::Bindings::RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW}
  let(:delete_acl_ptr)        {FFI::Pointer::NULL}

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
    Rdkafka::Admin::DeleteAclReport.new(matching_acls: delete_acls_array_ptr, matching_acls_count: 1)
  end

  after do
    if delete_acl_ptr != FFI::Pointer::NULL
      Rdkafka::Bindings.rd_kafka_AclBinding_destroy(delete_acl_ptr)
    end
  end

  it "should get deleted acl resource type as Rdkafka::Bindings::RD_KAFKA_RESOURCE_TOPIC" do
    expect(subject.deleted_acls[0].matching_acl_resource_type).to eq(Rdkafka::Bindings::RD_KAFKA_RESOURCE_TOPIC)
  end

  it "should get deleted acl resource name as acl-test-topic" do
    expect(subject.deleted_acls[0].matching_acl_resource_name).to eq(resource_name)
  end

  it "should get deleted acl resource pattern type as Rdkafka::Bindings::RD_KAFKA_RESOURCE_PATTERN_LITERAL" do
    expect(subject.deleted_acls[0].matching_acl_pattern_type).to eq(Rdkafka::Bindings::RD_KAFKA_RESOURCE_PATTERN_LITERAL)
  end

  it "should get deleted acl principal as User:anonymous" do
    expect(subject.deleted_acls[0].matching_acl_principal).to eq("User:anonymous")
  end

  it "should get deleted  acl host as * " do
    expect(subject.deleted_acls[0].matching_acl_host).to eq("*")
  end

  it "should get deleted acl operation as Rdkafka::Bindings::RD_KAFKA_ACL_OPERATION_READ" do
    expect(subject.deleted_acls[0].matching_acl_operation).to eq(Rdkafka::Bindings::RD_KAFKA_ACL_OPERATION_READ)
  end

  it "should get deleted acl permission_type as Rdkafka::Bindings::RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW" do
    expect(subject.deleted_acls[0].matching_acl_permission_type).to eq(Rdkafka::Bindings::RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW)
  end
end
