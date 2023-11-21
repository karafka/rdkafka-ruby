# frozen_string_literal: true

require "spec_helper"

describe Rdkafka::Admin::DeleteAclHandle do
  let(:response) { Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR }
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
    Rdkafka::Admin::DeleteAclHandle.new.tap do |handle|
      handle[:pending] = pending_handle
      handle[:response] = response
      handle[:response_string] = FFI::MemoryPointer.from_string("")
      handle[:matching_acls] = delete_acls_array_ptr
      handle[:matching_acls_count] = 1
    end
  end

  after do
    if delete_acl_ptr != FFI::Pointer::NULL
      Rdkafka::Bindings.rd_kafka_AclBinding_destroy(delete_acl_ptr)
    end
  end

  describe "#wait" do
    let(:pending_handle) { true }

    it "should wait until the timeout and then raise an error" do
      expect {
        subject.wait(max_wait_timeout: 0.1)
      }.to raise_error Rdkafka::Admin::DeleteAclHandle::WaitTimeoutError, /delete acl/
    end

    context "when not pending anymore and no error" do
      let(:pending_handle) { false }

      it "should return a delete acl report" do
        report = subject.wait

        expect(report.deleted_acls.length).to eq(1)
      end

      it "should wait without a timeout" do
        report = subject.wait(max_wait_timeout: nil)

        expect(report.deleted_acls[0].matching_acl_resource_name).to eq(resource_name)
      end
    end
  end

  describe "#raise_error" do
    let(:pending_handle) { false }

    it "should raise the appropriate error" do
      expect {
        subject.raise_error
      }.to raise_exception(Rdkafka::RdkafkaError, /Success \(no_error\)/)
    end
  end
end
