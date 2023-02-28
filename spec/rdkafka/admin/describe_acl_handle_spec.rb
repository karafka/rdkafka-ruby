# frozen_string_literal: true

require "spec_helper"

describe Rdkafka::Admin::DescribeAclHandle do
  let(:response) { Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR }
  let(:resource_name)         {"acl-test-topic"}
  let(:resource_type)         {Rdkafka::Bindings::RD_KAFKA_RESOURCE_TOPIC}
  let(:resource_pattern_type) {Rdkafka::Bindings::RD_KAFKA_RESOURCE_PATTERN_LITERAL}
  let(:principal)             {"User:anonymous"}
  let(:host)                  {"*"}
  let(:operation)             {Rdkafka::Bindings::RD_KAFKA_ACL_OPERATION_READ}
  let(:permission_type)       {Rdkafka::Bindings::RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW}

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
    pointer_array = [describe_acl_ptr]
    describe_acls_array_ptr = FFI::MemoryPointer.new(:pointer)
    describe_acls_array_ptr.write_array_of_pointer(pointer_array)
    Rdkafka::Admin::DescribeAclHandle.new.tap do |handle|
      handle[:pending] = pending_handle
      handle[:response] = response
      handle[:response_string] = FFI::MemoryPointer.from_string("")
      handle[:acls] = describe_acls_array_ptr
      handle[:acls_count] = 1
    end
  end

  describe "#wait" do
    let(:pending_handle) { true }

    it "should wait until the timeout and then raise an error" do
      expect {
        subject.wait(max_wait_timeout: 0.1)
      }.to raise_error Rdkafka::Admin::DescribeAclHandle::WaitTimeoutError, /describe acl/
    end

    context "when not pending anymore and no error" do
      let(:pending_handle) { false }

      it "should return a describe acl report" do
        report = subject.wait

        expect(report.acls.length).to eq(1)
      end

      it "should wait without a timeout" do
        report = subject.wait(max_wait_timeout: nil)

        expect(report.acls[0].matching_acl_resource_name).to eq("acl-test-topic")
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
