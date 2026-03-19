# frozen_string_literal: true

require_relative "../../../test_helper"

describe Rdkafka::Admin::DeleteAclHandle do
  before do
    @resource_name = TestTopics.unique
    @resource_type = Rdkafka::Bindings::RD_KAFKA_RESOURCE_TOPIC
    @resource_pattern_type = Rdkafka::Bindings::RD_KAFKA_RESOURCE_PATTERN_LITERAL
    @principal = "User:anonymous"
    @host = "*"
    @operation = Rdkafka::Bindings::RD_KAFKA_ACL_OPERATION_READ
    @permission_type = Rdkafka::Bindings::RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW
    @response = Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR
  end

  def build_handle(pending:)
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
    described_class.new.tap do |handle|
      handle[:pending] = pending
      handle[:response] = @response
      handle[:response_string] = FFI::MemoryPointer.from_string("")
      handle[:matching_acls] = delete_acls_array_ptr
      handle[:matching_acls_count] = 1
    end
  end

  describe "#wait" do
    context "when pending" do
      before do
        @handle = build_handle(pending: true)
      end

      it "waits until the timeout and then raise an error" do
        e = assert_raises(Rdkafka::Admin::DeleteAclHandle::WaitTimeoutError) {
          @handle.wait(max_wait_timeout_ms: 100)
        }
        assert_match(/delete acl/, e.message)
      end
    end

    context "when not pending anymore and no error" do
      before do
        @handle = build_handle(pending: false)
      end

      it "returns a delete acl report" do
        report = @handle.wait

        assert_equal 1, report.deleted_acls.length
      end

      it "waits without a timeout" do
        report = @handle.wait(max_wait_timeout_ms: nil)

        assert_equal @resource_name, report.deleted_acls[0].matching_acl_resource_name
      end
    end
  end

  describe "#raise_error" do
    before do
      @handle = build_handle(pending: false)
    end

    it "raises the appropriate error" do
      e = assert_raises(Rdkafka::RdkafkaError) { @handle.raise_error }
      assert_match(/Success \(no_error\)/, e.message)
    end
  end
end
