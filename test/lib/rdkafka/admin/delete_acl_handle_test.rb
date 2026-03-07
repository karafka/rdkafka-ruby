# frozen_string_literal: true

require "test_helper"

class DeleteAclHandleTest < Minitest::Test
  def new_subject(pending_handle:)
    resource_name = TestTopics.unique
    error_buffer = FFI::MemoryPointer.from_string(" " * 256)
    delete_acl_ptr = Rdkafka::Bindings.rd_kafka_AclBinding_new(
      Rdkafka::Bindings::RD_KAFKA_RESOURCE_TOPIC,
      FFI::MemoryPointer.from_string(resource_name),
      Rdkafka::Bindings::RD_KAFKA_RESOURCE_PATTERN_LITERAL,
      FFI::MemoryPointer.from_string("User:anonymous"),
      FFI::MemoryPointer.from_string("*"),
      Rdkafka::Bindings::RD_KAFKA_ACL_OPERATION_READ,
      Rdkafka::Bindings::RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW,
      error_buffer,
      256
    )
    raise Rdkafka::Config::ConfigError.new(error_buffer.read_string) if delete_acl_ptr.null?

    pointer_array = [delete_acl_ptr]
    delete_acls_array_ptr = FFI::MemoryPointer.new(:pointer)
    delete_acls_array_ptr.write_array_of_pointer(pointer_array)

    handle = Rdkafka::Admin::DeleteAclHandle.new
    handle[:pending] = pending_handle
    handle[:response] = Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR
    handle[:response_string] = FFI::MemoryPointer.from_string("")
    handle[:matching_acls] = delete_acls_array_ptr
    handle[:matching_acls_count] = 1
    [handle, resource_name]
  end

  def test_wait_raises_timeout
    subject, _ = new_subject(pending_handle: true)
    error = assert_raises(Rdkafka::Admin::DeleteAclHandle::WaitTimeoutError) do
      subject.wait(max_wait_timeout_ms: 100)
    end
    assert_match(/delete acl/, error.message)
  end

  def test_wait_returns_report_when_not_pending
    subject, _ = new_subject(pending_handle: false)
    report = subject.wait

    assert_equal 1, report.deleted_acls.length
  end

  def test_wait_without_timeout
    subject, resource_name = new_subject(pending_handle: false)
    report = subject.wait(max_wait_timeout_ms: nil)

    assert_equal resource_name, report.deleted_acls[0].matching_acl_resource_name
  end

  def test_raise_error
    subject, _ = new_subject(pending_handle: false)
    error = assert_raises(Rdkafka::RdkafkaError) { subject.raise_error }
    assert_match(/Success \(no_error\)/, error.message)
  end
end
