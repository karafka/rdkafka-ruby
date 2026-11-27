# frozen_string_literal: true

describe Rdkafka::Admin::DescribeAclHandle do
  before do
    @resource_name = TestTopics.unique
    @resource_type = Rdkafka::Bindings::RD_KAFKA_RESOURCE_TOPIC
    @resource_pattern_type = Rdkafka::Bindings::RD_KAFKA_RESOURCE_PATTERN_LITERAL
    @principal = "User:anonymous"
    @host = "*"
    @operation = Rdkafka::Bindings::RD_KAFKA_ACL_OPERATION_READ
    @permission_type = Rdkafka::Bindings::RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW
    @describe_acl_ptr = FFI::Pointer::NULL
    @response = Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR
  end

  after do
    if @describe_acl_ptr != FFI::Pointer::NULL
      Rdkafka::Bindings.rd_kafka_AclBinding_destroy(@describe_acl_ptr)
    end
  end

  def build_handle(pending_handle)
    error_buffer = FFI::MemoryPointer.from_string(" " * 256)
    describe_acl_ptr = Rdkafka::Bindings.rd_kafka_AclBinding_new(
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
    if describe_acl_ptr.null?
      raise Rdkafka::Config::ConfigError.new(error_buffer.read_string)
    end
    pointer_array = [describe_acl_ptr]
    describe_acls_array_ptr = FFI::MemoryPointer.new(:pointer)
    describe_acls_array_ptr.write_array_of_pointer(pointer_array)
    described_class.new.tap do |handle|
      handle[:pending] = pending_handle
      handle[:response] = @response
      handle.broker_message = ""
      handle.result = Rdkafka::Admin::DescribeAclReport.new(
        acls: describe_acls_array_ptr,
        acls_count: 1
      )
    end
  end

  describe "#wait" do
    before { @handle = build_handle(true) }

    it "waits until the timeout and then raise an error" do
      error = assert_raises(Rdkafka::Admin::DescribeAclHandle::WaitTimeoutError) do
        @handle.wait(max_wait_timeout_ms: 100)
      end

      assert_match(/describe acl/, error.message)
    end

    context "when not pending anymore and no error" do
      before { @handle = build_handle(false) }

      it "returns a describe acl report" do
        report = @handle.wait

        assert_equal(1, report.acls.length)
      end

      it "waits without a timeout" do
        report = @handle.wait(max_wait_timeout_ms: nil)

        assert_equal(@resource_name, report.acls[0].matching_acl_resource_name)
      end
    end
  end

  describe "#raise_error" do
    before { @handle = build_handle(false) }

    it "raises the appropriate error" do
      error = assert_raises(Rdkafka::RdkafkaError) { @handle.raise_error }

      assert_match(/Success \(no_error\)/, error.message)
    end
  end
end
