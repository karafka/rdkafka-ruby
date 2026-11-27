# frozen_string_literal: true

describe Rdkafka::Callbacks do
  describe Rdkafka::Callbacks::BackgroundEventCallback do
    before do
      @event_ptr = FFI::MemoryPointer.new(:int)
      Rdkafka::Bindings.stubs(:rd_kafka_event_destroy)
    end

    # Stubs a librdkafka result accessor taking a `size_t *cntp` out-parameter. The block
    # receives the count pointer so it can write the count, like the native call does.
    def stub_count_out_param(method_name, returns)
      Rdkafka::Bindings.stubs(method_name).returns(returns).with do |_result, count_ptr|
        yield(count_ptr)
        true
      end
    end

    def stub_event(event_type, handle_ptr)
      Rdkafka::Bindings.stubs(:rd_kafka_event_type).with(@event_ptr).returns(event_type)
      Rdkafka::Bindings.stubs(:rd_kafka_event_opaque).with(@event_ptr).returns(handle_ptr)
    end

    def expect_event_destroyed
      Rdkafka::Bindings.expects(:rd_kafka_event_destroy).with(@event_ptr)
    end

    context "when the event type is not handled" do
      before do
        Rdkafka::Bindings.stubs(:rd_kafka_event_type).with(@event_ptr).returns(-1)
      end

      it "destroys the event" do
        expect_event_destroyed

        described_class.call(nil, @event_ptr, nil)
      end
    end

    context "when processing the event raises" do
      before do
        Rdkafka::Bindings.stubs(:rd_kafka_event_type).raises(RuntimeError, "processing failed")
      end

      it "still destroys the event" do
        expect_event_destroyed

        error = assert_raises(RuntimeError) do
          described_class.call(nil, @event_ptr, nil)
        end

        assert_equal("processing failed", error.message)
      end
    end

    context "when handling a create topic result event" do
      before do
        @handle = Rdkafka::Admin::CreateTopicHandle.new
        topic_result_ptr = FFI::MemoryPointer.new(:int)
        topic_result_array_ptr = FFI::MemoryPointer.new(:pointer)
        topic_result_array_ptr.write_array_of_pointer([topic_result_ptr])

        @handle[:pending] = true
        Rdkafka::Admin::CreateTopicHandle.register(@handle)

        stub_event(Rdkafka::Bindings::RD_KAFKA_EVENT_CREATETOPICS_RESULT, @handle.to_ptr)
        Rdkafka::Bindings.stubs(:rd_kafka_event_CreateTopics_result).with(@event_ptr)
          .returns(FFI::MemoryPointer.new(:int))
        stub_count_out_param(:rd_kafka_CreateTopics_result_topics, topic_result_array_ptr) do |count_ptr|
          count_ptr.write_int(1)
        end
        Rdkafka::Bindings.stubs(
          rd_kafka_event_error: Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR,
          rd_kafka_topic_result_error: 0,
          rd_kafka_topic_result_error_string: FFI::MemoryPointer.from_string("broker error message"),
          rd_kafka_topic_result_name: FFI::MemoryPointer.from_string("example-topic")
        )
      end

      after { Rdkafka::Admin::CreateTopicHandle.remove(@handle.to_ptr.address) }

      it "copies the result into Ruby objects, resolves the handle and destroys the event" do
        expect_event_destroyed

        described_class.call(nil, @event_ptr, nil)

        refute_predicate(@handle, :pending?)
        assert_kind_of(Rdkafka::Admin::CreateTopicReport, @handle.result)
        assert_equal("example-topic", @handle.result.result_name)
        assert_equal("broker error message", @handle.result.error_string)
        assert_equal("broker error message", @handle.broker_message)
        assert_nil(Rdkafka::Admin::CreateTopicHandle.remove(@handle.to_ptr.address))
      end

      it "returns the prepared report from wait" do
        described_class.call(nil, @event_ptr, nil)

        assert_equal("example-topic", @handle.wait.result_name)
      end
    end

    context "count pointer sizing for size_t out-parameters" do
      # librdkafka's result accessors take a `size_t *cntp` and write a full native
      # size_t (8 bytes on 64-bit). The count pointer must therefore be allocated as
      # :size_t, not :int32, otherwise librdkafka overflows the 4-byte buffer.
      before do
        @captured_size = []

        # Unregistered opaque address: the handler parses the (empty) results then no-ops
        stub_event(Rdkafka::Bindings::RD_KAFKA_EVENT_CREATETOPICS_RESULT, FFI::Pointer.new(1))
        Rdkafka::Bindings.stubs(:rd_kafka_event_CreateTopics_result)
          .with(@event_ptr).returns(FFI::MemoryPointer.new(:int))
        stub_count_out_param(:rd_kafka_CreateTopics_result_topics, FFI::MemoryPointer.new(:pointer)) do |count_ptr|
          @captured_size << count_ptr.size
          count_ptr.write(:size_t, 0)
        end
      end

      it "allocates a full native size_t for the result count, not a 4-byte int" do
        described_class.call(nil, @event_ptr, nil)

        assert_equal(FFI.type_size(:size_t), @captured_size.first)
      end
    end

    context "when an operation fails at the operation level (empty results, event error set)" do
      # librdkafka signals whole-operation failures (brokers unreachable, client destroyed mid
      # request) by setting the event error and delivering an empty results array. The handler
      # must resolve the handle from the event error and never index into the empty array.
      before do
        @handle = Rdkafka::Admin::CreateTopicHandle.new
        # RD_KAFKA_RESP_ERR__TIMED_OUT
        @timed_out_code = -185

        @handle[:pending] = true
        Rdkafka::Admin::CreateTopicHandle.register(@handle)

        stub_event(Rdkafka::Bindings::RD_KAFKA_EVENT_CREATETOPICS_RESULT, @handle.to_ptr)
        Rdkafka::Bindings.stubs(:rd_kafka_event_CreateTopics_result).with(@event_ptr)
          .returns(FFI::MemoryPointer.new(:int))
        # Empty results array - this is what makes results[0] nil
        stub_count_out_param(:rd_kafka_CreateTopics_result_topics, FFI::MemoryPointer.new(:pointer)) do |count_ptr|
          count_ptr.write_int(0)
        end
        Rdkafka::Bindings.stubs(
          rd_kafka_event_error: @timed_out_code,
          rd_kafka_event_error_string: FFI::MemoryPointer.from_string("Local: Timed out"),
          # Spied so we can assert the empty results array is never indexed
          rd_kafka_topic_result_error: 0
        )
      end

      after { Rdkafka::Admin::CreateTopicHandle.remove(@handle.to_ptr.address) }

      it "resolves the handle from the event error without raising or touching the empty results" do
        # Must not index into the empty results array
        Rdkafka::Bindings.expects(:rd_kafka_topic_result_error).never
        expect_event_destroyed

        described_class.call(nil, @event_ptr, nil)

        refute_predicate(@handle, :pending?)
        assert_equal(@timed_out_code, @handle[:response])
        assert_equal("Local: Timed out", @handle.broker_message)
      end

      it "makes wait raise the real error instead of WaitTimeoutError" do
        described_class.call(nil, @event_ptr, nil)

        error = assert_raises(Rdkafka::RdkafkaError) { @handle.wait }

        refute_kind_of(Rdkafka::AbstractHandle::WaitTimeoutError, error)
        assert_equal(:timed_out, error.code)
      end
    end

    context "when handling a delete topic result event" do
      before do
        @handle = Rdkafka::Admin::DeleteTopicHandle.new
        topic_result_array_ptr = FFI::MemoryPointer.new(:pointer)
        topic_result_array_ptr.write_array_of_pointer([FFI::MemoryPointer.new(:int)])

        @handle[:pending] = true
        Rdkafka::Admin::DeleteTopicHandle.register(@handle)

        stub_event(Rdkafka::Bindings::RD_KAFKA_EVENT_DELETETOPICS_RESULT, @handle.to_ptr)
        Rdkafka::Bindings.stubs(:rd_kafka_event_DeleteTopics_result).with(@event_ptr)
          .returns(FFI::MemoryPointer.new(:int))
        stub_count_out_param(:rd_kafka_DeleteTopics_result_topics, topic_result_array_ptr) do |count_ptr|
          count_ptr.write_int(1)
        end
        Rdkafka::Bindings.stubs(
          rd_kafka_event_error: Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR,
          rd_kafka_topic_result_error: 0,
          rd_kafka_topic_result_error_string: FFI::Pointer::NULL,
          rd_kafka_topic_result_name: FFI::MemoryPointer.from_string("deleted-topic")
        )
      end

      after { Rdkafka::Admin::DeleteTopicHandle.remove(@handle.to_ptr.address) }

      it "resolves the handle with a DeleteTopicReport and destroys the event" do
        expect_event_destroyed

        described_class.call(nil, @event_ptr, nil)

        refute_predicate(@handle, :pending?)
        assert_kind_of(Rdkafka::Admin::DeleteTopicReport, @handle.result)
        assert_equal("deleted-topic", @handle.result.result_name)
      end
    end

    context "when handling a create partitions result event" do
      before do
        @handle = Rdkafka::Admin::CreatePartitionsHandle.new
        topic_result_array_ptr = FFI::MemoryPointer.new(:pointer)
        topic_result_array_ptr.write_array_of_pointer([FFI::MemoryPointer.new(:int)])

        @handle[:pending] = true
        Rdkafka::Admin::CreatePartitionsHandle.register(@handle)

        stub_event(Rdkafka::Bindings::RD_KAFKA_ADMIN_OP_CREATEPARTITIONS_RESULT, @handle.to_ptr)
        Rdkafka::Bindings.stubs(:rd_kafka_event_CreatePartitions_result).with(@event_ptr)
          .returns(FFI::MemoryPointer.new(:int))
        stub_count_out_param(:rd_kafka_CreatePartitions_result_topics, topic_result_array_ptr) do |count_ptr|
          count_ptr.write_int(1)
        end
        Rdkafka::Bindings.stubs(
          rd_kafka_event_error: Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR,
          rd_kafka_topic_result_error: 0,
          rd_kafka_topic_result_error_string: FFI::Pointer::NULL,
          rd_kafka_topic_result_name: FFI::MemoryPointer.from_string("partitioned-topic")
        )
      end

      after { Rdkafka::Admin::CreatePartitionsHandle.remove(@handle.to_ptr.address) }

      it "resolves the handle with a CreatePartitionsReport and destroys the event" do
        expect_event_destroyed

        described_class.call(nil, @event_ptr, nil)

        refute_predicate(@handle, :pending?)
        assert_kind_of(Rdkafka::Admin::CreatePartitionsReport, @handle.result)
        assert_equal("partitioned-topic", @handle.result.result_name)
      end
    end

    context "when handling a delete groups result event" do
      before do
        @handle = Rdkafka::Admin::DeleteGroupsHandle.new
        group_result_array_ptr = FFI::MemoryPointer.new(:pointer)
        group_result_array_ptr.write_array_of_pointer([FFI::MemoryPointer.new(:int)])

        @handle[:pending] = true
        Rdkafka::Admin::DeleteGroupsHandle.register(@handle)

        stub_event(Rdkafka::Bindings::RD_KAFKA_EVENT_DELETEGROUPS_RESULT, @handle.to_ptr)
        Rdkafka::Bindings.stubs(:rd_kafka_event_DeleteGroups_result).with(@event_ptr)
          .returns(FFI::MemoryPointer.new(:int))
        stub_count_out_param(:rd_kafka_DeleteGroups_result_groups, group_result_array_ptr) do |count_ptr|
          count_ptr.write_int(1)
        end
        Rdkafka::Bindings.stubs(
          rd_kafka_event_error: Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR,
          # A null native error means "no error" in GroupResult
          rd_kafka_group_result_error: Rdkafka::Bindings::NativeError.new(FFI::Pointer::NULL),
          rd_kafka_group_result_name: FFI::MemoryPointer.from_string("deleted-group")
        )
      end

      after { Rdkafka::Admin::DeleteGroupsHandle.remove(@handle.to_ptr.address) }

      it "resolves the handle with a DeleteGroupsReport and destroys the event" do
        expect_event_destroyed

        described_class.call(nil, @event_ptr, nil)

        refute_predicate(@handle, :pending?)
        assert_kind_of(Rdkafka::Admin::DeleteGroupsReport, @handle.result)
        assert_equal("deleted-group", @handle.result.result_name)
      end
    end

    context "when handling a create ACL result event" do
      before do
        @handle = Rdkafka::Admin::CreateAclHandle.new
        acl_result_array_ptr = FFI::MemoryPointer.new(:pointer)
        acl_result_array_ptr.write_array_of_pointer([FFI::MemoryPointer.new(:int)])

        @handle[:pending] = true
        Rdkafka::Admin::CreateAclHandle.register(@handle)

        stub_event(Rdkafka::Bindings::RD_KAFKA_EVENT_CREATEACLS_RESULT, @handle.to_ptr)
        Rdkafka::Bindings.stubs(:rd_kafka_event_CreateAcls_result).with(@event_ptr)
          .returns(FFI::MemoryPointer.new(:int))
        stub_count_out_param(:rd_kafka_CreateAcls_result_acls, acl_result_array_ptr) do |count_ptr|
          count_ptr.write_int(1)
        end
        Rdkafka::Bindings.stubs(
          rd_kafka_event_error: Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR,
          rd_kafka_acl_result_error: FFI::MemoryPointer.new(:int),
          rd_kafka_error_code: Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR,
          rd_kafka_error_string: FFI::MemoryPointer.from_string("")
        )
      end

      after { Rdkafka::Admin::CreateAclHandle.remove(@handle.to_ptr.address) }

      it "resolves the handle with a CreateAclReport and destroys the event" do
        expect_event_destroyed

        described_class.call(nil, @event_ptr, nil)

        refute_predicate(@handle, :pending?)
        assert_kind_of(Rdkafka::Admin::CreateAclReport, @handle.result)
        assert_equal(Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR, @handle.result.rdkafka_response)
      end
    end

    context "when handling a delete ACL result event" do
      def result_error
        Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR
      end

      before do
        @handle = Rdkafka::Admin::DeleteAclHandle.new
        response_array_ptr = FFI::MemoryPointer.new(:pointer)
        response_array_ptr.write_array_of_pointer([FFI::MemoryPointer.new(:int)])

        @handle[:pending] = true
        Rdkafka::Admin::DeleteAclHandle.register(@handle)

        stub_event(Rdkafka::Bindings::RD_KAFKA_EVENT_DELETEACLS_RESULT, @handle.to_ptr)
        Rdkafka::Bindings.stubs(:rd_kafka_event_DeleteAcls_result).with(@event_ptr)
          .returns(FFI::MemoryPointer.new(:int))
        stub_count_out_param(:rd_kafka_DeleteAcls_result_responses, response_array_ptr) do |count_ptr|
          count_ptr.write_int(1)
        end
        Rdkafka::Bindings.stubs(
          rd_kafka_event_error: Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR,
          rd_kafka_DeleteAcls_result_response_error: FFI::MemoryPointer.new(:int),
          rd_kafka_error_code: result_error,
          rd_kafka_error_string: FFI::MemoryPointer.from_string("")
        )
        stub_count_out_param(:rd_kafka_DeleteAcls_result_response_matching_acls, FFI::MemoryPointer.new(:pointer)) do |count_ptr|
          count_ptr.write_int(0)
        end
      end

      after { Rdkafka::Admin::DeleteAclHandle.remove(@handle.to_ptr.address) }

      it "resolves the handle with a DeleteAclReport and destroys the event" do
        expect_event_destroyed

        described_class.call(nil, @event_ptr, nil)

        refute_predicate(@handle, :pending?)
        assert_kind_of(Rdkafka::Admin::DeleteAclReport, @handle.result)
        assert_empty(@handle.result.deleted_acls)
      end

      context "and a single ACL deletion reports an error" do
        def result_error
          -185
        end

        it "resolves the handle with the error and an empty DeleteAclReport" do
          expect_event_destroyed

          described_class.call(nil, @event_ptr, nil)

          refute_predicate(@handle, :pending?)
          assert_equal(-185, @handle[:response])
          assert_kind_of(Rdkafka::Admin::DeleteAclReport, @handle.result)
          assert_empty(@handle.result.deleted_acls)
        end
      end
    end

    context "when handling a describe ACL result event with an error" do
      before do
        @handle = Rdkafka::Admin::DescribeAclHandle.new

        @handle[:pending] = true
        Rdkafka::Admin::DescribeAclHandle.register(@handle)

        stub_event(Rdkafka::Bindings::RD_KAFKA_EVENT_DESCRIBEACLS_RESULT, @handle.to_ptr)
        Rdkafka::Bindings.stubs(
          rd_kafka_event_error: -195,
          rd_kafka_event_error_string: FFI::MemoryPointer.from_string("Local: Broker transport failure")
        )
      end

      after { Rdkafka::Admin::DescribeAclHandle.remove(@handle.to_ptr.address) }

      it "resolves the handle with the error and an empty DescribeAclReport" do
        expect_event_destroyed

        described_class.call(nil, @event_ptr, nil)

        refute_predicate(@handle, :pending?)
        assert_equal(-195, @handle[:response])
        assert_equal("Local: Broker transport failure", @handle.broker_message)
        assert_kind_of(Rdkafka::Admin::DescribeAclReport, @handle.result)
        assert_empty(@handle.result.acls)
      end
    end

    context "when handling a describe configs result event whose parsing raises" do
      before do
        @handle = Rdkafka::Admin::DescribeConfigsHandle.new
        @parse_error = Rdkafka::RdkafkaError.new(1)

        @handle[:pending] = true
        Rdkafka::Admin::DescribeConfigsHandle.register(@handle)

        stub_event(Rdkafka::Bindings::RD_KAFKA_EVENT_DESCRIBECONFIGS_RESULT, @handle.to_ptr)
        Rdkafka::Bindings.stubs(:rd_kafka_event_error).with(@event_ptr).returns(0)
        Rdkafka::Bindings.stubs(:rd_kafka_event_error_string).with(@event_ptr)
          .returns(FFI::MemoryPointer.from_string(""))
        Rdkafka::Bindings.stubs(:rd_kafka_event_DescribeConfigs_result).with(@event_ptr)
          .returns(FFI::MemoryPointer.new(:int))
        stub_count_out_param(:rd_kafka_DescribeConfigs_result_resources, FFI::MemoryPointer.new(:pointer)) do |count_ptr|
          count_ptr.write_int(1)
        end
        Rdkafka::Admin::DescribeConfigsReport.stubs(:new).raises(@parse_error)
      end

      after { Rdkafka::Admin::DescribeConfigsHandle.remove(@handle.to_ptr.address) }

      it "captures the exception, resolves the handle and re-raises on wait" do
        expect_event_destroyed

        described_class.call(nil, @event_ptr, nil)

        refute_predicate(@handle, :pending?)
        assert_equal(@parse_error, @handle.result)
        assert_raises(Rdkafka::RdkafkaError) { @handle.wait }
      end
    end

    context "when handling an incremental alter configs result event with an error" do
      before do
        @handle = Rdkafka::Admin::IncrementalAlterConfigsHandle.new

        @handle[:pending] = true
        Rdkafka::Admin::IncrementalAlterConfigsHandle.register(@handle)

        stub_event(Rdkafka::Bindings::RD_KAFKA_EVENT_INCREMENTALALTERCONFIGS_RESULT, @handle.to_ptr)
        Rdkafka::Bindings.stubs(
          rd_kafka_event_error: -185,
          rd_kafka_event_error_string: FFI::MemoryPointer.from_string("Local: Timed out")
        )
      end

      after { Rdkafka::Admin::IncrementalAlterConfigsHandle.remove(@handle.to_ptr.address) }

      it "resolves the handle with the error and an empty report" do
        expect_event_destroyed

        described_class.call(nil, @event_ptr, nil)

        refute_predicate(@handle, :pending?)
        assert_equal(-185, @handle[:response])
        assert_kind_of(Rdkafka::Admin::IncrementalAlterConfigsReport, @handle.result)
        assert_empty(@handle.result.resources)
      end
    end

    context "when handling a list offsets result event with an error" do
      before do
        @handle = Rdkafka::Admin::ListOffsetsHandle.new

        @handle[:pending] = true
        Rdkafka::Admin::ListOffsetsHandle.register(@handle)

        stub_event(Rdkafka::Bindings::RD_KAFKA_EVENT_LISTOFFSETS_RESULT, @handle.to_ptr)
        Rdkafka::Bindings.stubs(
          rd_kafka_event_error: -185,
          rd_kafka_event_error_string: FFI::MemoryPointer.from_string("Local: Timed out")
        )
      end

      after { Rdkafka::Admin::ListOffsetsHandle.remove(@handle.to_ptr.address) }

      it "resolves the handle with the error and an empty ListOffsetsReport" do
        expect_event_destroyed

        described_class.call(nil, @event_ptr, nil)

        refute_predicate(@handle, :pending?)
        assert_equal(-185, @handle[:response])
        assert_kind_of(Rdkafka::Admin::ListOffsetsReport, @handle.result)
        assert_empty(@handle.result.offsets)
      end
    end

    context "when handling a list offsets result event whose parsing raises" do
      before do
        @handle = Rdkafka::Admin::ListOffsetsHandle.new
        @parse_error = Rdkafka::RdkafkaError.new(1)

        @handle[:pending] = true
        Rdkafka::Admin::ListOffsetsHandle.register(@handle)

        stub_event(Rdkafka::Bindings::RD_KAFKA_EVENT_LISTOFFSETS_RESULT, @handle.to_ptr)
        Rdkafka::Bindings.stubs(:rd_kafka_event_error).with(@event_ptr).returns(0)
        Rdkafka::Bindings.stubs(:rd_kafka_event_error_string).with(@event_ptr)
          .returns(FFI::MemoryPointer.from_string(""))
        Rdkafka::Bindings.stubs(:rd_kafka_event_ListOffsets_result).with(@event_ptr)
          .returns(FFI::MemoryPointer.new(:int))
        stub_count_out_param(:rd_kafka_ListOffsets_result_infos, FFI::MemoryPointer.new(:pointer)) do |count_ptr|
          count_ptr.write(:size_t, 1)
        end
        Rdkafka::Admin::ListOffsetsReport.stubs(:new).raises(@parse_error)
      end

      after { Rdkafka::Admin::ListOffsetsHandle.remove(@handle.to_ptr.address) }

      it "captures the exception, resolves the handle and re-raises on wait" do
        expect_event_destroyed

        described_class.call(nil, @event_ptr, nil)

        refute_predicate(@handle, :pending?)
        assert_equal(@parse_error, @handle.result)
        assert_raises(Rdkafka::RdkafkaError) { @handle.wait }
      end
    end
  end

  describe Rdkafka::Callbacks::BaseHandler do
    it "raises NotImplementedError when .call is invoked on the abstract base" do
      error = assert_raises(NotImplementedError) do
        described_class.call(FFI::MemoryPointer.new(:int))
      end

      assert_match(/must implement \.call/, error.message)
    end
  end

  {
    Rdkafka::Callbacks::TopicResult => :create_topic_results_from_array,
    Rdkafka::Callbacks::GroupResult => :create_group_results_from_array,
    Rdkafka::Callbacks::CreateAclResult => :create_acl_results_from_array,
    Rdkafka::Callbacks::DeleteAclResult => :delete_acl_results_from_array
  }.each do |result_class, builder|
    describe "#{result_class}.#{builder}" do
      before do
        @addresses = [0x1000, 0x2000, 0x3000]
        @array_pointer = FFI::MemoryPointer.new(:pointer, @addresses.size)
        @array_pointer.write_array_of_pointer(@addresses.map { |address| FFI::Pointer.new(address) })

        result_class.stubs(:new)
      end

      it "reads every element of the pointer array, not just the first" do
        @addresses.each do |address|
          result_class.expects(:new).with { |result_pointer| result_pointer.address == address }.returns(address)
        end

        results = result_class.public_send(builder, @addresses.size, @array_pointer)

        assert_equal(@addresses, results)
      end

      it "returns no results for an empty array" do
        assert_empty(result_class.public_send(builder, 0, FFI::Pointer::NULL))
      end
    end
  end
end
