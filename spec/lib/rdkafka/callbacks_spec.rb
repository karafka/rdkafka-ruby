# frozen_string_literal: true

RSpec.describe Rdkafka::Callbacks do
  describe Rdkafka::Callbacks::BackgroundEventCallback do
    let(:event_ptr) { FFI::MemoryPointer.new(:int) }

    before { allow(Rdkafka::Bindings).to receive(:rd_kafka_event_destroy) }

    context "when the event type is not handled" do
      before do
        allow(Rdkafka::Bindings).to receive(:rd_kafka_event_type).with(event_ptr).and_return(-1)
      end

      it "destroys the event" do
        described_class.call(nil, event_ptr, nil)

        expect(Rdkafka::Bindings).to have_received(:rd_kafka_event_destroy).with(event_ptr)
      end
    end

    context "when processing the event raises" do
      before do
        allow(Rdkafka::Bindings).to receive(:rd_kafka_event_type).and_raise(RuntimeError, "processing failed")
      end

      it "still destroys the event" do
        expect {
          described_class.call(nil, event_ptr, nil)
        }.to raise_error(RuntimeError, "processing failed")

        expect(Rdkafka::Bindings).to have_received(:rd_kafka_event_destroy).with(event_ptr)
      end
    end

    context "when handling a create topic result event" do
      let(:handle) { Rdkafka::Admin::CreateTopicHandle.new }
      let(:result_ptr) { FFI::MemoryPointer.new(:int) }
      let(:topic_result_ptr) { FFI::MemoryPointer.new(:int) }
      let(:topic_result_array_ptr) do
        FFI::MemoryPointer.new(:pointer).tap { |ptr| ptr.write_array_of_pointer([topic_result_ptr]) }
      end
      let(:error_string_ptr) { FFI::MemoryPointer.from_string("broker error message") }
      let(:result_name_ptr) { FFI::MemoryPointer.from_string("example-topic") }

      before do
        handle[:pending] = true
        Rdkafka::Admin::CreateTopicHandle.register(handle)

        allow(Rdkafka::Bindings).to receive(:rd_kafka_event_type).with(event_ptr)
          .and_return(Rdkafka::Bindings::RD_KAFKA_EVENT_CREATETOPICS_RESULT)
        allow(Rdkafka::Bindings).to receive(:rd_kafka_event_CreateTopics_result).with(event_ptr).and_return(result_ptr)
        allow(Rdkafka::Bindings).to receive(:rd_kafka_CreateTopics_result_topics) do |_result, count_ptr|
          count_ptr.write_int(1)
          topic_result_array_ptr
        end
        allow(Rdkafka::Bindings).to receive_messages(
          rd_kafka_topic_result_error: 0,
          rd_kafka_topic_result_error_string: error_string_ptr,
          rd_kafka_topic_result_name: result_name_ptr
        )
        allow(Rdkafka::Bindings).to receive(:rd_kafka_event_opaque).with(event_ptr).and_return(handle.to_ptr)
      end

      after { Rdkafka::Admin::CreateTopicHandle.remove(handle.to_ptr.address) }

      it "copies the result into Ruby objects, resolves the handle and destroys the event" do
        described_class.call(nil, event_ptr, nil)

        expect(handle.pending?).to be false
        expect(handle.result).to be_a(Rdkafka::Admin::CreateTopicReport)
        expect(handle.result.result_name).to eq("example-topic")
        expect(handle.result.error_string).to eq("broker error message")
        expect(handle.broker_message).to eq("broker error message")
        expect(Rdkafka::Admin::CreateTopicHandle.remove(handle.to_ptr.address)).to be_nil
        expect(Rdkafka::Bindings).to have_received(:rd_kafka_event_destroy).with(event_ptr)
      end

      it "returns the prepared report from wait" do
        described_class.call(nil, event_ptr, nil)

        expect(handle.wait.result_name).to eq("example-topic")
      end
    end

    context "when handling a delete topic result event" do
      let(:handle) { Rdkafka::Admin::DeleteTopicHandle.new }
      let(:result_ptr) { FFI::MemoryPointer.new(:int) }
      let(:topic_result_ptr) { FFI::MemoryPointer.new(:int) }
      let(:topic_result_array_ptr) do
        FFI::MemoryPointer.new(:pointer).tap { |ptr| ptr.write_array_of_pointer([topic_result_ptr]) }
      end
      let(:result_name_ptr) { FFI::MemoryPointer.from_string("deleted-topic") }

      before do
        handle[:pending] = true
        Rdkafka::Admin::DeleteTopicHandle.register(handle)

        allow(Rdkafka::Bindings).to receive(:rd_kafka_event_type).with(event_ptr)
          .and_return(Rdkafka::Bindings::RD_KAFKA_EVENT_DELETETOPICS_RESULT)
        allow(Rdkafka::Bindings).to receive(:rd_kafka_event_DeleteTopics_result).with(event_ptr).and_return(result_ptr)
        allow(Rdkafka::Bindings).to receive(:rd_kafka_DeleteTopics_result_topics) do |_result, count_ptr|
          count_ptr.write_int(1)
          topic_result_array_ptr
        end
        allow(Rdkafka::Bindings).to receive_messages(
          rd_kafka_topic_result_error: 0,
          rd_kafka_topic_result_error_string: FFI::Pointer::NULL,
          rd_kafka_topic_result_name: result_name_ptr
        )
        allow(Rdkafka::Bindings).to receive(:rd_kafka_event_opaque).with(event_ptr).and_return(handle.to_ptr)
      end

      after { Rdkafka::Admin::DeleteTopicHandle.remove(handle.to_ptr.address) }

      it "resolves the handle with a DeleteTopicReport and destroys the event" do
        described_class.call(nil, event_ptr, nil)

        expect(handle.pending?).to be false
        expect(handle.result).to be_a(Rdkafka::Admin::DeleteTopicReport)
        expect(handle.result.result_name).to eq("deleted-topic")
        expect(Rdkafka::Bindings).to have_received(:rd_kafka_event_destroy).with(event_ptr)
      end
    end

    context "when handling a create partitions result event" do
      let(:handle) { Rdkafka::Admin::CreatePartitionsHandle.new }
      let(:result_ptr) { FFI::MemoryPointer.new(:int) }
      let(:topic_result_ptr) { FFI::MemoryPointer.new(:int) }
      let(:topic_result_array_ptr) do
        FFI::MemoryPointer.new(:pointer).tap { |ptr| ptr.write_array_of_pointer([topic_result_ptr]) }
      end
      let(:result_name_ptr) { FFI::MemoryPointer.from_string("partitioned-topic") }

      before do
        handle[:pending] = true
        Rdkafka::Admin::CreatePartitionsHandle.register(handle)

        allow(Rdkafka::Bindings).to receive(:rd_kafka_event_type).with(event_ptr)
          .and_return(Rdkafka::Bindings::RD_KAFKA_ADMIN_OP_CREATEPARTITIONS_RESULT)
        allow(Rdkafka::Bindings).to receive(:rd_kafka_event_CreatePartitions_result).with(event_ptr).and_return(result_ptr)
        allow(Rdkafka::Bindings).to receive(:rd_kafka_CreatePartitions_result_topics) do |_result, count_ptr|
          count_ptr.write_int(1)
          topic_result_array_ptr
        end
        allow(Rdkafka::Bindings).to receive_messages(
          rd_kafka_topic_result_error: 0,
          rd_kafka_topic_result_error_string: FFI::Pointer::NULL,
          rd_kafka_topic_result_name: result_name_ptr
        )
        allow(Rdkafka::Bindings).to receive(:rd_kafka_event_opaque).with(event_ptr).and_return(handle.to_ptr)
      end

      after { Rdkafka::Admin::CreatePartitionsHandle.remove(handle.to_ptr.address) }

      it "resolves the handle with a CreatePartitionsReport and destroys the event" do
        described_class.call(nil, event_ptr, nil)

        expect(handle.pending?).to be false
        expect(handle.result).to be_a(Rdkafka::Admin::CreatePartitionsReport)
        expect(handle.result.result_name).to eq("partitioned-topic")
        expect(Rdkafka::Bindings).to have_received(:rd_kafka_event_destroy).with(event_ptr)
      end
    end

    context "when handling a delete groups result event" do
      let(:handle) { Rdkafka::Admin::DeleteGroupsHandle.new }
      let(:result_ptr) { FFI::MemoryPointer.new(:int) }
      let(:group_result_ptr) { FFI::MemoryPointer.new(:int) }
      let(:group_result_array_ptr) do
        FFI::MemoryPointer.new(:pointer).tap { |ptr| ptr.write_array_of_pointer([group_result_ptr]) }
      end
      let(:result_name_ptr) { FFI::MemoryPointer.from_string("deleted-group") }
      # A null native error means "no error" in GroupResult
      let(:null_native_error) { Rdkafka::Bindings::NativeError.new(FFI::Pointer::NULL) }

      before do
        handle[:pending] = true
        Rdkafka::Admin::DeleteGroupsHandle.register(handle)

        allow(Rdkafka::Bindings).to receive(:rd_kafka_event_type).with(event_ptr)
          .and_return(Rdkafka::Bindings::RD_KAFKA_EVENT_DELETEGROUPS_RESULT)
        allow(Rdkafka::Bindings).to receive(:rd_kafka_event_DeleteGroups_result).with(event_ptr).and_return(result_ptr)
        allow(Rdkafka::Bindings).to receive(:rd_kafka_DeleteGroups_result_groups) do |_result, count_ptr|
          count_ptr.write_int(1)
          group_result_array_ptr
        end
        allow(Rdkafka::Bindings).to receive_messages(
          rd_kafka_group_result_error: null_native_error,
          rd_kafka_group_result_name: result_name_ptr
        )
        allow(Rdkafka::Bindings).to receive(:rd_kafka_event_opaque).with(event_ptr).and_return(handle.to_ptr)
      end

      after { Rdkafka::Admin::DeleteGroupsHandle.remove(handle.to_ptr.address) }

      it "resolves the handle with a DeleteGroupsReport and destroys the event" do
        described_class.call(nil, event_ptr, nil)

        expect(handle.pending?).to be false
        expect(handle.result).to be_a(Rdkafka::Admin::DeleteGroupsReport)
        expect(handle.result.result_name).to eq("deleted-group")
        expect(Rdkafka::Bindings).to have_received(:rd_kafka_event_destroy).with(event_ptr)
      end
    end

    context "when handling a create ACL result event" do
      let(:handle) { Rdkafka::Admin::CreateAclHandle.new }
      let(:result_ptr) { FFI::MemoryPointer.new(:int) }
      let(:acl_result_ptr) { FFI::MemoryPointer.new(:int) }
      let(:acl_result_array_ptr) do
        FFI::MemoryPointer.new(:pointer).tap { |ptr| ptr.write_array_of_pointer([acl_result_ptr]) }
      end
      let(:error_ptr) { FFI::MemoryPointer.new(:int) }
      let(:error_string_ptr) { FFI::MemoryPointer.from_string("") }

      before do
        handle[:pending] = true
        Rdkafka::Admin::CreateAclHandle.register(handle)

        allow(Rdkafka::Bindings).to receive(:rd_kafka_event_type).with(event_ptr)
          .and_return(Rdkafka::Bindings::RD_KAFKA_EVENT_CREATEACLS_RESULT)
        allow(Rdkafka::Bindings).to receive(:rd_kafka_event_CreateAcls_result).with(event_ptr).and_return(result_ptr)
        allow(Rdkafka::Bindings).to receive(:rd_kafka_CreateAcls_result_acls) do |_result, count_ptr|
          count_ptr.write_int(1)
          acl_result_array_ptr
        end
        allow(Rdkafka::Bindings).to receive_messages(
          rd_kafka_acl_result_error: error_ptr,
          rd_kafka_error_code: Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR,
          rd_kafka_error_string: error_string_ptr
        )
        allow(Rdkafka::Bindings).to receive(:rd_kafka_event_opaque).with(event_ptr).and_return(handle.to_ptr)
      end

      after { Rdkafka::Admin::CreateAclHandle.remove(handle.to_ptr.address) }

      it "resolves the handle with a CreateAclReport and destroys the event" do
        described_class.call(nil, event_ptr, nil)

        expect(handle.pending?).to be false
        expect(handle.result).to be_a(Rdkafka::Admin::CreateAclReport)
        expect(handle.result.rdkafka_response).to eq(Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR)
        expect(Rdkafka::Bindings).to have_received(:rd_kafka_event_destroy).with(event_ptr)
      end
    end

    context "when handling a delete ACL result event" do
      let(:handle) { Rdkafka::Admin::DeleteAclHandle.new }
      let(:result_ptr) { FFI::MemoryPointer.new(:int) }
      let(:response_ptr) { FFI::MemoryPointer.new(:int) }
      let(:response_array_ptr) do
        FFI::MemoryPointer.new(:pointer).tap { |ptr| ptr.write_array_of_pointer([response_ptr]) }
      end
      let(:error_ptr) { FFI::MemoryPointer.new(:int) }
      let(:error_string_ptr) { FFI::MemoryPointer.from_string("") }
      let(:matching_acls_ptr) { FFI::MemoryPointer.new(:pointer) }
      let(:result_error) { Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR }

      before do
        handle[:pending] = true
        Rdkafka::Admin::DeleteAclHandle.register(handle)

        allow(Rdkafka::Bindings).to receive(:rd_kafka_event_type).with(event_ptr)
          .and_return(Rdkafka::Bindings::RD_KAFKA_EVENT_DELETEACLS_RESULT)
        allow(Rdkafka::Bindings).to receive(:rd_kafka_event_DeleteAcls_result).with(event_ptr).and_return(result_ptr)
        allow(Rdkafka::Bindings).to receive(:rd_kafka_DeleteAcls_result_responses) do |_result, count_ptr|
          count_ptr.write_int(1)
          response_array_ptr
        end
        allow(Rdkafka::Bindings).to receive_messages(
          rd_kafka_DeleteAcls_result_response_error: error_ptr,
          rd_kafka_error_code: result_error,
          rd_kafka_error_string: error_string_ptr
        )
        allow(Rdkafka::Bindings).to receive(:rd_kafka_DeleteAcls_result_response_matching_acls) do |_response, count_ptr|
          count_ptr.write_int(0)
          matching_acls_ptr
        end
        allow(Rdkafka::Bindings).to receive(:rd_kafka_event_opaque).with(event_ptr).and_return(handle.to_ptr)
      end

      after { Rdkafka::Admin::DeleteAclHandle.remove(handle.to_ptr.address) }

      it "resolves the handle with a DeleteAclReport and destroys the event" do
        described_class.call(nil, event_ptr, nil)

        expect(handle.pending?).to be false
        expect(handle.result).to be_a(Rdkafka::Admin::DeleteAclReport)
        expect(handle.result.deleted_acls).to eq([])
        expect(Rdkafka::Bindings).to have_received(:rd_kafka_event_destroy).with(event_ptr)
      end

      context "and a single ACL deletion reports an error" do
        let(:result_error) { -185 }

        it "resolves the handle with the error and an empty DeleteAclReport" do
          described_class.call(nil, event_ptr, nil)

          expect(handle.pending?).to be false
          expect(handle[:response]).to eq(-185)
          expect(handle.result).to be_a(Rdkafka::Admin::DeleteAclReport)
          expect(handle.result.deleted_acls).to eq([])
          expect(Rdkafka::Bindings).to have_received(:rd_kafka_event_destroy).with(event_ptr)
        end
      end
    end

    context "when handling a describe ACL result event with an error" do
      let(:handle) { Rdkafka::Admin::DescribeAclHandle.new }
      let(:error_string_ptr) { FFI::MemoryPointer.from_string("Local: Broker transport failure") }

      before do
        handle[:pending] = true
        Rdkafka::Admin::DescribeAclHandle.register(handle)

        allow(Rdkafka::Bindings).to receive(:rd_kafka_event_type).with(event_ptr)
          .and_return(Rdkafka::Bindings::RD_KAFKA_EVENT_DESCRIBEACLS_RESULT)
        allow(Rdkafka::Bindings).to receive_messages(
          rd_kafka_event_error: -195,
          rd_kafka_event_error_string: error_string_ptr
        )
        allow(Rdkafka::Bindings).to receive(:rd_kafka_event_opaque).with(event_ptr).and_return(handle.to_ptr)
      end

      after { Rdkafka::Admin::DescribeAclHandle.remove(handle.to_ptr.address) }

      it "resolves the handle with the error and an empty DescribeAclReport" do
        described_class.call(nil, event_ptr, nil)

        expect(handle.pending?).to be false
        expect(handle[:response]).to eq(-195)
        expect(handle.broker_message).to eq("Local: Broker transport failure")
        expect(handle.result).to be_a(Rdkafka::Admin::DescribeAclReport)
        expect(handle.result.acls).to eq([])
        expect(Rdkafka::Bindings).to have_received(:rd_kafka_event_destroy).with(event_ptr)
      end
    end

    context "when handling a describe configs result event whose parsing raises" do
      let(:handle) { Rdkafka::Admin::DescribeConfigsHandle.new }
      let(:describe_configs_result_ptr) { FFI::MemoryPointer.new(:int) }
      let(:config_entries_ptr) { FFI::MemoryPointer.new(:pointer) }
      let(:error_string_ptr) { FFI::MemoryPointer.from_string("") }
      let(:parse_error) { Rdkafka::RdkafkaError.new(1) }

      before do
        handle[:pending] = true
        Rdkafka::Admin::DescribeConfigsHandle.register(handle)

        allow(Rdkafka::Bindings).to receive(:rd_kafka_event_type).with(event_ptr)
          .and_return(Rdkafka::Bindings::RD_KAFKA_EVENT_DESCRIBECONFIGS_RESULT)
        allow(Rdkafka::Bindings).to receive(:rd_kafka_event_error).with(event_ptr).and_return(0)
        allow(Rdkafka::Bindings).to receive(:rd_kafka_event_error_string).with(event_ptr).and_return(error_string_ptr)
        allow(Rdkafka::Bindings).to receive(:rd_kafka_event_DescribeConfigs_result).with(event_ptr).and_return(describe_configs_result_ptr)
        allow(Rdkafka::Bindings).to receive(:rd_kafka_DescribeConfigs_result_resources) do |_result, count_ptr|
          count_ptr.write_int(1)
          config_entries_ptr
        end
        allow(Rdkafka::Bindings).to receive(:rd_kafka_event_opaque).with(event_ptr).and_return(handle.to_ptr)
        allow(Rdkafka::Admin::DescribeConfigsReport).to receive(:new).and_raise(parse_error)
      end

      after { Rdkafka::Admin::DescribeConfigsHandle.remove(handle.to_ptr.address) }

      it "captures the exception, resolves the handle and re-raises on wait" do
        described_class.call(nil, event_ptr, nil)

        expect(handle.pending?).to be false
        expect(handle.result).to eq(parse_error)
        expect { handle.wait }.to raise_error(Rdkafka::RdkafkaError)
        expect(Rdkafka::Bindings).to have_received(:rd_kafka_event_destroy).with(event_ptr)
      end
    end

    context "when handling an incremental alter configs result event with an error" do
      let(:handle) { Rdkafka::Admin::IncrementalAlterConfigsHandle.new }
      let(:error_string_ptr) { FFI::MemoryPointer.from_string("Local: Timed out") }

      before do
        handle[:pending] = true
        Rdkafka::Admin::IncrementalAlterConfigsHandle.register(handle)

        allow(Rdkafka::Bindings).to receive(:rd_kafka_event_type).with(event_ptr)
          .and_return(Rdkafka::Bindings::RD_KAFKA_EVENT_INCREMENTALALTERCONFIGS_RESULT)
        allow(Rdkafka::Bindings).to receive_messages(
          rd_kafka_event_error: -185,
          rd_kafka_event_error_string: error_string_ptr
        )
        allow(Rdkafka::Bindings).to receive(:rd_kafka_event_opaque).with(event_ptr).and_return(handle.to_ptr)
      end

      after { Rdkafka::Admin::IncrementalAlterConfigsHandle.remove(handle.to_ptr.address) }

      it "resolves the handle with the error and an empty report" do
        described_class.call(nil, event_ptr, nil)

        expect(handle.pending?).to be false
        expect(handle[:response]).to eq(-185)
        expect(handle.result).to be_a(Rdkafka::Admin::IncrementalAlterConfigsReport)
        expect(handle.result.resources).to eq([])
        expect(Rdkafka::Bindings).to have_received(:rd_kafka_event_destroy).with(event_ptr)
      end
    end

    context "when handling a list offsets result event with an error" do
      let(:handle) { Rdkafka::Admin::ListOffsetsHandle.new }
      let(:error_string_ptr) { FFI::MemoryPointer.from_string("Local: Timed out") }

      before do
        handle[:pending] = true
        Rdkafka::Admin::ListOffsetsHandle.register(handle)

        allow(Rdkafka::Bindings).to receive(:rd_kafka_event_type).with(event_ptr)
          .and_return(Rdkafka::Bindings::RD_KAFKA_EVENT_LISTOFFSETS_RESULT)
        allow(Rdkafka::Bindings).to receive_messages(
          rd_kafka_event_error: -185,
          rd_kafka_event_error_string: error_string_ptr
        )
        allow(Rdkafka::Bindings).to receive(:rd_kafka_event_opaque).with(event_ptr).and_return(handle.to_ptr)
      end

      after { Rdkafka::Admin::ListOffsetsHandle.remove(handle.to_ptr.address) }

      it "resolves the handle with the error and an empty ListOffsetsReport" do
        described_class.call(nil, event_ptr, nil)

        expect(handle.pending?).to be false
        expect(handle[:response]).to eq(-185)
        expect(handle.result).to be_a(Rdkafka::Admin::ListOffsetsReport)
        expect(handle.result.offsets).to eq([])
        expect(Rdkafka::Bindings).to have_received(:rd_kafka_event_destroy).with(event_ptr)
      end
    end
  end

  describe Rdkafka::Callbacks::BaseHandler do
    it "raises NotImplementedError when .call is invoked on the abstract base" do
      expect { described_class.call(FFI::MemoryPointer.new(:int)) }
        .to raise_error(NotImplementedError, /must implement \.call/)
    end
  end
end
