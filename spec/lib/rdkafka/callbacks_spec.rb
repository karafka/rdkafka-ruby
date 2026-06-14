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
      let(:create_topics_result_ptr) { FFI::MemoryPointer.new(:int) }
      let(:topic_result_ptr) { FFI::MemoryPointer.new(:int) }
      let(:topic_result_array_ptr) do
        FFI::MemoryPointer.new(:pointer).tap do |ptr|
          ptr.write_array_of_pointer([topic_result_ptr])
        end
      end
      let(:error_string_ptr) { FFI::MemoryPointer.from_string("broker error message") }
      let(:result_name_ptr) { FFI::MemoryPointer.from_string("example-topic") }

      before do
        handle[:pending] = true
        Rdkafka::Admin::CreateTopicHandle.register(handle)

        allow(Rdkafka::Bindings)
          .to receive(:rd_kafka_event_type)
          .with(event_ptr)
          .and_return(Rdkafka::Bindings::RD_KAFKA_EVENT_CREATETOPICS_RESULT)
        allow(Rdkafka::Bindings)
          .to receive(:rd_kafka_event_CreateTopics_result)
          .with(event_ptr)
          .and_return(create_topics_result_ptr)
        allow(Rdkafka::Bindings)
          .to receive(:rd_kafka_CreateTopics_result_topics) do |_result_ptr, count_ptr|
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
        # The handle must be removed from the registry by the callback
        expect(Rdkafka::Admin::CreateTopicHandle.remove(handle.to_ptr.address)).to be_nil
        expect(Rdkafka::Bindings).to have_received(:rd_kafka_event_destroy).with(event_ptr)
      end

      it "returns the prepared report from wait" do
        described_class.call(nil, event_ptr, nil)

        report = handle.wait

        expect(report.result_name).to eq("example-topic")
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

        allow(Rdkafka::Bindings)
          .to receive(:rd_kafka_event_type)
          .with(event_ptr)
          .and_return(Rdkafka::Bindings::RD_KAFKA_EVENT_DESCRIBECONFIGS_RESULT)
        allow(Rdkafka::Bindings).to receive(:rd_kafka_event_error).with(event_ptr).and_return(0)
        allow(Rdkafka::Bindings).to receive(:rd_kafka_event_error_string).with(event_ptr).and_return(error_string_ptr)
        allow(Rdkafka::Bindings)
          .to receive(:rd_kafka_event_DescribeConfigs_result)
          .with(event_ptr)
          .and_return(describe_configs_result_ptr)
        allow(Rdkafka::Bindings)
          .to receive(:rd_kafka_DescribeConfigs_result_resources) do |_result_ptr, count_ptr|
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
  end
end
