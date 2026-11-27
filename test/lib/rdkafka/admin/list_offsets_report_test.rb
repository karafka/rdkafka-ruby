# frozen_string_literal: true

describe Rdkafka::Admin::ListOffsetsReport do
  describe "#initialize" do
    context "when result_infos is NULL" do
      before { @report = described_class.new(result_infos: FFI::Pointer::NULL, result_count: 0) }

      it "returns empty offsets" do
        assert_equal([], @report.offsets)
      end
    end

    context "when a partition carries an error" do
      it "raises an RdkafkaError naming the failing partition" do
        # 3 == UNKNOWN_TOPIC_OR_PART
        failing_partition = { err: 3, partition: 99, topic: "example" }

        Rdkafka::Bindings
          .stubs(:rd_kafka_ListOffsetsResultInfo_topic_partition)
          .returns(FFI::Pointer::NULL)
        Rdkafka::Bindings::TopicPartition
          .stubs(:new)
          .returns(failing_partition)

        result_infos = FFI::MemoryPointer.new(:pointer, 1)
        result_infos.write_array_of_pointer([FFI::Pointer::NULL])

        error = assert_raises(Rdkafka::RdkafkaError) do
          described_class.new(result_infos: result_infos, result_count: 1)
        end

        assert_match(/partition 99 of 'example'/, error.message)
      end
    end
  end
end
