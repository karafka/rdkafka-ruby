# frozen_string_literal: true

RSpec.describe Rdkafka::Admin::ListOffsetsReport do
  describe "#initialize" do
    context "when result_infos is NULL" do
      let(:report) { described_class.new(result_infos: FFI::Pointer::NULL, result_count: 0) }

      it "returns empty offsets" do
        expect(report.offsets).to eq([])
      end
    end

    context "when a partition carries an error" do
      it "raises an RdkafkaError naming the failing partition" do
        # 3 == UNKNOWN_TOPIC_OR_PART
        failing_partition = { err: 3, partition: 99, topic: "example" }

        allow(Rdkafka::Bindings)
          .to receive(:rd_kafka_ListOffsetsResultInfo_topic_partition)
          .and_return(FFI::Pointer::NULL)
        allow(Rdkafka::Bindings::TopicPartition)
          .to receive(:new)
          .and_return(failing_partition)

        result_infos = FFI::MemoryPointer.new(:pointer, 1)
        result_infos.write_array_of_pointer([FFI::Pointer::NULL])

        expect do
          described_class.new(result_infos: result_infos, result_count: 1)
        end.to raise_error(Rdkafka::RdkafkaError, /partition 99 of 'example'/)
      end
    end
  end
end
