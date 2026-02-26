# frozen_string_literal: true

RSpec.describe Rdkafka::Admin::ListOffsetsReport do
  describe "#initialize" do
    context "when result_infos is NULL" do
      subject { described_class.new(result_infos: FFI::Pointer::NULL, result_count: 0) }

      it "returns empty offsets" do
        expect(subject.offsets).to eq([])
      end
    end
  end
end
