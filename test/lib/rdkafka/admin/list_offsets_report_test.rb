# frozen_string_literal: true

require_relative "../../../test_helper"

describe Rdkafka::Admin::ListOffsetsReport do
  describe "#initialize" do
    context "when result_infos is NULL" do
      before do
        @report = described_class.new(result_infos: FFI::Pointer::NULL, result_count: 0)
      end

      it "returns empty offsets" do
        assert_equal [], @report.offsets
      end
    end
  end
end
