# frozen_string_literal: true

describe Rdkafka::Admin::ListOffsetsReport do
  it "returns empty offsets when null" do
    subject = described_class.new(result_infos: FFI::Pointer::NULL, result_count: 0)

    assert_equal [], subject.offsets
  end
end
