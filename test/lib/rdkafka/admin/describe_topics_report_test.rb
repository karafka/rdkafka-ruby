# frozen_string_literal: true

describe Rdkafka::Admin::DescribeTopicsReport do
  before { @report = described_class.new(FFI::Pointer::NULL) }

  it "is empty for a NULL result" do
    assert_equal([], @report.topics)
  end
end
