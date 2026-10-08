# frozen_string_literal: true

RSpec.describe Rdkafka::Admin::DescribeTopicsReport do
  subject(:report) { described_class.new(FFI::Pointer::NULL) }

  it "is empty for a NULL result" do
    expect(report.topics).to eq([])
  end
end
