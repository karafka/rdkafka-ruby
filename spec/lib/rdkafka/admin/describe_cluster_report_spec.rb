# frozen_string_literal: true

RSpec.describe Rdkafka::Admin::DescribeClusterReport do
  subject(:report) { described_class.new(FFI::Pointer::NULL) }

  it "is empty for a NULL result" do
    expect(report.cluster_id).to be_nil
    expect(report.controller).to be_nil
    expect(report.nodes).to eq([])
    expect(report.authorized_operations).to be_nil
  end
end
