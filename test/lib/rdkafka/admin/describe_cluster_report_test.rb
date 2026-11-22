# frozen_string_literal: true

describe Rdkafka::Admin::DescribeClusterReport do
  before { @report = described_class.new(FFI::Pointer::NULL) }

  it "is empty for a NULL result" do
    assert_nil(@report.cluster_id)
    assert_nil(@report.controller)
    assert_equal([], @report.nodes)
    assert_nil(@report.authorized_operations)
  end
end
