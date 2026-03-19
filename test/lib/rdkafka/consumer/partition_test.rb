# frozen_string_literal: true

require_relative "../../../test_helper"

describe Rdkafka::Consumer::Partition do
  before do
    @offset = 100
    @err = 0
    @partition = described_class.new(1, @offset, @err)
  end

  it "has a partition" do
    assert_equal 1, @partition.partition
  end

  it "has an offset" do
    assert_equal 100, @partition.offset
  end

  it "has an err code" do
    assert_equal 0, @partition.err
  end

  describe "#to_s" do
    it "returns a human readable representation" do
      assert_equal "<Partition 1 offset=100>", @partition.to_s
    end
  end

  describe "#inspect" do
    it "returns a human readable representation" do
      assert_equal "<Partition 1 offset=100>", @partition.to_s
    end

    context "without offset" do
      before do
        @partition_no_offset = described_class.new(1, nil, 0)
      end

      it "returns a human readable representation" do
        assert_equal "<Partition 1>", @partition_no_offset.to_s
      end
    end

    context "with err code" do
      before do
        @partition_with_err = described_class.new(1, 100, 1)
      end

      it "returns a human readable representation" do
        assert_equal "<Partition 1 offset=100 err=1>", @partition_with_err.to_s
      end
    end
  end

  describe "#==" do
    it "equals another partition with the same content" do
      assert_equal described_class.new(1, 100), @partition
    end

    it "does not equal another partition with different content" do
      refute_equal described_class.new(2, 101), @partition
    end
  end
end
