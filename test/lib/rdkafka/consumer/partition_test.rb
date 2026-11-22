# frozen_string_literal: true

describe Rdkafka::Consumer::Partition do
  before do
    @offset = 100
    @err = 0
  end

  def build_partition
    described_class.new(1, @offset, @err)
  end

  it "has a partition" do
    assert_equal(1, build_partition.partition)
  end

  it "has an offset" do
    assert_equal(100, build_partition.offset)
  end

  it "has an err code" do
    assert_equal(0, build_partition.err)
  end

  describe "#to_s" do
    it "returns a human readable representation" do
      assert_equal("<Partition 1 offset=100>", build_partition.to_s)
    end
  end

  describe "#inspect" do
    it "returns a human readable representation" do
      assert_equal("<Partition 1 offset=100>", build_partition.to_s)
    end

    context "without offset" do
      before { @offset = nil }

      it "returns a human readable representation" do
        assert_equal("<Partition 1>", build_partition.to_s)
      end
    end

    context "with err code" do
      before { @err = 1 }

      it "returns a human readable representation" do
        assert_equal("<Partition 1 offset=100 err=1>", build_partition.to_s)
      end
    end
  end

  describe "#==" do
    it "equals another partition with the same content" do
      assert_equal(described_class.new(1, 100), build_partition)
    end

    it "does not equal another partition with different content" do
      refute_equal(described_class.new(2, 101), build_partition)
    end
  end
end
