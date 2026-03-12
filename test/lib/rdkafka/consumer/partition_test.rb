# frozen_string_literal: true

describe Rdkafka::Consumer::Partition do
  subject { described_class.new(1, offset, err) }

  let(:offset) { 100 }
  let(:err) { 0 }

  it "has a partition" do
    assert_equal 1, subject.partition
  end

  it "has an offset" do
    assert_equal 100, subject.offset
  end

  it "has an err code" do
    assert_equal 0, subject.err
  end

  describe "#to_s" do
    it "returns a human readable representation" do
      assert_equal "<Partition 1 offset=100>", subject.to_s
    end
  end

  describe "#inspect" do
    it "returns a human readable representation" do
      assert_equal "<Partition 1 offset=100>", subject.to_s
    end

    describe "without offset" do
      let(:offset) { nil }

      it "returns a human readable representation" do
        assert_equal "<Partition 1>", subject.to_s
      end
    end

    describe "with err code" do
      let(:err) { 1 }

      it "returns a human readable representation" do
        assert_equal "<Partition 1 offset=100 err=1>", subject.to_s
      end
    end
  end

  describe "#==" do
    it "equals another partition with the same content" do
      assert_equal described_class.new(1, 100), subject
    end

    it "does not equal another partition with different content" do
      refute_equal described_class.new(2, 101), subject
    end
  end
end
