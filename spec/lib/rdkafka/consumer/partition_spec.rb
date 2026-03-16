# frozen_string_literal: true

RSpec.describe Rdkafka::Consumer::Partition do
  def build_partition(offset: 100, err: 0)
    Rdkafka::Consumer::Partition.new(1, offset, err)
  end

  it "has a partition" do
    expect(build_partition.partition).to eq 1
  end

  it "has an offset" do
    expect(build_partition.offset).to eq 100
  end

  it "has an err code" do
    expect(build_partition.err).to eq 0
  end

  describe "#to_s" do
    it "returns a human readable representation" do
      expect(build_partition.to_s).to eq "<Partition 1 offset=100>"
    end
  end

  describe "#inspect" do
    it "returns a human readable representation" do
      expect(build_partition.to_s).to eq "<Partition 1 offset=100>"
    end

    context "without offset" do
      it "returns a human readable representation" do
        expect(build_partition(offset: nil).to_s).to eq "<Partition 1>"
      end
    end

    context "with err code" do
      it "returns a human readable representation" do
        expect(build_partition(err: 1).to_s).to eq "<Partition 1 offset=100 err=1>"
      end
    end
  end

  describe "#==" do
    it "equals another partition with the same content" do
      expect(build_partition).to eq described_class.new(1, 100)
    end

    it "does not equal another partition with different content" do
      expect(build_partition).not_to eq described_class.new(2, 101)
    end
  end
end
