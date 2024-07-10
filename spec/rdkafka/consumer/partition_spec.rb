require "spec_helper"

describe Rdkafka::Consumer::Partition do
  let(:offset) { 100 }
  let(:err) { 0 }
  subject { Rdkafka::Consumer::Partition.new(1, offset, err) }

  it "should have a partition" do
    expect(subject.partition).to eq 1
  end

  it "should have an offset" do
    expect(subject.offset).to eq 100
  end

  it "should have an err code" do
    expect(subject.err).to eq 0
  end

  describe "#to_s" do
    it "should return a human readable representation" do
      expect(subject.to_s).to eq "<Partition 1 offset=100>"
    end
  end

  describe "#inspect" do
    it "should return a human readable representation" do
      expect(subject.to_s).to eq "<Partition 1 offset=100>"
    end

    context "without offset" do
      let(:offset) { nil }

      it "should return a human readable representation" do
        expect(subject.to_s).to eq "<Partition 1>"
      end
    end

    context "with err code" do
      let(:err) { 1 }

      it "should return a human readable representation" do
        expect(subject.to_s).to eq "<Partition 1 offset=100 err=1>"
      end
    end
  end

  describe "#==" do
    it "should equal another partition with the same content" do
      expect(subject).to eq Rdkafka::Consumer::Partition.new(1, 100)
    end

    it "should not equal another partition with different content" do
      expect(subject).not_to eq Rdkafka::Consumer::Partition.new(2, 101)
    end
  end
end
