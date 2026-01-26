# frozen_string_literal: true

RSpec.describe Rdkafka::Consumer::Partition do
  subject { described_class.new(1, offset, err) }

  let(:offset) { 100 }
  let(:err) { 0 }

  it "has a partition" do
    expect(subject.partition).to eq 1
  end

  it "has an offset" do
    expect(subject.offset).to eq 100
  end

  it "has an err code" do
    expect(subject.err).to eq 0
  end

  describe "#to_s" do
    it "returns a human readable representation" do
      expect(subject.to_s).to eq "<Partition 1 offset=100>"
    end
  end

  describe "#inspect" do
    it "returns a human readable representation" do
      expect(subject.to_s).to eq "<Partition 1 offset=100>"
    end

    context "without offset" do
      let(:offset) { nil }

      it "returns a human readable representation" do
        expect(subject.to_s).to eq "<Partition 1>"
      end
    end

    context "with err code" do
      let(:err) { 1 }

      it "returns a human readable representation" do
        expect(subject.to_s).to eq "<Partition 1 offset=100 err=1>"
      end
    end
  end

  describe "#==" do
    it "equals another partition with the same content" do
      expect(subject).to eq described_class.new(1, 100)
    end

    it "does not equal another partition with different content" do
      expect(subject).not_to eq described_class.new(2, 101)
    end
  end
end
