require "spec_helper"

describe Rdkafka::RdkafkaError do
  it "should raise a type error for a nil response" do
    expect {
      Rdkafka::RdkafkaError.new(nil)
    }.to raise_error TypeError
  end

  describe "#code" do
    it "should handle an invalid response" do
      expect(Rdkafka::RdkafkaError.new(933975).code).to eq :err_933975?
    end

    it "should return error messages from rdkafka" do
      expect(Rdkafka::RdkafkaError.new(10).code).to eq :msg_size_too_large
    end

    it "should strip a leading underscore" do
      expect(Rdkafka::RdkafkaError.new(-191).code).to eq :partition_eof
    end
  end

  describe "#to_s" do
    it "should handle an invalid response" do
      expect(Rdkafka::RdkafkaError.new(933975).to_s).to eq "Err-933975? (err_933975?)"
    end

    it "should return error messages from rdkafka" do
      expect(Rdkafka::RdkafkaError.new(10).to_s).to eq "Broker: Message size too large (msg_size_too_large)"
    end
  end

  describe "#is_partition_eof?" do
    it "should be false when not partition eof" do
      expect(Rdkafka::RdkafkaError.new(933975).is_partition_eof?).to be false
    end

    it "should be true when partition eof" do
      expect(Rdkafka::RdkafkaError.new(-191).is_partition_eof?).to be true
    end
  end
end
