require "spec_helper"

describe Rdkafka::RdkafkaError do
  describe "#code" do
    it "should handle a nil response" do
      expect(Rdkafka::RdkafkaError.new(nil).code).to eq :unknown_error
    end

    it "should handle an invalid response" do
      expect(Rdkafka::RdkafkaError.new(933975).code).to eq :err_933975?
    end

    it "should return error messages from rdkafka" do
      expect(Rdkafka::RdkafkaError.new(10).code).to eq :msg_size_too_large
    end
  end

  describe "#to_s" do
    it "should handle a nil response" do
      expect(Rdkafka::RdkafkaError.new(nil).to_s).to eq "Unknown error: Response code is nil"
    end

    it "should handle an invalid response" do
      expect(Rdkafka::RdkafkaError.new(933975).to_s).to eq "Err-933975?"
    end

    it "should return error messages from rdkafka" do
      expect(Rdkafka::RdkafkaError.new(10).to_s).to eq "Broker: Message size too large"
    end
  end
end
