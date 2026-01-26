# frozen_string_literal: true

RSpec.describe Rdkafka::RdkafkaError do
  it "raises a type error for a nil response" do
    expect {
      described_class.new(nil)
    }.to raise_error TypeError
  end

  it "creates an error with a message prefix" do
    expect(described_class.new(10, "message prefix").message_prefix).to eq "message prefix"
  end

  it "creates an error with a broker message" do
    expect(described_class.new(10, broker_message: "broker message").broker_message).to eq "broker message"
  end

  describe "#code" do
    it "handles an invalid response" do
      expect(described_class.new(933975).code).to eq :err_933975?
    end

    it "returns error messages from rdkafka" do
      expect(described_class.new(10).code).to eq :msg_size_too_large
    end

    it "strips a leading underscore" do
      expect(described_class.new(-191).code).to eq :partition_eof
    end
  end

  describe "#to_s" do
    it "handles an invalid response" do
      expect(described_class.new(933975).to_s).to eq "Err-933975? (err_933975?)"
    end

    it "returns error messages from rdkafka" do
      expect(described_class.new(10).to_s).to eq "Broker: Message size too large (msg_size_too_large)"
    end

    it "adds the message prefix if present" do
      expect(described_class.new(10, "Error explanation").to_s).to eq "Error explanation - Broker: Message size too large (msg_size_too_large)"
    end
  end

  describe "#message" do
    it "handles an invalid response" do
      expect(described_class.new(933975).message).to eq "Err-933975? (err_933975?)"
    end

    it "returns error messages from rdkafka" do
      expect(described_class.new(10).message).to eq "Broker: Message size too large (msg_size_too_large)"
    end

    it "adds the message prefix if present" do
      expect(described_class.new(10, "Error explanation").message).to eq "Error explanation - Broker: Message size too large (msg_size_too_large)"
    end
  end

  describe "#is_partition_eof?" do
    it "is false when not partition eof" do
      expect(described_class.new(933975).is_partition_eof?).to be false
    end

    it "is true when partition eof" do
      expect(described_class.new(-191).is_partition_eof?).to be true
    end
  end

  describe "#==" do
    subject { described_class.new(10, "Error explanation") }

    it "equals another error with the same content" do
      expect(subject).to eq described_class.new(10, "Error explanation")
    end

    it "does not equal another error with a different error code" do
      expect(subject).not_to eq described_class.new(20, "Error explanation")
    end

    it "does not equal another error with a different message" do
      expect(subject).not_to eq described_class.new(10, "Different error explanation")
    end

    it "does not equal another error with no message" do
      expect(subject).not_to eq described_class.new(10)
    end
  end
end

RSpec.describe Rdkafka::LibraryLoadError do
  it "is a subclass of BaseError" do
    expect(described_class.new).to be_a(Rdkafka::BaseError)
  end

  it "accepts a message" do
    error = described_class.new("test message")
    expect(error.message).to eq("test message")
  end
end
