# frozen_string_literal: true

describe Rdkafka::RdkafkaError do
  it "raises a type error for a nil response" do
    assert_raises(TypeError) { described_class.new(nil) }
  end

  it "creates an error with a message prefix" do
    assert_equal "message prefix", described_class.new(10, "message prefix").message_prefix
  end

  it "creates an error with a broker message" do
    assert_equal "broker message", described_class.new(10, broker_message: "broker message").broker_message
  end

  it "creates an error with an instance name" do
    assert_equal "rdkafka#producer-1", described_class.new(10, instance_name: "rdkafka#producer-1").instance_name
  end

  it "defaults instance name to nil" do
    assert_nil described_class.new(10).instance_name
  end

  describe "#code" do
    it "handles an invalid response" do
      assert_equal :err_933975?, described_class.new(933975).code
    end

    it "returns error messages from rdkafka" do
      assert_equal :msg_size_too_large, described_class.new(10).code
    end

    it "strips a leading underscore" do
      assert_equal :partition_eof, described_class.new(-191).code
    end
  end

  describe "#to_s" do
    it "handles an invalid response" do
      assert_equal "Err-933975? (err_933975?)", described_class.new(933975).to_s
    end

    it "returns error messages from rdkafka" do
      assert_equal "Broker: Message size too large (msg_size_too_large)", described_class.new(10).to_s
    end

    it "adds the message prefix if present" do
      assert_equal "Error explanation - Broker: Message size too large (msg_size_too_large)", described_class.new(10, "Error explanation").to_s
    end

    it "adds the instance name if present" do
      assert_equal "Broker: Message size too large (msg_size_too_large) [rdkafka#producer-1]", described_class.new(10, instance_name: "rdkafka#producer-1").to_s
    end

    it "adds both prefix and instance name" do
      assert_equal "Error explanation - Broker: Message size too large (msg_size_too_large) [rdkafka#producer-1]", described_class.new(10, "Error explanation", instance_name: "rdkafka#producer-1").to_s
    end
  end

  describe "#message" do
    it "handles an invalid response" do
      assert_equal "Err-933975? (err_933975?)", described_class.new(933975).message
    end

    it "returns error messages from rdkafka" do
      assert_equal "Broker: Message size too large (msg_size_too_large)", described_class.new(10).message
    end

    it "adds the message prefix if present" do
      assert_equal "Error explanation - Broker: Message size too large (msg_size_too_large)", described_class.new(10, "Error explanation").message
    end

    it "adds the instance name if present" do
      assert_equal "Broker: Message size too large (msg_size_too_large) [rdkafka#producer-1]", described_class.new(10, instance_name: "rdkafka#producer-1").message
    end
  end

  describe "#is_partition_eof?" do
    it "is false when not partition eof" do
      refute_predicate described_class.new(933975), :is_partition_eof?
    end

    it "is true when partition eof" do
      assert_predicate described_class.new(-191), :is_partition_eof?
    end
  end

  describe "#==" do
    subject { described_class.new(10, "Error explanation") }

    it "equals another error with the same content" do
      assert_equal described_class.new(10, "Error explanation"), subject
    end

    it "does not equal another error with a different error code" do
      refute_equal described_class.new(20, "Error explanation"), subject
    end

    it "does not equal another error with a different message" do
      refute_equal described_class.new(10, "Different error explanation"), subject
    end

    it "does not equal another error with no message" do
      refute_equal described_class.new(10), subject
    end

    it "does not equal another error with a different instance name" do
      error_a = described_class.new(10, instance_name: "rdkafka#producer-1")
      error_b = described_class.new(10, instance_name: "rdkafka#producer-2")

      refute_equal error_b, error_a
    end

    it "equals another error with the same instance name" do
      error_a = described_class.new(10, instance_name: "rdkafka#producer-1")
      error_b = described_class.new(10, instance_name: "rdkafka#producer-1")

      assert_equal error_b, error_a
    end
  end
end

describe Rdkafka::LibraryLoadError do
  it "is a subclass of BaseError" do
    assert_kind_of Rdkafka::BaseError, described_class.new
  end

  it "accepts a message" do
    error = described_class.new("test message")

    assert_equal "test message", error.message
  end
end
