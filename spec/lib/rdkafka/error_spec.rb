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

  it "has empty frozen details by default" do
    error = described_class.new(10, "message prefix")
    expect(error.details).to eq({})
    expect(error.details).to be_frozen
  end

  it "creates an error with a broker message" do
    expect(described_class.new(10, broker_message: "broker message").broker_message).to eq "broker message"
  end

  it "creates an error with an instance name" do
    expect(described_class.new(10, instance_name: "rdkafka#producer-1").instance_name).to eq "rdkafka#producer-1"
  end

  it "defaults instance_name to nil" do
    expect(described_class.new(10).instance_name).to be_nil
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

    it "adds the instance name if present" do
      expect(described_class.new(10, instance_name: "rdkafka#producer-1").to_s).to eq "Broker: Message size too large (msg_size_too_large) [rdkafka#producer-1]"
    end

    it "adds both message prefix and instance name if present" do
      expect(described_class.new(10, "Error explanation", instance_name: "rdkafka#producer-1").to_s).to eq "Error explanation - Broker: Message size too large (msg_size_too_large) [rdkafka#producer-1]"
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

    it "adds the instance name if present" do
      expect(described_class.new(10, instance_name: "rdkafka#producer-1").message).to eq "Broker: Message size too large (msg_size_too_large) [rdkafka#producer-1]"
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
    let(:error) { described_class.new(10, "Error explanation") }

    it "equals another error with the same content" do
      expect(error).to eq described_class.new(10, "Error explanation")
    end

    it "does not equal another error with a different error code" do
      expect(error).not_to eq described_class.new(20, "Error explanation")
    end

    it "does not equal another error with a different message" do
      expect(error).not_to eq described_class.new(10, "Different error explanation")
    end

    it "does not equal another error with no message" do
      expect(error).not_to eq described_class.new(10)
    end

    it "does not equal another error with a different instance name" do
      error_a = described_class.new(10, instance_name: "rdkafka#producer-1")
      error_b = described_class.new(10, instance_name: "rdkafka#producer-2")
      expect(error_a).not_to eq error_b
    end

    it "equals another error with the same instance name" do
      error_a = described_class.new(10, instance_name: "rdkafka#producer-1")
      error_b = described_class.new(10, instance_name: "rdkafka#producer-1")
      expect(error_a).to eq error_b
    end
  end

  describe "#fatal?" do
    it "returns false for errors created directly without fatal flag" do
      error = described_class.new(10)
      expect(error.fatal?).to be false
    end

    it "returns true when fatal flag is explicitly set" do
      error = described_class.new(47, fatal: true)
      expect(error.fatal?).to be true
    end

    it "returns false for error code -150 when created directly" do
      # Error code -150 is NAMED "fatal" but the flag defaults to false
      error = described_class.new(-150)
      expect(error.code).to eq :fatal
      expect(error.fatal?).to be false
    end
  end

  describe "#retryable?" do
    it "returns false for errors created directly without retryable flag" do
      error = described_class.new(10)
      expect(error.retryable?).to be false
    end

    it "returns true when retryable flag is explicitly set" do
      error = described_class.new(10, retryable: true)
      expect(error.retryable?).to be true
    end
  end

  describe "#abortable?" do
    it "returns false for errors created directly without abortable flag" do
      error = described_class.new(10)
      expect(error.abortable?).to be false
    end

    it "returns true when abortable flag is explicitly set" do
      error = described_class.new(48, abortable: true)
      expect(error.abortable?).to be true
    end
  end

  describe ".build" do
    context "with a rd_kafka_error_t pointer" do
      let(:pointer) { FFI::Pointer.new(1) }

      before do
        allow(Rdkafka::Bindings).to receive(:rd_kafka_error_code).with(pointer).and_return(10)
        allow(Rdkafka::Bindings).to receive_messages(
          rd_kafka_error_is_fatal: 0,
          rd_kafka_error_is_retriable: 0,
          rd_kafka_error_txn_requires_abort: 0
        )
        allow(Rdkafka::Bindings).to receive(:rd_kafka_error_destroy)
      end

      it "forwards broker_message to the built error" do
        error = described_class.build(pointer, broker_message: "broker said no")
        expect(error.broker_message).to eq("broker said no")
      end

      it "forwards instance_name to the built error" do
        error = described_class.build(pointer, instance_name: "rdkafka#producer-1")
        expect(error.instance_name).to eq("rdkafka#producer-1")
      end
    end

    context "with a Bindings::Message struct" do
      let(:message) do
        Rdkafka::Bindings::Message.new.tap { |m| m[:err] = 10 }
      end

      it "forwards instance_name to the built error" do
        error = described_class.build(message, instance_name: "rdkafka#consumer-1")
        expect(error.instance_name).to eq("rdkafka#consumer-1")
      end
    end
  end

  describe ".build_fatal" do
    let(:config) { rdkafka_producer_config("enable.idempotence" => true) }
    let(:producer) { config.producer }

    after do
      producer.close
    end

    it "builds a fatal error from librdkafka's fatal error state" do
      # Include Testing module to access trigger_test_fatal_error
      producer.singleton_class.include(Rdkafka::Testing)

      # Trigger a real fatal error using librdkafka's testing facility
      result = producer.trigger_test_fatal_error(47, "Test fatal error for build_fatal")
      expect(result).to eq(0)

      # Build error from fatal error state
      producer.instance_variable_get(:@native_kafka).with_inner do |inner|
        error = described_class.build_fatal(inner)

        expect(error).to be_a(described_class)
        expect(error.rdkafka_response).to eq(47)
        expect(error.code).to eq(:invalid_producer_epoch)
        expect(error.fatal?).to be true
        expect(error.broker_message).to eq("test_fatal_error: Test fatal error for build_fatal")
      end

      producer.close
    end

    it "uses fallback when no fatal error is present" do
      # Call build_fatal on a producer without a fatal error
      producer.instance_variable_get(:@native_kafka).with_inner do |inner|
        error = described_class.build_fatal(
          inner,
          fallback_error_code: 999,
          fallback_message: "Fallback message"
        )

        expect(error).to be_a(described_class)
        expect(error.rdkafka_response).to eq(999)
        expect(error.fatal?).to be true
        expect(error.broker_message).to eq("Fallback message")
      end
    end
  end

  describe ".validate! with integrated fatal error handling" do
    let(:config) { rdkafka_producer_config("enable.idempotence" => true) }
    let(:producer) { config.producer }

    after do
      # Only close if not already marked for cleanup (fatal error tests mark for cleanup)
      producer.close unless producer.instance_variable_get(:@native_kafka).closed?
    end

    it "discovers underlying fatal error when client_ptr provided" do
      producer.singleton_class.include(Rdkafka::Testing)
      producer.trigger_test_fatal_error(47, "Test fatal error for validate!")

      expect {
        producer.instance_variable_get(:@native_kafka).with_inner do |inner|
          described_class.validate!(-150, client_ptr: inner)
        end
      }.to raise_error(described_class) do |error|
        expect(error.rdkafka_response).to eq(47)  # Remapped from -150
        expect(error.code).to eq(:invalid_producer_epoch)
        expect(error.fatal?).to be true
        expect(error.broker_message).to eq("test_fatal_error: Test fatal error for validate!")
      end

      producer.close
    end

    it "raises fatal error without remapping when client_ptr not provided" do
      # Without client_ptr, -150 is raised as-is without remapping
      expect {
        described_class.validate!(-150)
      }.to raise_error(described_class) do |error|
        expect(error.rdkafka_response).to eq(-150)
        expect(error.code).to eq(:fatal)
        expect(error.fatal?).to be false  # Not marked as fatal without client_ptr
      end
    end

    it "handles non-fatal errors normally without client_ptr" do
      expect {
        described_class.validate!(10, "Test prefix")
      }.to raise_error(described_class) do |error|
        expect(error.rdkafka_response).to eq(10)
        expect(error.code).to eq(:msg_size_too_large)
        expect(error.fatal?).to be false
        expect(error.message_prefix).to eq("Test prefix")
      end
    end

    it "handles non-fatal errors normally with client_ptr" do
      expect {
        producer.instance_variable_get(:@native_kafka).with_inner do |inner|
          described_class.validate!(10, "Test prefix", client_ptr: inner)
        end
      }.to raise_error(described_class) do |error|
        expect(error.rdkafka_response).to eq(10)
        expect(error.code).to eq(:msg_size_too_large)
        expect(error.fatal?).to be false
        expect(error.message_prefix).to eq("Test prefix")
      end
    end

    it "returns false when no error (with client_ptr)" do
      result = producer.instance_variable_get(:@native_kafka).with_inner do |inner|
        described_class.validate!(0, client_ptr: inner)
      end
      expect(result).to be false
    end

    it "returns false when no error (without client_ptr)" do
      result = described_class.validate!(0)
      expect(result).to be false
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
