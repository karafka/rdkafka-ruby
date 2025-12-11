# frozen_string_literal: true

RSpec.describe Rdkafka::RdkafkaError do
  it "should raise a type error for a nil response" do
    expect {
      Rdkafka::RdkafkaError.new(nil)
    }.to raise_error TypeError
  end

  it "should create an error with a message prefix" do
    expect(Rdkafka::RdkafkaError.new(10, "message prefix").message_prefix).to eq "message prefix"
  end

  it "should have empty frozen details by default" do
    error = Rdkafka::RdkafkaError.new(10, "message prefix")
    expect(error.details).to eq({})
    expect(error.details).to be_frozen
  end

  it "should create an error with a broker message" do
    expect(Rdkafka::RdkafkaError.new(10, broker_message: "broker message").broker_message).to eq "broker message"
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

    it "should add the message prefix if present" do
      expect(Rdkafka::RdkafkaError.new(10, "Error explanation").to_s).to eq "Error explanation - Broker: Message size too large (msg_size_too_large)"
    end
  end

  describe "#message" do
    it "should handle an invalid response" do
      expect(Rdkafka::RdkafkaError.new(933975).message).to eq "Err-933975? (err_933975?)"
    end

    it "should return error messages from rdkafka" do
      expect(Rdkafka::RdkafkaError.new(10).message).to eq "Broker: Message size too large (msg_size_too_large)"
    end

    it "should add the message prefix if present" do
      expect(Rdkafka::RdkafkaError.new(10, "Error explanation").message).to eq "Error explanation - Broker: Message size too large (msg_size_too_large)"
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

  describe "#==" do
    subject { Rdkafka::RdkafkaError.new(10, "Error explanation") }

    it "should equal another error with the same content" do
      expect(subject).to eq Rdkafka::RdkafkaError.new(10, "Error explanation")
    end

    it "should not equal another error with a different error code" do
      expect(subject).not_to eq Rdkafka::RdkafkaError.new(20, "Error explanation")
    end

    it "should not equal another error with a different message" do
      expect(subject).not_to eq Rdkafka::RdkafkaError.new(10, "Different error explanation")
    end

    it "should not equal another error with no message" do
      expect(subject).not_to eq Rdkafka::RdkafkaError.new(10)
    end
  end

  describe "#fatal?" do
    it "should return false for errors created directly without fatal flag" do
      error = Rdkafka::RdkafkaError.new(10)
      expect(error.fatal?).to be false
    end

    it "should return true when fatal flag is explicitly set" do
      error = Rdkafka::RdkafkaError.new(47, fatal: true)
      expect(error.fatal?).to be true
    end

    it "should return false for error code -150 when created directly" do
      # Error code -150 is NAMED "fatal" but the flag defaults to false
      error = Rdkafka::RdkafkaError.new(-150)
      expect(error.code).to eq :fatal
      expect(error.fatal?).to be false
    end
  end

  describe "#retryable?" do
    it "should return false for errors created directly without retryable flag" do
      error = Rdkafka::RdkafkaError.new(10)
      expect(error.retryable?).to be false
    end

    it "should return true when retryable flag is explicitly set" do
      error = Rdkafka::RdkafkaError.new(10, retryable: true)
      expect(error.retryable?).to be true
    end
  end

  describe "#abortable?" do
    it "should return false for errors created directly without abortable flag" do
      error = Rdkafka::RdkafkaError.new(10)
      expect(error.abortable?).to be false
    end

    it "should return true when abortable flag is explicitly set" do
      error = Rdkafka::RdkafkaError.new(48, abortable: true)
      expect(error.abortable?).to be true
    end
  end

  describe ".build_fatal" do
    let(:config) { rdkafka_producer_config('enable.idempotence' => true) }
    let(:producer) { config.producer }

    after do
      producer.close
    end

    it "should build a fatal error from librdkafka's fatal error state" do
      # Include Testing module to access trigger_test_fatal_error
      producer.singleton_class.include(Rdkafka::Testing)

      # Trigger a real fatal error using librdkafka's testing facility
      result = producer.trigger_test_fatal_error(47, "Test fatal error for build_fatal")
      expect(result).to eq(0)

      # Build error from fatal error state
      producer.instance_variable_get(:@native_kafka).with_inner do |inner|
        error = Rdkafka::RdkafkaError.build_fatal(inner)

        expect(error).to be_a(Rdkafka::RdkafkaError)
        expect(error.rdkafka_response).to eq(47)
        expect(error.code).to eq(:invalid_producer_epoch)
        expect(error.fatal?).to be true
        expect(error.broker_message).to eq("test_fatal_error: Test fatal error for build_fatal")
      end

      producer.close
    end

    it "should use fallback when no fatal error is present" do
      # Call build_fatal on a producer without a fatal error
      producer.instance_variable_get(:@native_kafka).with_inner do |inner|
        error = Rdkafka::RdkafkaError.build_fatal(
          inner,
          fallback_error_code: 999,
          fallback_message: "Fallback message"
        )

        expect(error).to be_a(Rdkafka::RdkafkaError)
        expect(error.rdkafka_response).to eq(999)
        expect(error.fatal?).to be true
        expect(error.broker_message).to eq("Fallback message")
      end
    end
  end

  describe ".validate! with integrated fatal error handling" do
    let(:config) { rdkafka_producer_config('enable.idempotence' => true) }
    let(:producer) { config.producer }

    after do
      # Only close if not already marked for cleanup (fatal error tests mark for cleanup)
      producer.close unless producer.instance_variable_get(:@native_kafka).closed?
    end

    it "should discover underlying fatal error when client_ptr provided" do
      producer.singleton_class.include(Rdkafka::Testing)
      producer.trigger_test_fatal_error(47, "Test fatal error for validate!")

      expect {
        producer.instance_variable_get(:@native_kafka).with_inner do |inner|
          Rdkafka::RdkafkaError.validate!(-150, client_ptr: inner)
        end
      }.to raise_error(Rdkafka::RdkafkaError) do |error|
        expect(error.rdkafka_response).to eq(47)  # Remapped from -150
        expect(error.code).to eq(:invalid_producer_epoch)
        expect(error.fatal?).to be true
        expect(error.broker_message).to eq("test_fatal_error: Test fatal error for validate!")
      end

      producer.close
    end

    it "should raise fatal error without remapping when client_ptr not provided" do
      # Without client_ptr, -150 is raised as-is without remapping
      expect {
        Rdkafka::RdkafkaError.validate!(-150)
      }.to raise_error(Rdkafka::RdkafkaError) do |error|
        expect(error.rdkafka_response).to eq(-150)
        expect(error.code).to eq(:fatal)
        expect(error.fatal?).to be false  # Not marked as fatal without client_ptr
      end
    end

    it "should handle non-fatal errors normally without client_ptr" do
      expect {
        Rdkafka::RdkafkaError.validate!(10, "Test prefix")
      }.to raise_error(Rdkafka::RdkafkaError) do |error|
        expect(error.rdkafka_response).to eq(10)
        expect(error.code).to eq(:msg_size_too_large)
        expect(error.fatal?).to be false
        expect(error.message_prefix).to eq("Test prefix")
      end
    end

    it "should handle non-fatal errors normally with client_ptr" do
      expect {
        producer.instance_variable_get(:@native_kafka).with_inner do |inner|
          Rdkafka::RdkafkaError.validate!(10, "Test prefix", client_ptr: inner)
        end
      }.to raise_error(Rdkafka::RdkafkaError) do |error|
        expect(error.rdkafka_response).to eq(10)
        expect(error.code).to eq(:msg_size_too_large)
        expect(error.fatal?).to be false
        expect(error.message_prefix).to eq("Test prefix")
      end
    end

    it "should return false when no error (with client_ptr)" do
      result = producer.instance_variable_get(:@native_kafka).with_inner do |inner|
        Rdkafka::RdkafkaError.validate!(0, client_ptr: inner)
      end
      expect(result).to be false
    end

    it "should return false when no error (without client_ptr)" do
      result = Rdkafka::RdkafkaError.validate!(0)
      expect(result).to be false
    end
  end
end

RSpec.describe Rdkafka::LibraryLoadError do
  it "should be a subclass of BaseError" do
    expect(Rdkafka::LibraryLoadError.new).to be_a(Rdkafka::BaseError)
  end

  it "should accept a message" do
    error = Rdkafka::LibraryLoadError.new("test message")
    expect(error.message).to eq("test message")
  end
end
