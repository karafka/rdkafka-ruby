# frozen_string_literal: true

# Centralized stubbing helpers for Rdkafka::Bindings FFI methods.
# These patterns standardize how FFI bindings are mocked across tests,
# making minitest migration easier (replace with mocha stubs or manual method replacement).
#
# In minitest/mocha, these would become:
#   stub_binding_return(:rd_kafka_subscribe, 20) → Rdkafka::Bindings.stubs(:rd_kafka_subscribe).returns(20)
#   stub_binding_raise(:rd_kafka_CreateTopics, RuntimeError.new("oops")) → similar mocha pattern
module MockBindingsHelpers
  extend self

  # Stub an Rdkafka::Bindings method to return a specific value.
  # Used primarily to simulate error codes from FFI calls.
  #
  # Example: stub_binding_return(:rd_kafka_subscribe, 20)
  def stub_binding_return(method, return_value)
    allow(Rdkafka::Bindings).to receive(method).and_return(return_value)
  end

  # Stub an Rdkafka::Bindings method to raise an exception.
  # Used to test error handling when FFI calls fail unexpectedly.
  #
  # Example: stub_binding_raise(:rd_kafka_CreateTopics, RuntimeError.new("oops"))
  def stub_binding_raise(method, exception)
    allow(Rdkafka::Bindings).to receive(method).and_raise(exception)
  end

  # Stub the background queue to return NULL (simulating unavailability).
  # Used in admin specs to test error handling when background queue is not available.
  def stub_null_background_queue
    allow(Rdkafka::Bindings).to receive(:rd_kafka_queue_get_background).and_return(FFI::Pointer::NULL)
  end

  # Stub rd_kafka_message_headers to prevent segfaults when testing with
  # manually constructed native messages (which lack proper header support).
  #
  # This is a safety mock — removing it causes segmentation faults because
  # librdkafka tries to access memory that was never properly initialized.
  def stub_message_headers_unavailable
    allow(Rdkafka::Bindings).to receive(:rd_kafka_message_headers)
      .with(any_args)
      .and_return(Rdkafka::Bindings::RD_KAFKA_RESP_ERR__NOENT)
  end

  # Stub rd_kafka_message_timestamp to return a specific millisecond value.
  # Used in message tests to control timestamp behavior without a real Kafka message.
  def stub_message_timestamp(timestamp_ms)
    allow(Rdkafka::Bindings).to receive(:rd_kafka_message_timestamp).and_return(timestamp_ms)
  end
end
