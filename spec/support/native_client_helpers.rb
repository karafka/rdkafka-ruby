# frozen_string_literal: true

# Helpers for creating native rdkafka clients and topics in tests.
# Can be included as instance methods or called as module methods.
module NativeClientHelpers
  extend self

  # Creates a new native rdkafka client (producer type) for low-level testing.
  # Uses the consumer config internally but creates a producer-type client.
  #
  # @return [FFI::Pointer] pointer to the native rd_kafka_t instance
  def new_native_client
    config = rdkafka_consumer_config
    config.send(:native_kafka, config.send(:native_config), :rd_kafka_producer)
  end

  # Creates a new native rdkafka topic handle for low-level testing.
  #
  # @param topic_name [String] the topic name to create a handle for
  # @param native_client [FFI::Pointer] pointer to a native rd_kafka_t instance
  # @return [FFI::Pointer] pointer to the native rd_kafka_topic_t instance
  def new_native_topic(topic_name = "topic_name", native_client:)
    Rdkafka::Bindings.rd_kafka_topic_new(
      native_client,
      topic_name,
      nil
    )
  end
end

# Wrapper for creating raw native rdkafka consumers for low-level testing.
# Provides a block-based interface that ensures proper cleanup of the native consumer.
class RdKafkaTestConsumer
  # Creates a raw native rdkafka consumer, yields it to the block, and ensures
  # it is properly closed and destroyed afterward.
  #
  # @yield [FFI::Pointer] pointer to the native rd_kafka_t consumer instance
  # @return [Object] the return value of the block
  def self.with
    consumer = Rdkafka::Bindings.rd_kafka_new(
      :rd_kafka_consumer,
      nil,
      nil,
      0
    )
    yield consumer
  ensure
    Rdkafka::Bindings.rd_kafka_consumer_close(consumer)
    Rdkafka::Bindings.rd_kafka_destroy(consumer)
  end
end
