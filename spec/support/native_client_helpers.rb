# frozen_string_literal: true

# Helpers for creating native rdkafka clients and topics in tests.
# Can be included as instance methods or called as module methods.
module NativeClientHelpers
  extend self

  def new_native_client
    config = rdkafka_consumer_config
    config.send(:native_kafka, config.send(:native_config), :rd_kafka_producer)
  end

  def new_native_topic(topic_name = "topic_name", native_client:)
    Rdkafka::Bindings.rd_kafka_topic_new(
      native_client,
      topic_name,
      nil
    )
  end
end

# Wrapper for creating raw native rdkafka consumers for low-level testing.
class RdKafkaTestConsumer
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
