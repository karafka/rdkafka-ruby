# frozen_string_literal: true

class RdKafkaTestConsumer
  def self.with
    config = Rdkafka::Bindings.rd_kafka_conf_new
    errstr = FFI::MemoryPointer.new(:char, 256)
    consumer = Rdkafka::Bindings.rd_kafka_new(
      :rd_kafka_consumer,
      config,
      errstr,
      256
    )
    raise "Failed to create test consumer: #{errstr.read_string}" if consumer.null?
    yield consumer
  ensure
    if consumer && !consumer.null?
      Rdkafka::Bindings.rd_kafka_consumer_close(consumer)
      Rdkafka::Bindings.rd_kafka_destroy(consumer)
    end
  end
end
