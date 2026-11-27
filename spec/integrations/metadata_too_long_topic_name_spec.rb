# frozen_string_literal: true

# This integration test verifies that a topic name longer than librdkafka accepts (512 chars)
# raises an `Rdkafka::RdkafkaError` instead of crashing the process.
#
# For such a name `rd_kafka_topic_new` returns NULL, and a NULL `FFI::Pointer` is truthy.
#
# Requires a running Kafka broker at localhost:9092.
#
# Exit codes:
# - 0: every too long name raised `topic_exception` and a 512 chars name still reaches the broker
# - 1: a call did not raise the expected error (a crash ends the process with a signal)

require "rdkafka"

TOO_LONG = "a" * 513
AT_LIMIT = "a" * 512

def topic_exception?
  yield
  false
rescue Rdkafka::RdkafkaError => e
  e.code == :topic_exception
end

admin = Rdkafka::Config.new("bootstrap.servers": "localhost:9092").admin
producer = Rdkafka::Config.new("bootstrap.servers": "localhost:9092").producer
consumer = Rdkafka::Config.new(
  "bootstrap.servers": "localhost:9092",
  "group.id": "metadata-too-long-topic-name"
).consumer

begin
  exit(1) unless topic_exception? { admin.metadata(TOO_LONG) }
  exit(1) unless topic_exception? { admin.metadata(AT_LIMIT) }
  exit(1) unless topic_exception? { producer.metadata(TOO_LONG) }
  exit(1) unless topic_exception? { producer.partition_count(TOO_LONG) }
  exit(1) unless topic_exception? { consumer.metadata(TOO_LONG) }
  exit(1) unless topic_exception? { consumer.seek_by(TOO_LONG, 0, 0) }
ensure
  consumer.close
  producer.close
  admin.close
end
