unless ENV["CI"] == "true"
  require "simplecov"
  SimpleCov.start do
    add_filter "/spec/"
  end
end

require "pry"
require "rspec"
require "rdkafka"

`docker-compose exec kafka kafka-topics --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 3 --if-not-exists --topic consume_test_topic`
`docker-compose exec kafka kafka-topics --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 3 --if-not-exists --topic empty_test_topic`
`docker-compose exec kafka kafka-topics --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 3 --if-not-exists --topic load_test_topic`
`docker-compose exec kafka kafka-topics --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 3 --if-not-exists --topic produce_test_topic`
`docker-compose exec kafka kafka-topics --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 3 --if-not-exists --topic rake_test_topic`
`docker-compose exec kafka kafka-topics --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 3 --if-not-exists --topic watermarks_test_topic`
`docker-compose exec kafka kafka-topics --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 25 --if-not-exists --topic partitioner_test_topic`

def rdkafka_config(config_overrides={})
  config = {
    :"api.version.request" => false,
    :"broker.version.fallback" => "1.0",
    :"bootstrap.servers" => "localhost:9092",
    :"group.id" => "ruby-test-#{Random.new.rand(0..1_000_000)}",
    :"auto.offset.reset" => "earliest",
    :"enable.partition.eof" => false
  }
  if ENV["DEBUG_PRODUCER"]
    config[:debug] = "broker,topic,msg"
  elsif ENV["DEBUG_CONSUMER"]
    config[:debug] = "cgrp,topic,fetch"
  end
  config.merge!(config_overrides)
  Rdkafka::Config.new(config)
end

def new_native_client
  config = rdkafka_config
  config.send(:native_kafka, config.send(:native_config), :rd_kafka_producer)
end

def new_native_topic(topic_name="topic_name", native_client: )
  Rdkafka::Bindings.rd_kafka_topic_new(
    native_client,
    topic_name,
    nil
  )
end

def wait_for_message(topic:, delivery_report:, timeout_in_seconds: 30, consumer: nil)
  new_consumer = !!consumer
  consumer ||= rdkafka_config.consumer
  consumer.subscribe(topic)
  timeout = Time.now.to_i + timeout_in_seconds
  loop do
    if timeout <= Time.now.to_i
      raise "Timeout of #{timeout_in_seconds} seconds reached in wait_for_message"
    end
    message = consumer.poll(100)
    if message &&
        message.partition == delivery_report.partition &&
        message.offset == delivery_report.offset
      return message
    end
  end
ensure
  consumer.close if new_consumer
end

def wait_for_assignment(consumer)
  10.times do
    break if !consumer.assignment.empty?
    sleep 1
  end
end

def wait_for_unassignment(consumer)
  10.times do
    break if consumer.assignment.empty?
    sleep 1
  end
end
