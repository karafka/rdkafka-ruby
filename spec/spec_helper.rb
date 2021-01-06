unless ENV["CI"] == "true"
  require "simplecov"
  SimpleCov.start do
    add_filter "/spec/"
  end
end

require "pry"
require "rspec"
require "rdkafka"

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

RSpec.configure do |config|
  config.before(:suite) do
    admin = rdkafka_config.admin
    {
        consume_test_topic:      3,
        empty_test_topic:        3,
        load_test_topic:         3,
        produce_test_topic:      3,
        rake_test_topic:         3,
        watermarks_test_topic:   3,
        partitioner_test_topic: 25,
    }.each do |topic, partitions|
      create_topic_handle = admin.create_topic(topic.to_s, partitions, 1)
      begin
        create_topic_handle.wait(max_wait_timeout: 15)
      rescue Rdkafka::RdkafkaError => ex
        raise unless ex.message.match?(/topic_already_exists/)
      end
    end
    admin.close
  end
end
