# frozen_string_literal: true

Warning[:performance] = true if RUBY_VERSION >= "3.3"
Warning[:deprecated] = true
$VERBOSE = true

require "warning"

Warning.process do |warning|
  next unless warning.include?(Dir.pwd)
  # Allow OpenStruct usage only in tests
  next if warning.include?("OpenStruct use") && warning.include?("_test")

  raise "Warning in your code: #{warning}"
end

require "simplecov"
SimpleCov.start do
  add_filter "/test/"
  enable_coverage :branch
end

require "minitest/autorun"
require "minitest/spec"
require "minitest/mock"
require "pry"
require "rdkafka"
require "timeout"
require "securerandom"

require_relative "support/minitest_extensions"
require_relative "support/minitest_setup"
require_relative "support/timeout_test"
require_relative "support/rd_kafka_test_consumer"

# Module to hold dynamically generated test topics with UUIDs
module TestTopics
  class << self
    # Generate a unique topic name with it- prefix and UUID
    def unique
      "it-#{SecureRandom.uuid}"
    end

    # Shared topic for partition_count tests (needs stable partition count = 1)
    def example_topic
      @example_topic ||= unique
    end

    # Shared topic for consumer tests (3 partitions)
    def consume_test_topic
      @consume_test_topic ||= unique
    end

    # Shared topic for producer tests (3 partitions)
    def produce_test_topic
      @produce_test_topic ||= unique
    end

    # Shared topic for producer partitioner tests (25 partitions)
    def partitioner_test_topic
      @partitioner_test_topic ||= unique
    end
  end
end

def rdkafka_base_config
  if ENV["KAFKA_SSL_ENABLED"] == "true"
    {
      "bootstrap.servers": "localhost:9093",
      # Display statistics and refresh often just to cover those in tests
      "statistics.interval.ms": 1_000,
      "topic.metadata.refresh.interval.ms": 1_000,
      # SSL Configuration
      "security.protocol": "SSL",
      "ssl.ca.location": "./ssl/ca-cert",
      "ssl.endpoint.identification.algorithm": "none"
    }
  else
    {
      "bootstrap.servers": "127.0.0.1:9092",
      # Display statistics and refresh often just to cover those in tests
      "statistics.interval.ms": 1_000,
      "topic.metadata.refresh.interval.ms": 1_000
    }
  end
end

def rdkafka_config(config_overrides = {})
  # Generate the base config
  config = rdkafka_base_config
  # Merge overrides
  config.merge!(config_overrides)
  # Return it
  Rdkafka::Config.new(config)
end

def rdkafka_consumer_config(config_overrides = {})
  # Generate the base config
  config = rdkafka_base_config
  # Add consumer specific fields to it
  config[:"auto.offset.reset"] = "earliest"
  config[:"enable.partition.eof"] = false
  config[:"group.id"] = "ruby-test-#{SecureRandom.uuid}"
  # Enable debug mode if required
  if ENV["DEBUG_CONSUMER"]
    config[:debug] = "cgrp,topic,fetch"
  end
  # Merge overrides
  config.merge!(config_overrides)
  # Return it
  Rdkafka::Config.new(config)
end

def rdkafka_producer_config(config_overrides = {})
  # Generate the base config
  config = rdkafka_base_config
  # Enable debug mode if required
  if ENV["DEBUG_PRODUCER"]
    config[:debug] = "broker,topic,msg"
  end
  # Merge overrides
  config.merge!(config_overrides)
  # Return it
  Rdkafka::Config.new(config)
end

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

def wait_for_message(topic:, delivery_report:, timeout_in_seconds: 60, consumer: nil)
  # When no consumer is passed, use direct partition assignment with auto-commit
  # disabled. This avoids consumer group rebalance delays that cause flaky timeouts.
  # When a consumer IS passed, use subscribe so the caller retains group state
  # (needed for tests that check position/committed offsets after consuming).
  if consumer
    consumer.subscribe(topic)
    wait_for_assignment(consumer)
    fetch_consumer = consumer
  else
    fetch_consumer = rdkafka_consumer_config(
      "enable.auto.commit": false,
      "allow.auto.create.topics": true
    ).consumer

    tpl = Rdkafka::Consumer::TopicPartitionList.new
    tpl.add_topic(topic, [delivery_report.partition])
    fetch_consumer.assign(tpl)

    timeout = Time.now.to_i + timeout_in_seconds

    # Poll + seek loop: wait for partition to be ready, then seek to target offset
    loop do
      if timeout <= Time.now.to_i
        raise "Timeout of #{timeout_in_seconds} seconds reached in wait_for_message"
      end

      begin
        fetch_consumer.poll(500)
        fetch_consumer.seek_by(topic, delivery_report.partition, delivery_report.offset)
        break
      rescue Rdkafka::RdkafkaError => e
        if %i[unknown_topic_or_part not_leader_for_partition not_coordinator].include?(e.code)
          sleep(0.5)
          next
        else
          raise
        end
      end
    end
  end

  timeout ||= Time.now.to_i + timeout_in_seconds

  loop do
    if timeout <= Time.now.to_i
      raise "Timeout of #{timeout_in_seconds} seconds reached in wait_for_message"
    end

    begin
      message = fetch_consumer.poll(1000)
      if message &&
          message.partition == delivery_report.partition &&
          message.offset == delivery_report.offset
        return message
      end
    rescue Rdkafka::RdkafkaError => e
      if %i[unknown_topic_or_part not_coordinator].include?(e.code)
        sleep(0.5)
        next
      else
        raise
      end
    end
  end
ensure
  fetch_consumer&.close unless consumer
end

def wait_for_assignment(consumer)
  30.times do
    return if !consumer.assignment.empty?
    sleep 1
  end
end

def wait_for_unassignment(consumer)
  10.times do
    break if consumer.assignment.empty?
    sleep 1
  end
end

def wait_for_topic(admin, topic)
  admin.metadata(topic)
rescue Rdkafka::RdkafkaError => e
  raise unless e.code == :unknown_topic_or_part

  sleep(0.5)

  retry
end

def notify_listener(consumer, listener, topic:, &block)
  # 1. subscribe and poll
  consumer.subscribe(topic)
  wait_for_assignment(consumer)
  consumer.poll(100)

  block&.call

  # 2. unsubscribe
  consumer.unsubscribe
  wait_for_unassignment(consumer)
  consumer.close
end

# Creates a unique topic for test isolation and waits for it to be ready.
# Returns the topic name.
def create_topic_for_test(partitions: 3)
  topic_name = "it-#{SecureRandom.uuid}"
  admin = rdkafka_config.admin
  handle = admin.create_topic(topic_name, partitions, 1)
  handle.wait(max_wait_timeout_ms: 15_000)
  wait_for_topic(admin, topic_name)
  admin.close
  topic_name
end

# Suite-level topic creation for topics that are truly shared across tests
TOPICS_CREATED_MUTEX = Mutex.new
$topics_initialized = false

def ensure_topics_created
  return if $topics_initialized

  TOPICS_CREATED_MUTEX.synchronize do
    return if $topics_initialized

    admin = rdkafka_config.admin
    topics = {
      TestTopics.example_topic => 1,
      TestTopics.consume_test_topic => 3,
      TestTopics.produce_test_topic => 3,
      TestTopics.partitioner_test_topic => 25
    }
    topics.each do |topic, partitions|
      create_topic_handle = admin.create_topic(topic, partitions, 1)
      begin
        create_topic_handle.wait(max_wait_timeout_ms: 1_000)
      rescue Rdkafka::RdkafkaError => ex
        raise unless ex.message.match?(/topic_already_exists/)
      end
    end
    topics.each_key { |topic| wait_for_topic(admin, topic) }
    admin.close
    $topics_initialized = true
  end
end
