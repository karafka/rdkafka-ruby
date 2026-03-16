# frozen_string_literal: true

# Config builder methods for Kafka test setup.
# Can be included as instance methods or called as module methods.
module KafkaConfigHelpers
  extend self

  def rdkafka_base_config
    if ENV["KAFKA_SSL_ENABLED"] == "true"
      {
        "bootstrap.servers": "localhost:9093",
        # Display statistics and refresh often just to cover those in specs
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
        # Display statistics and refresh often just to cover those in specs
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
end
