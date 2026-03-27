# frozen_string_literal: true

# Config builder methods for Kafka test setup.
# Can be included as instance methods or called as module methods.
module KafkaConfigHelpers
  extend self

  # Returns the base rdkafka configuration hash, selecting SSL or plaintext
  # depending on the KAFKA_SSL_ENABLED environment variable.
  #
  # @return [Hash] base configuration hash for rdkafka
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

  # Builds an Rdkafka::Config from the base config with optional overrides.
  #
  # @param config_overrides [Hash] configuration keys to merge on top of base config
  # @return [Rdkafka::Config] configured rdkafka config instance
  def rdkafka_config(config_overrides = {})
    # Generate the base config
    config = rdkafka_base_config
    # Merge overrides
    config.merge!(config_overrides)
    # Return it
    Rdkafka::Config.new(config)
  end

  # Builds a consumer-specific Rdkafka::Config with auto.offset.reset, partition.eof,
  # and a unique group.id already set.
  #
  # @param config_overrides [Hash] configuration keys to merge on top of consumer config
  # @return [Rdkafka::Config] configured rdkafka consumer config instance
  def rdkafka_consumer_config(config_overrides = {})
    # Generate the base config
    config = rdkafka_base_config
    # Add consumer specific fields to it
    config[:"auto.offset.reset"] = "earliest"
    config[:"enable.partition.eof"] = false
    config[:"group.id"] = "ruby-test-#{TestTopics.spec_hash}-#{SecureRandom.uuid}"
    # Enable debug mode if required
    if ENV["DEBUG_CONSUMER"]
      config[:debug] = "cgrp,topic,fetch"
    end
    # Merge overrides
    config.merge!(config_overrides)
    # Return it
    Rdkafka::Config.new(config)
  end

  # Builds a producer-specific Rdkafka::Config from the base config.
  #
  # @param config_overrides [Hash] configuration keys to merge on top of producer config
  # @return [Rdkafka::Config] configured rdkafka producer config instance
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
