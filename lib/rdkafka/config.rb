require "logger"

module Rdkafka
  class Config
    @@logger = Logger.new(STDOUT)

    def self.logger
      @@logger
    end

    def self.logger=(logger)
      @@logger=logger
    end

    DEFAULT_CONFIG = {
      # Request api version so advanced features work
      :"api.version.request" => true
    }.freeze

    REQUIRED_CONFIG = {
      # Enable log queues so we get callbacks in our own threads
      :"log.queue" => true
    }.freeze

    def initialize(config_hash = {})
      @config_hash = DEFAULT_CONFIG.merge(config_hash)
    end

    def []=(key, value)
      @config_hash[key] = value
    end

    def [](key)
      @config_hash[key]
    end

    def consumer
      kafka = native_kafka(native_config, :rd_kafka_consumer)
      # Redirect the main queue to the consumer
      Rdkafka::FFI.rd_kafka_poll_set_consumer(kafka)
      # Return consumer with Kafka client
      Rdkafka::Consumer.new(kafka)
    end

    def producer
      # Create Kafka config
      config = native_config
      # Set callback to receive delivery reports on config
      Rdkafka::FFI.rd_kafka_conf_set_dr_msg_cb(config, Rdkafka::FFI::DeliveryCallback)
      # Return producer with Kafka client
      Rdkafka::Producer.new(native_kafka(config, :rd_kafka_producer))
    end

    class ConfigError < RuntimeError; end
    class ClientCreationError < RuntimeError; end

    private

    # This method is only intented to be used to create a client,
    # using it in another way will leak memory.
    def native_config
      Rdkafka::FFI.rd_kafka_conf_new.tap do |config|
        @config_hash.merge(REQUIRED_CONFIG).each do |key, value|
          error_buffer = ::FFI::MemoryPointer.from_string(" " * 256)
          result = Rdkafka::FFI.rd_kafka_conf_set(
            config,
            key.to_s,
            value.to_s,
            error_buffer,
            256
          )
          unless result == :config_ok
            raise ConfigError.new(error_buffer.read_string)
          end
        end
        # Set log callback
        Rdkafka::FFI.rd_kafka_conf_set_log_cb(config, Rdkafka::FFI::LogCallback)
      end
    end

    def native_kafka(config, type)
      error_buffer = ::FFI::MemoryPointer.from_string(" " * 256)
      handle = Rdkafka::FFI.rd_kafka_new(
        type,
        config,
        error_buffer,
        256
      )

      if handle.nil?
        raise ClientCreationError.new(error_buffer.read_string)
      end

      # Redirect log to handle's queue
      Rdkafka::FFI.rd_kafka_set_log_queue(
        handle,
        Rdkafka::FFI.rd_kafka_queue_get_main(handle)
      )

      ::FFI::AutoPointer.new(
        handle,
        Rdkafka::FFI.method(:rd_kafka_destroy)
      )
    end
  end
end
