module Rdkafka
  class Config
    DEFAULT_CONFIG = {
      "api.version.request" => "true"
    }

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
      Rdkafka::Consumer.new(native_kafka(native_config, :rd_kafka_consumer))
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
      config = Rdkafka::FFI.rd_kafka_conf_new

      @config_hash.each do |key, value|
        error_buffer = ::FFI::MemoryPointer.from_string(" " * 256)
        result = Rdkafka::FFI.rd_kafka_conf_set(
          config,
          key,
          value,
          error_buffer,
          256
        )
        unless result == :config_ok
          raise ConfigError.new(error_buffer.read_string)
        end
      end

      config
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

      ::FFI::AutoPointer.new(
        handle,
        Rdkafka::FFI.method(:rd_kafka_destroy)
      )
    end
  end
end
