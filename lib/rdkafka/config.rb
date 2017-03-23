module Rdkafka
  class Config
    def initialize(config_hash = {})
      @config_hash = config_hash
    end

    def []=(key, value)
      @config_hash[key] = value
    end

    def [](key)
      @config_hash[key]
    end

    def consumer
      native_config
    end

    def producer
      native_config
    end

    class ConfigError < RuntimeError; end

    private

    def native_config
      config = ::FFI::AutoPointer.new(
        Rdkafka::FFI.rd_kafka_conf_new,
        Rdkafka::FFI.method(:rd_kafka_conf_destroy)
      )

      @config_hash.each do |key, value|
        error_buffer = ::FFI::MemoryPointer.from_string(" " * 100)
        result = Rdkafka::FFI.rd_kafka_conf_set(
          config,
          key,
          value,
          error_buffer,
          100
        )
        unless result == :config_ok
          error_string = error_buffer.read_string
          raise ConfigError.new(error_string)
        end
      end

      config
    end
  end
end
