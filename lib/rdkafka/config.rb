require "logger"

module Rdkafka
  # Configuration for a Kafka consumer or producer. You can create an instance and use
  # the consumer and producer methods to create a client. Documentation of the available
  # configuration options is available on https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md.
  class Config
    # @private
    @@logger = Logger.new(STDOUT)
    # @private
    @@statistics_callback = nil
    # @private
    @@error_callback = nil
    # @private
    @@opaques = {}
    # @private
    @@log_queue = Queue.new

    Thread.start do
      loop do
        severity, msg = @@log_queue.pop
        @@logger.add(severity, msg)
      end
    end

    # Returns the current logger, by default this is a logger to stdout.
    #
    # @return [Logger]
    def self.logger
      @@logger
    end


    # Returns a queue whose contents will be passed to the configured logger. Each entry
    # should follow the format [Logger::Severity, String]. The benefit over calling the
    # logger directly is that this is safe to use from trap contexts.
    #
    # @return [Queue]
    def self.log_queue
      @@log_queue
    end

    # Set the logger that will be used for all logging output by this library.
    #
    # @param logger [Logger] The logger to be used
    #
    # @return [nil]
    def self.logger=(logger)
      raise NoLoggerError if logger.nil?
      @@logger=logger
    end

    # Set a callback that will be called every time the underlying client emits statistics.
    # You can configure if and how often this happens using `statistics.interval.ms`.
    # The callback is called with a hash that's documented here: https://github.com/edenhill/librdkafka/blob/master/STATISTICS.md
    #
    # @param callback [Proc, #call] The callback
    #
    # @return [nil]
    def self.statistics_callback=(callback)
      raise TypeError.new("Callback has to be callable") unless callback.respond_to?(:call)
      @@statistics_callback = callback
    end

    # Returns the current statistics callback, by default this is nil.
    #
    # @return [Proc, nil]
    def self.statistics_callback
      @@statistics_callback
    end

    # Set a callback that will be called every time the underlying client emits an error.
    # If this callback is not set, global errors such as brokers becoming unavailable will only be sent to the logger, as defined by librdkafka.
    # The callback is called with an instance of RdKafka::Error.
    #
    # @param callback [Proc, #call] The callback
    #
    # @return [nil]
    def self.error_callback=(callback)
      raise TypeError.new("Callback has to be callable") unless callback.respond_to?(:call)
      @@error_callback = callback
    end

    # Returns the current error callback, by default this is nil.
    #
    # @return [Proc, nil]
    def self.error_callback
      @@error_callback
    end

    # @private
    def self.opaques
      @@opaques
    end

    # Default config that can be overwritten.
    DEFAULT_CONFIG = {
      # Request api version so advanced features work
      :"api.version.request" => true
    }.freeze

    # Required config that cannot be overwritten.
    REQUIRED_CONFIG = {
      # Enable log queues so we get callbacks in our own Ruby threads
      :"log.queue" => true
    }.freeze

    # Returns a new config with the provided options which are merged with {DEFAULT_CONFIG}.
    #
    # @param config_hash [Hash{String,Symbol => String}] The config options for rdkafka
    #
    # @return [Config]
    def initialize(config_hash = {})
      @config_hash = DEFAULT_CONFIG.merge(config_hash)
      @consumer_rebalance_listener = nil
    end

    # Set a config option.
    #
    # @param key [String] The config option's key
    # @param value [String] The config option's value
    #
    # @return [nil]
    def []=(key, value)
      @config_hash[key] = value
    end

    # Get a config option with the specified key
    #
    # @param key [String] The config option's key
    #
    # @return [String, nil] The config option or `nil` if it is not present
    def [](key)
      @config_hash[key]
    end

    # Get notifications on partition assignment/revocation for the subscribed topics
    #
    # @param listener [Object, #on_partitions_assigned, #on_partitions_revoked] listener instance
    def consumer_rebalance_listener=(listener)
      @consumer_rebalance_listener = listener
    end

    # Create a consumer with this configuration.
    #
    # @raise [ConfigError] When the configuration contains invalid options
    # @raise [ClientCreationError] When the native client cannot be created
    #
    # @return [Consumer] The created consumer
    def consumer
      opaque = Opaque.new
      config = native_config(opaque)

      if @consumer_rebalance_listener
        opaque.consumer_rebalance_listener = @consumer_rebalance_listener
        Rdkafka::Bindings.rd_kafka_conf_set_rebalance_cb(config, Rdkafka::Bindings::RebalanceCallback)
      end

      kafka = native_kafka(config, :rd_kafka_consumer)

      # Redirect the main queue to the consumer
      Rdkafka::Bindings.rd_kafka_poll_set_consumer(kafka)

      # Return consumer with Kafka client
      Rdkafka::Consumer.new(kafka)
    end

    # Create a producer with this configuration.
    #
    # @raise [ConfigError] When the configuration contains invalid options
    # @raise [ClientCreationError] When the native client cannot be created
    #
    # @return [Producer] The created producer
    def producer
      # Create opaque
      opaque = Opaque.new
      # Create Kafka config
      config = native_config(opaque)
      # Set callback to receive delivery reports on config
      Rdkafka::Bindings.rd_kafka_conf_set_dr_msg_cb(config, Rdkafka::Callbacks::DeliveryCallbackFunction)
      # Return producer with Kafka client
      Rdkafka::Producer.new(native_kafka(config, :rd_kafka_producer)).tap do |producer|
        opaque.producer = producer
      end
    end

    # Create an admin instance with this configuration.
    #
    # @raise [ConfigError] When the configuration contains invalid options
    # @raise [ClientCreationError] When the native client cannot be created
    #
    # @return [Admin] The created admin instance
    def admin
      opaque = Opaque.new
      config = native_config(opaque)
      Rdkafka::Bindings.rd_kafka_conf_set_background_event_cb(config, Rdkafka::Callbacks::BackgroundEventCallbackFunction)
      Rdkafka::Admin.new(native_kafka(config, :rd_kafka_producer))
    end

    # Error that is returned by the underlying rdkafka error if an invalid configuration option is present.
    class ConfigError < RuntimeError; end

    # Error that is returned by the underlying rdkafka library if the client cannot be created.
    class ClientCreationError < RuntimeError; end

    # Error that is raised when trying to set a nil logger
    class NoLoggerError < RuntimeError; end

    private

    # This method is only intended to be used to create a client,
    # using it in another way will leak memory.
    def native_config(opaque=nil)
      Rdkafka::Bindings.rd_kafka_conf_new.tap do |config|
        # Create config
        @config_hash.merge(REQUIRED_CONFIG).each do |key, value|
          error_buffer = FFI::MemoryPointer.from_string(" " * 256)
          result = Rdkafka::Bindings.rd_kafka_conf_set(
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

        # Set opaque pointer that's used as a proxy for callbacks
        if opaque
          pointer = ::FFI::Pointer.new(:pointer, opaque.object_id)
          Rdkafka::Bindings.rd_kafka_conf_set_opaque(config, pointer)

          # Store opaque with the pointer as key. We use this approach instead
          # of trying to convert the pointer to a Ruby object because there is
          # no risk of a segfault this way.
          Rdkafka::Config.opaques[pointer.to_i] = opaque
        end

        # Set log callback
        Rdkafka::Bindings.rd_kafka_conf_set_log_cb(config, Rdkafka::Bindings::LogCallback)

        # Set stats callback
        Rdkafka::Bindings.rd_kafka_conf_set_stats_cb(config, Rdkafka::Bindings::StatsCallback)

        # Set error callback
        Rdkafka::Bindings.rd_kafka_conf_set_error_cb(config, Rdkafka::Bindings::ErrorCallback)
      end
    end

    def native_kafka(config, type)
      error_buffer = FFI::MemoryPointer.from_string(" " * 256)
      handle = Rdkafka::Bindings.rd_kafka_new(
        type,
        config,
        error_buffer,
        256
      )

      if handle.null?
        raise ClientCreationError.new(error_buffer.read_string)
      end

      # Redirect log to handle's queue
      Rdkafka::Bindings.rd_kafka_set_log_queue(
        handle,
        Rdkafka::Bindings.rd_kafka_queue_get_main(handle)
      )

      # Return handle which should be closed using rd_kafka_destroy after usage.
      handle
    end
  end

  # @private
  class Opaque
    attr_accessor :producer
    attr_accessor :consumer_rebalance_listener

    def call_delivery_callback(delivery_handle)
      producer.call_delivery_callback(delivery_handle) if producer
    end

    def call_on_partitions_assigned(consumer, list)
      return unless consumer_rebalance_listener
      return unless consumer_rebalance_listener.respond_to?(:on_partitions_assigned)

      consumer_rebalance_listener.on_partitions_assigned(consumer, list)
    end

    def call_on_partitions_revoked(consumer, list)
      return unless consumer_rebalance_listener
      return unless consumer_rebalance_listener.respond_to?(:on_partitions_revoked)

      consumer_rebalance_listener.on_partitions_revoked(consumer, list)
    end
  end
end
