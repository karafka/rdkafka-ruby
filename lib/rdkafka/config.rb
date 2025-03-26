# frozen_string_literal: true

module Rdkafka
  # Configuration for a Kafka consumer or producer. You can create an instance and use
  # the consumer and producer methods to create a client. Documentation of the available
  # configuration options is available on https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md.
  class Config
    # @private
    @@logger = Logger.new(STDOUT)
    # @private
    @@statistics_callback = nil
    # @private
    @@error_callback = nil
    # @private
    @@opaques = ObjectSpace::WeakMap.new
    # @private
    @@log_queue = Queue.new
    # We memoize thread on the first log flush
    # This allows us also to restart logger thread on forks
    @@log_thread = nil
    # @private
    @@log_mutex = Mutex.new
    # @private
    @@oauthbearer_token_refresh_callback = nil

    # Returns the current logger, by default this is a logger to stdout.
    #
    # @return [Logger]
    def self.logger
      @@logger
    end

    # Makes sure that there is a thread for consuming logs
    # We do not spawn thread immediately and we need to check if it operates to support forking
    def self.ensure_log_thread
      return if @@log_thread && @@log_thread.alive?

      @@log_mutex.synchronize do
        # Restart if dead (fork, crash)
        @@log_thread = nil if @@log_thread && !@@log_thread.alive?

        @@log_thread ||= Thread.start do
          loop do
            severity, msg = @@log_queue.pop
            @@logger.add(severity, msg)
          end
        end
      end
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
      @@logger = logger
    end

    # Set a callback that will be called every time the underlying client emits statistics.
    # You can configure if and how often this happens using `statistics.interval.ms`.
    # The callback is called with a hash that's documented here: https://github.com/confluentinc/librdkafka/blob/master/STATISTICS.md
    #
    # @param callback [Proc, #call] The callback
    #
    # @return [nil]
    def self.statistics_callback=(callback)
      raise TypeError.new("Callback has to be callable") unless callback.respond_to?(:call) || callback == nil
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

    # Sets the SASL/OAUTHBEARER token refresh callback.
    # This callback will be triggered when it is time to refresh the client's OAUTHBEARER token
    #
    # @param callback [Proc, #call] The callback
    #
    # @return [nil]
    def self.oauthbearer_token_refresh_callback=(callback)
      raise TypeError.new("Callback has to be callable") unless callback.respond_to?(:call) || callback == nil
      @@oauthbearer_token_refresh_callback = callback
    end

    # Returns the current oauthbearer_token_refresh_callback callback, by default this is nil.
    #
    # @return [Proc, nil]
    def self.oauthbearer_token_refresh_callback
      @@oauthbearer_token_refresh_callback
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
      Callbacks.ensure_ffi_running

      @config_hash = DEFAULT_CONFIG.merge(config_hash)
      @consumer_rebalance_listener = nil
      @consumer_poll_set = true
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

    # Should we use a single queue for the underlying consumer and events.
    #
    # This is an advanced API that allows for more granular control of the polling process.
    # When this value is set to `false` (`true` by defualt), there will be two queues that need to
    # be polled:
    #   - main librdkafka queue for events
    #   - consumer queue with messages and rebalances
    #
    # It is recommended to use the defaults and only set it to `false` in advance multi-threaded
    # and complex cases where granular events handling control is needed.
    #
    # @param poll_set [Boolean]
    def consumer_poll_set=(poll_set)
      @consumer_poll_set = poll_set
    end

    # Creates a consumer with this configuration.
    #
    # @param native_kafka_auto_start [Boolean] should the native kafka operations be started
    #   automatically. Defaults to true. Set to false only when doing complex initialization.
    # @return [Consumer] The created consumer
    #
    # @raise [ConfigError] When the configuration contains invalid options
    # @raise [ClientCreationError] When the native client cannot be created
    def consumer(native_kafka_auto_start: true)
      opaque = Opaque.new
      config = native_config(opaque)

      if @consumer_rebalance_listener
        opaque.consumer_rebalance_listener = @consumer_rebalance_listener
        Rdkafka::Bindings.rd_kafka_conf_set_rebalance_cb(config, Rdkafka::Bindings::RebalanceCallback)
      end

      # Create native client
      kafka = native_kafka(config, :rd_kafka_consumer)

      # Redirect the main queue to the consumer queue
      Rdkafka::Bindings.rd_kafka_poll_set_consumer(kafka) if @consumer_poll_set

      # Return consumer with Kafka client
      Rdkafka::Consumer.new(
        Rdkafka::NativeKafka.new(
          kafka,
          run_polling_thread: false,
          opaque: opaque,
          auto_start: native_kafka_auto_start
        )
      )
    end

    # Create a producer with this configuration.
    #
    # @param native_kafka_auto_start [Boolean] should the native kafka operations be started
    #   automatically. Defaults to true. Set to false only when doing complex initialization.
    # @param native_kafka_poll_timeout_ms [Integer] ms poll time of the native Kafka
    # @return [Producer] The created producer
    #
    # @raise [ConfigError] When the configuration contains invalid options
    # @raise [ClientCreationError] When the native client cannot be created
    def producer(native_kafka_auto_start: true, native_kafka_poll_timeout_ms: 100)
      # Create opaque
      opaque = Opaque.new
      # Create Kafka config
      config = native_config(opaque)
      # Set callback to receive delivery reports on config
      Rdkafka::Bindings.rd_kafka_conf_set_dr_msg_cb(config, Rdkafka::Callbacks::DeliveryCallbackFunction)
      # Return producer with Kafka client
      partitioner_name = self[:partitioner] || self["partitioner"]

      kafka = native_kafka(config, :rd_kafka_producer)

      Rdkafka::Producer.new(
        Rdkafka::NativeKafka.new(
          kafka,
          run_polling_thread: true,
          opaque: opaque,
          auto_start: native_kafka_auto_start,
          timeout_ms: native_kafka_poll_timeout_ms
        ),
        partitioner_name
      ).tap do |producer|
        opaque.producer = producer
      end
    end

    # Creates an admin instance with this configuration.
    #
    # @param native_kafka_auto_start [Boolean] should the native kafka operations be started
    #   automatically. Defaults to true. Set to false only when doing complex initialization.
    # @param native_kafka_poll_timeout_ms [Integer] ms poll time of the native Kafka
    # @return [Admin] The created admin instance
    #
    # @raise [ConfigError] When the configuration contains invalid options
    # @raise [ClientCreationError] When the native client cannot be created
    def admin(native_kafka_auto_start: true, native_kafka_poll_timeout_ms: 100)
      opaque = Opaque.new
      config = native_config(opaque)
      Rdkafka::Bindings.rd_kafka_conf_set_background_event_cb(config, Rdkafka::Callbacks::BackgroundEventCallbackFunction)

      kafka = native_kafka(config, :rd_kafka_producer)

      Rdkafka::Admin.new(
        Rdkafka::NativeKafka.new(
          kafka,
          run_polling_thread: true,
          opaque: opaque,
          auto_start: native_kafka_auto_start,
          timeout_ms: native_kafka_poll_timeout_ms
        )
      )
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
    def native_config(opaque = nil)
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

        # Set oauth callback
        if Rdkafka::Config.oauthbearer_token_refresh_callback
          Rdkafka::Bindings.rd_kafka_conf_set_oauthbearer_token_refresh_cb(config, Rdkafka::Bindings::OAuthbearerTokenRefreshCallback)
        end
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

    def call_delivery_callback(delivery_report, delivery_handle)
      producer.call_delivery_callback(delivery_report, delivery_handle) if producer
    end

    def call_on_partitions_assigned(list)
      return unless consumer_rebalance_listener
      return unless consumer_rebalance_listener.respond_to?(:on_partitions_assigned)

      consumer_rebalance_listener.on_partitions_assigned(list)
    end

    def call_on_partitions_revoked(list)
      return unless consumer_rebalance_listener
      return unless consumer_rebalance_listener.respond_to?(:on_partitions_revoked)

      consumer_rebalance_listener.on_partitions_revoked(list)
    end
  end
end
