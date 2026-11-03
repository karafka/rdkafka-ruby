# frozen_string_literal: true

module Rdkafka
  # Configuration for a Kafka consumer or producer. You can create an instance and use
  # the consumer and producer methods to create a client. Documentation of the available
  # configuration options is available on
  # https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md.
  class Config
    # @!visibility private
    @@logger = Logger.new($stdout)
    # @!visibility private
    @@statistics_callback = nil
    # @!visibility private
    @@error_callback = nil
    # @!visibility private
    @@opaques = ObjectSpace::WeakMap.new
    # @!visibility private
    @@log_queue = Queue.new
    # We memoize thread on first log flush. This allows us also to restart logger thread on forks
    @@log_thread = nil
    # @!visibility private
    @@log_mutex = Mutex.new
    # @!visibility private
    @@oauthbearer_token_refresh_callback = nil
    # @!visibility private
    # Whether the partition key length passed to the partitioner is the byte length (`bytesize`)
    # rather than the character length (`size`). `false` (character length) is the legacy default
    # to avoid changing partition assignment for existing multibyte/binary keys; set to `true` to
    # opt into the correct, other-client-consistent byte-length hashing.
    @@partitioner_key_uses_bytesize = false

    # Returns the current logger, by default this is a logger to stdout.
    #
    # @return [Logger]
    def self.logger
      @@logger
    end

    # Makes sure that there is a thread for consuming logs
    # We do not spawn thread immediately and we need to check if it operates to support forking
    def self.ensure_log_thread
      return if @@log_thread&.alive?

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
    # The callback is called with a hash that's documented here:
    # https://github.com/confluentinc/librdkafka/blob/master/STATISTICS.md
    #
    # @param callback [Proc, #call, nil] callable object or nil to clear
    # @return [nil]
    def self.statistics_callback=(callback)
      raise TypeError.new("Callback has to be callable") unless callback.respond_to?(:call) || callback.nil?
      @@statistics_callback = callback
    end

    # Returns the current statistics callback, by default this is nil.
    #
    # @return [Proc, nil]
    def self.statistics_callback
      @@statistics_callback
    end

    # Set a callback that will be called every time the underlying client emits an error.
    # If this callback is not set, global errors such as brokers becoming unavailable will only be
    # sent to the logger, as defined by librdkafka.
    # The callback is called with an instance of RdKafka::Error.
    #
    # @param callback [Proc, #call, nil] callable object to handle errors or nil to clear
    # @return [nil]
    def self.error_callback=(callback)
      raise TypeError.new("Callback has to be callable") unless callback.respond_to?(:call) || callback.nil?
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
    # @param callback [Proc, #call, nil] callable object to handle token refresh or nil to clear
    # @return [nil]
    def self.oauthbearer_token_refresh_callback=(callback)
      raise TypeError.new("Callback has to be callable") unless callback.respond_to?(:call) || callback.nil?
      @@oauthbearer_token_refresh_callback = callback
    end

    # Returns the current oauthbearer_token_refresh_callback callback, by default this is nil.
    #
    # @return [Proc, nil]
    def self.oauthbearer_token_refresh_callback
      @@oauthbearer_token_refresh_callback
    end

    # Whether the partitioner hashes the partition key by its byte length instead of its character
    # length. Defaults to `false` (character length) to preserve existing partition assignment for
    # multibyte/binary keys. Passing the byte length is the correct, other-client-consistent
    # behavior; enable it (and be aware it can move such keys to a different partition) with
    # `Rdkafka::Config.partitioner_key_uses_bytesize = true`. This default is expected to change in
    # a future major version.
    #
    # @return [Boolean]
    def self.partitioner_key_uses_bytesize
      @@partitioner_key_uses_bytesize
    end

    # Sets whether the partitioner hashes the partition key by its byte length rather than its
    # character length. The key length method is resolved here once rather than on every produced
    # message, and each call re-resolves it, so changing the setting at runtime takes effect
    # immediately.
    #
    # @param value [Boolean] true to hash by byte length, false (default) for character length
    # @return [void]
    def self.partitioner_key_uses_bytesize=(value)
      @@partitioner_key_uses_bytesize = value

      Rdkafka::Bindings.singleton_class.alias_method(
        :partition_key_length,
        value ? :partition_key_bytesize : :partition_key_size
      )
    end

    # Returns the time-to-live of the process-wide partitions count cache used by
    # `produce(partition_key:)`.
    #
    # @return [Integer] time-to-live in milliseconds
    def self.partition_count_cache_ttl_ms
      Rdkafka::Producer.partitions_count_cache.ttl_ms
    end

    # Sets the time-to-live of the process-wide partitions count cache shared by all the producers.
    # Cached counts are kept, so it can be changed at any time.
    #
    # @param value [Integer] time-to-live in milliseconds
    #   (defaults to {Defaults::PARTITIONS_COUNT_CACHE_TTL_MS})
    # @return [void]
    # @raise [ArgumentError] when the value is not a positive Integer
    def self.partition_count_cache_ttl_ms=(value)
      Rdkafka::Producer.partitions_count_cache.ttl_ms = value
    end

    # @private
    def self.opaques
      @@opaques
    end

    # Default config that can be overwritten.
    DEFAULT_CONFIG = {}.freeze

    # Required config that cannot be overwritten.
    REQUIRED_CONFIG = {
      # Enable log queues so we get callbacks in our own Ruby threads
      "log.queue": true
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
      @consumer_offset_commit_listener = nil
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
    # @return [Object, #on_partitions_assigned, #on_partitions_revoked] listener instance
    attr_writer :consumer_rebalance_listener

    # Get the result of every offset commit, including async commits that the broker rejects
    #
    # `#on_offsets_committed` gets an {RdkafkaError} (nil on success) and the
    # {Consumer::TopicPartitionList} of the commit, whose partitions carry their own errors. It
    # runs inside {Consumer#poll} (or {Consumer#events_poll} when `consumer_poll_set` is false)
    # and inside {Consumer#close}, on the thread that calls them.
    #
    # `#on_offsets_commit_failed` gets the same arguments, but only for a failed commit: not on
    # success and not on `no_offset` (nothing new to commit). A listener with only this method
    # costs nothing for the other commits.
    #
    # @return [Object, #on_offsets_committed, #on_offsets_commit_failed] listener instance
    attr_writer :consumer_offset_commit_listener

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
    # @return [Boolean]
    attr_writer :consumer_poll_set

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

      # Set callback to receive background events, so admin-style operations issued on the
      # consumer handle (e.g. Consumer#list_offsets) get their results dispatched back to their
      # handles. librdkafka spawns its internally-managed background thread because of this.
      Rdkafka::Bindings.rd_kafka_conf_set_background_event_cb(config, Rdkafka::Callbacks::BackgroundEventCallbackFunction)

      if @consumer_rebalance_listener
        opaque.consumer_rebalance_listener = @consumer_rebalance_listener
        Rdkafka::Bindings.rd_kafka_conf_set_rebalance_cb(config, Rdkafka::Bindings::RebalanceCallback)
      end

      if @consumer_offset_commit_listener
        opaque.consumer_offset_commit_listener = @consumer_offset_commit_listener
        Rdkafka::Bindings.rd_kafka_conf_set_offset_commit_cb(config, Rdkafka::Bindings::OffsetCommitCallback)
      end

      build_native_client(
        config,
        :rd_kafka_consumer,
        opaque: opaque,
        run_polling_thread: false,
        auto_start: native_kafka_auto_start
      ) do |kafka, native_kafka|
        Rdkafka::Bindings.rd_kafka_poll_set_consumer(kafka) if @consumer_poll_set
        Rdkafka::Consumer.new(native_kafka)
      end
    end

    # Creates a KIP-932 share group consumer with this configuration.
    #
    # `:"group.id"` is required. The acknowledgement mode is selected with the optional
    # `:"share.acknowledgement.mode"` property (`implicit` by default, or `explicit`). librdkafka
    # rejects a number of regular-consumer properties for share consumers (for example
    # `enable.auto.commit`, `auto.offset.reset` or `partition.assignment.strategy`) with a
    # {ClientCreationError}.
    #
    # @return [ShareConsumer] The created share consumer
    #
    # @raise [ConfigError] When the configuration contains invalid options or a rebalance
    #   listener is set (share group assignment is broker-driven and has no rebalances)
    # @raise [ClientCreationError] When the native client cannot be created
    #
    # @note The share consumer is a preview librdkafka feature and requires a broker with share
    #   groups enabled (Apache Kafka 4.2.0+).
    def share_consumer
      if @consumer_rebalance_listener
        raise ConfigError.new("`consumer_rebalance_listener` is not supported for share consumers")
      end

      opaque = Opaque.new
      config = native_config(opaque)

      handle = create_native_handle(config) do |error_buffer|
        Rdkafka::Bindings.rd_kafka_share_consumer_new(config, error_buffer, 256)
      end

      share_consumer = nil

      # Share consumers cannot go through `build_native_client`: they own the native handle
      # directly rather than a `NativeKafka` wrapper (single-threaded by design, no polling
      # thread). They still need both of the things that factory provides - registration, so the
      # `at_exit` hook closes them before Ruby's shutdown finalization rather than letting
      # librdkafka be `dlclose`d with its threads still running, and a teardown that destroys the
      # handle when construction fails part-way - so both are done here.
      begin
        # Forward the log queue (enabled via REQUIRED_CONFIG) to the main queue, which
        # rd_kafka_share_poll drains. There is no background polling thread for share consumers:
        # log, statistics and error callbacks are all serviced from within ShareConsumer#poll.
        log_queue_error = Rdkafka::Bindings.rd_kafka_share_set_log_queue(handle, FFI::Pointer::NULL)
        Rdkafka::Bindings.rd_kafka_error_destroy(log_queue_error) unless log_queue_error.null?

        share_consumer = Rdkafka::ShareConsumer.new(handle, opaque: opaque)
        opaque.share_consumer = share_consumer

        Rdkafka::Clients.register(share_consumer)
      rescue Exception
        begin
          if share_consumer
            share_consumer.close
          else
            destroy_error = Rdkafka::Bindings.rd_kafka_share_destroy(handle)
            Rdkafka::Bindings.rd_kafka_error_destroy(destroy_error) unless destroy_error.null?
          end
        rescue Exception => err
          # Never let a teardown failure replace the error that caused it: that error is what the
          # caller needs to see, and it is re-raised below regardless.
          Rdkafka::Config.logger.error("Unhandled exception: #{err.class} - #{err.message}")
        end

        raise
      end

      share_consumer
    end

    # Create a producer with this configuration.
    #
    # @param native_kafka_auto_start [Boolean] should the native kafka operations be started
    #   automatically. Defaults to true. Set to false only when doing complex initialization.
    # @param native_kafka_poll_timeout_ms [Integer] ms poll time of the native Kafka
    # @param run_polling_thread [Boolean] should the background polling thread be started.
    #   Defaults to true. Set to false when using the FD API for fiber scheduler integration.
    # @return [Producer] The created producer
    #
    # @raise [ConfigError] When the configuration contains invalid options
    # @raise [ClientCreationError] When the native client cannot be created
    def producer(native_kafka_auto_start: true, native_kafka_poll_timeout_ms: Defaults::NATIVE_KAFKA_POLL_TIMEOUT_MS, run_polling_thread: true)
      # Create opaque
      opaque = Opaque.new
      # Create Kafka config
      config = native_config(opaque)
      # Set callback to receive delivery reports on config
      Rdkafka::Bindings.rd_kafka_conf_set_dr_msg_cb(config, Rdkafka::Callbacks::DeliveryCallbackFunction)
      # Return producer with Kafka client
      partitioner_name = self[:partitioner] || self["partitioner"]

      build_native_client(
        config,
        :rd_kafka_producer,
        opaque: opaque,
        run_polling_thread: run_polling_thread,
        auto_start: native_kafka_auto_start,
        timeout_ms: native_kafka_poll_timeout_ms
      ) do |_kafka, native_kafka|
        Rdkafka::Producer.new(native_kafka, partitioner_name).tap do |producer|
          opaque.producer = producer
        end
      end
    end

    # Creates an admin instance with this configuration.
    #
    # @param native_kafka_auto_start [Boolean] should the native kafka operations be started
    #   automatically. Defaults to true. Set to false only when doing complex initialization.
    # @param native_kafka_poll_timeout_ms [Integer] ms poll time of the native Kafka
    # @param run_polling_thread [Boolean] should the background polling thread be started.
    #   Defaults to true. Set to false when using the FD API for fiber scheduler integration.
    # @return [Admin] The created admin instance
    #
    # @raise [ConfigError] When the configuration contains invalid options
    # @raise [ClientCreationError] When the native client cannot be created
    def admin(native_kafka_auto_start: true, native_kafka_poll_timeout_ms: Defaults::NATIVE_KAFKA_POLL_TIMEOUT_MS, run_polling_thread: true)
      opaque = Opaque.new
      config = native_config(opaque)
      Rdkafka::Bindings.rd_kafka_conf_set_background_event_cb(config, Rdkafka::Callbacks::BackgroundEventCallbackFunction)

      build_native_client(
        config,
        :rd_kafka_producer,
        opaque: opaque,
        run_polling_thread: run_polling_thread,
        auto_start: native_kafka_auto_start,
        timeout_ms: native_kafka_poll_timeout_ms
      ) do |_kafka, native_kafka|
        Rdkafka::Admin.new(native_kafka)
      end
    end

    # Returns all configuration properties and their current values for this config.
    #
    # Uses `rd_kafka_conf_dump` to retrieve every property (including defaults and
    # internal properties like `client.software.name`) as a flat Hash.
    #
    # @return [Hash{Symbol => String}] property names mapped to their current values
    #
    # @raise [ConfigError] When the configuration contains invalid options
    # @note The librdkafka C API does not distinguish between producer-only, consumer-only,
    #   and global properties at the configuration level. All properties are returned
    #   regardless of the intended client type.
    #
    # @note The returned Hash may include sensitive values such as authentication
    #   credentials and key passwords. Do not log or serialize the returned data
    #   unless you have explicitly redacted secret entries.
    def describe_properties
      config = nil
      dump_ptr = nil
      count = 0

      config = native_config
      count_ptr = Rdkafka::Bindings::SizePtr.new
      dump_ptr = Rdkafka::Bindings.rd_kafka_conf_dump(config, count_ptr)

      count = count_ptr[:value]
      result = {}

      (0...count).step(2) do |i|
        key = dump_ptr.get_pointer(i * FFI::Pointer.size).read_string
        value = dump_ptr.get_pointer((i + 1) * FFI::Pointer.size).read_string
        result[key.to_sym] = value
      end

      result
    ensure
      Rdkafka::Bindings.rd_kafka_conf_dump_free(dump_ptr, count) if dump_ptr
      Rdkafka::Bindings.rd_kafka_conf_destroy(config) if config
    end

    # Error that is returned by the underlying rdkafka error if an invalid configuration option is
    # present.
    class ConfigError < RuntimeError; end

    # Error that is returned by the underlying rdkafka library if the client cannot be created.
    class ClientCreationError < RuntimeError; end

    # Error that is raised when trying to set a nil logger
    class NoLoggerError < RuntimeError; end

    private

    # Builds a public client around a native Kafka handle, destroying the current owner if setup
    # fails before ownership is transferred to the caller.
    # @param config [FFI::Pointer] native rdkafka configuration
    # @param type [Symbol] native client type
    # @param opaque [Object] callback context retained for the native client
    # @param run_polling_thread [Boolean] whether to run a Ruby polling thread
    # @param auto_start [Boolean] whether to start the polling thread after wrapping the client
    # @param timeout_ms [Integer] polling timeout in milliseconds
    # @yieldparam kafka [FFI::Pointer] native Kafka handle
    # @yieldparam native_kafka [NativeKafka] Ruby owner of the native handle
    # @return [Consumer, Producer, Admin] the constructed public client
    # @private
    def build_native_client(
      config,
      type,
      opaque:,
      run_polling_thread:,
      auto_start:,
      timeout_ms: Defaults::NATIVE_KAFKA_POLL_TIMEOUT_MS
    )
      kafka = native_kafka(config, type)
      native_kafka = nil
      client = nil

      begin
        native_kafka = Rdkafka::NativeKafka.new(
          kafka,
          run_polling_thread: run_polling_thread,
          opaque: opaque,
          auto_start: false,
          timeout_ms: timeout_ms
        )
        client = yield(kafka, native_kafka)
        native_kafka.start if auto_start
        Rdkafka::Clients.register(client)
      rescue Exception
        owner = client || native_kafka

        begin
          owner ? owner.close : Rdkafka::Bindings.rd_kafka_destroy(kafka)
        rescue Exception => err
          # Never let a teardown failure replace the error that caused it: that error is what the
          # caller needs to see, and it is re-raised below regardless.
          Rdkafka::Config.logger.error("Unhandled exception: #{err.class} - #{err.message}")
        end

        raise
      end
    end

    # This method is only intended to be used to create a client,
    # using it in another way will leak memory.
    #
    # @param opaque [Object, nil] optional opaque pointer for callbacks
    # @return [FFI::Pointer] native rdkafka configuration pointer
    def native_config(opaque = nil)
      config = Rdkafka::Bindings.rd_kafka_conf_new

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

      config
    rescue Exception
      # rd_kafka_conf_new allocates a conf the caller owns until it is handed to rd_kafka_new. If
      # we raise before returning it (e.g. an invalid option in the loop above), destroy it here
      # so the native config is not leaked.
      Rdkafka::Bindings.rd_kafka_conf_destroy(config) if config
      raise
    end

    # Creates a native client handle via the given creation block, enforcing the conf ownership
    # rule shared by all librdkafka constructors: on success librdkafka takes ownership of the
    # conf and frees it itself; on failure the application keeps ownership (see rdkafka.c), so
    # it must be destroyed here to avoid leaking the conf on every failed client creation.
    #
    # @param config [FFI::Pointer] pointer to the native config
    # @yield [error_buffer] performs the native creation call
    # @yieldparam error_buffer [FFI::MemoryPointer] 256-byte buffer for the error string
    # @return [FFI::Pointer] non-NULL native handle
    # @raise [ClientCreationError] When the native client cannot be created
    # @private
    def create_native_handle(config)
      error_buffer = FFI::MemoryPointer.new(:char, 256)
      handle = yield(error_buffer)

      if handle.null?
        Rdkafka::Bindings.rd_kafka_conf_destroy(config)
        raise ClientCreationError.new(error_buffer.read_string)
      end

      handle
    end

    # Creates a native Kafka handle
    # @param config [FFI::Pointer] pointer to the native config
    # @param type [Symbol] type of client (:rd_kafka_producer or :rd_kafka_consumer)
    # @return [FFI::Pointer] pointer to the native Kafka handle
    # @private
    def native_kafka(config, type)
      handle = create_native_handle(config) do |error_buffer|
        Rdkafka::Bindings.rd_kafka_new(
          type,
          config,
          error_buffer,
          256
        )
      end

      # Redirect log to handle's queue
      main_queue = Rdkafka::Bindings.rd_kafka_queue_get_main(handle)
      Rdkafka::Bindings.rd_kafka_set_log_queue(handle, main_queue)
      Rdkafka::Bindings.rd_kafka_queue_destroy(main_queue)

      # Return handle which should be closed using rd_kafka_destroy after usage.
      handle
    rescue Exception
      Rdkafka::Bindings.rd_kafka_destroy(handle) if handle && !handle.null?
      raise
    end
  end

  # @private
  class Opaque
    attr_accessor :producer
    attr_accessor :consumer_rebalance_listener
    attr_accessor :consumer_offset_commit_listener
    # Backreference set for share consumers so conf-level callbacks (which receive the opaque)
    # can hand instance-scoped data - currently the librdkafka client name - back to the
    # ShareConsumer, which has no native accessor for it
    attr_accessor :share_consumer

    # Forwards the librdkafka client name to clients that cannot query it themselves. Regular
    # clients resolve their name lazily via rd_kafka_name on their own handle, so only the
    # share consumer (whose handle exposes no name accessor) consumes this.
    # @param name [String] librdkafka client name (e.g. "rdkafka#consumer-1")
    def capture_client_name(name)
      share_consumer&.name = name
    end

    # Invokes the delivery callback on the producer if one is set
    # @param delivery_report [Rdkafka::Producer::DeliveryReport] the delivery report
    # @param delivery_handle [Rdkafka::Producer::DeliveryHandle] the delivery handle
    def call_delivery_callback(delivery_report, delivery_handle)
      producer&.call_delivery_callback(delivery_report, delivery_handle)
    end

    # Invokes the on_partitions_assigned callback on the rebalance listener if set
    # @param list [Rdkafka::Consumer::TopicPartitionList] the assigned partitions
    def call_on_partitions_assigned(list)
      return unless consumer_rebalance_listener
      return unless consumer_rebalance_listener.respond_to?(:on_partitions_assigned)

      consumer_rebalance_listener.on_partitions_assigned(list)
    end

    # Invokes the on_partitions_revoked callback on the rebalance listener if set
    # @param list [Rdkafka::Consumer::TopicPartitionList] the revoked partitions
    def call_on_partitions_revoked(list)
      return unless consumer_rebalance_listener
      return unless consumer_rebalance_listener.respond_to?(:on_partitions_revoked)

      consumer_rebalance_listener.on_partitions_revoked(list)
    end

    # Invokes the on_offsets_committed callback on the offset commit listener if set
    # @param error [Rdkafka::RdkafkaError, nil] the commit error or nil on success
    # @param list [Rdkafka::Consumer::TopicPartitionList] the offsets of the commit
    def call_on_offsets_committed(error, list)
      return unless consumer_offset_commit_listener
      return unless consumer_offset_commit_listener.respond_to?(:on_offsets_committed)

      consumer_offset_commit_listener.on_offsets_committed(error, list)
    end

    # Invokes the on_offsets_commit_failed callback on the offset commit listener if set
    # @param error [Rdkafka::RdkafkaError] the commit error
    # @param list [Rdkafka::Consumer::TopicPartitionList] the offsets of the commit
    def call_on_offsets_commit_failed(error, list)
      return unless consumer_offset_commit_listener
      return unless consumer_offset_commit_listener.respond_to?(:on_offsets_commit_failed)

      consumer_offset_commit_listener.on_offsets_commit_failed(error, list)
    end

    # @return [Boolean] true if the offset commit listener wants the result of every commit
    def offsets_committed_listened?
      consumer_offset_commit_listener.respond_to?(:on_offsets_committed)
    end

    # @return [Boolean] true if the offset commit listener wants the failed commits
    def offsets_commit_failed_listened?
      consumer_offset_commit_listener.respond_to?(:on_offsets_commit_failed)
    end
  end
end
