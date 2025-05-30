# frozen_string_literal: true

module Rdkafka
  # A producer for Kafka messages. To create a producer set up a {Config} and call {Config#producer producer} on that.
  class Producer
    include Helpers::Time
    include Helpers::OAuth

    # @private
    @@partitions_count_cache = PartitionsCountCache.new

    # Global (process wide) partitions cache. We use it to store number of topics partitions,
    # either from the librdkafka statistics (if enabled) or via direct inline calls every now and
    # then. Since the partitions count can only grow and should be same for all consumers and
    # producers, we can use a global cache as long as we ensure that updates only move up.
    #
    # @note It is critical to remember, that not all users may have statistics callbacks enabled,
    #   hence we should not make assumption that this cache is always updated from the stats.
    #
    # @return [Rdkafka::Producer::PartitionsCountCache]
    def self.partitions_count_cache
      @@partitions_count_cache
    end

    # @param partitions_count_cache [Rdkafka::Producer::PartitionsCountCache]
    def self.partitions_count_cache=(partitions_count_cache)
      @@partitions_count_cache = partitions_count_cache
    end

    # Empty hash used as a default
    EMPTY_HASH = {}.freeze

    private_constant :EMPTY_HASH

    # Raised when there was a critical issue when invoking rd_kafka_topic_new
    # This is a temporary solution until https://github.com/karafka/rdkafka-ruby/issues/451 is
    # resolved and this is normalized in all the places
    class TopicHandleCreationError < RuntimeError; end

    # @private
    # Returns the current delivery callback, by default this is nil.
    #
    # @return [Proc, nil]
    attr_reader :delivery_callback

    # @private
    # Returns the number of arguments accepted by the callback, by default this is nil.
    #
    # @return [Integer, nil]
    attr_reader :delivery_callback_arity

    # @private
    # @param native_kafka [NativeKafka]
    # @param partitioner_name [String, nil] name of the partitioner we want to use or nil to use
    #   the "consistent_random" default
    def initialize(native_kafka, partitioner_name)
      @topics_refs_map = {}
      @topics_configs = {}
      @native_kafka = native_kafka
      @partitioner_name = partitioner_name || "consistent_random"

      # Makes sure, that native kafka gets closed before it gets GCed by Ruby
      ObjectSpace.define_finalizer(self, native_kafka.finalizer)
    end

    # Sets alternative set of configuration details that can be set per topic
    # @note It is not allowed to re-set the same topic config twice because of the underlying
    #   librdkafka caching
    # @param topic [String] The topic name
    # @param config [Hash] config we want to use per topic basis
    # @param config_hash [Integer] hash of the config. We expect it here instead of computing it,
    #   because it is already computed during the retrieval attempt in the `#produce` flow.
    def set_topic_config(topic, config, config_hash)
      # Ensure lock on topic reference just in case
      @native_kafka.with_inner do |inner|
        @topics_refs_map[topic] ||= {}
        @topics_configs[topic] ||= {}

        return if @topics_configs[topic].key?(config_hash)

        # If config is empty, we create an empty reference that will be used with defaults
        rd_topic_config = if config.empty?
                            nil
                          else
                            Rdkafka::Bindings.rd_kafka_topic_conf_new.tap do |topic_config|
                              config.each do |key, value|
                                error_buffer = FFI::MemoryPointer.new(:char, 256)
                                result = Rdkafka::Bindings.rd_kafka_topic_conf_set(
                                  topic_config,
                                  key.to_s,
                                  value.to_s,
                                  error_buffer,
                                  256
                                )

                                unless result == :config_ok
                                  raise Config::ConfigError.new(error_buffer.read_string)
                                end
                              end
                            end
                          end

        topic_handle = Bindings.rd_kafka_topic_new(inner, topic, rd_topic_config)

        raise TopicHandleCreationError.new("Error creating topic handle for topic #{topic}") if topic_handle.null?

        @topics_configs[topic][config_hash] = config
        @topics_refs_map[topic][config_hash] = topic_handle
      end
    end

    # Starts the native Kafka polling thread and kicks off the init polling
    # @note Not needed to run unless explicit start was disabled
    def start
      @native_kafka.start
    end

    # @return [String] producer name
    def name
      @name ||= @native_kafka.with_inner do |inner|
        ::Rdkafka::Bindings.rd_kafka_name(inner)
      end
    end

    # Set a callback that will be called every time a message is successfully produced.
    # The callback is called with a {DeliveryReport} and {DeliveryHandle}
    #
    # @param callback [Proc, #call] The callback
    #
    # @return [nil]
    def delivery_callback=(callback)
      raise TypeError.new("Callback has to be callable") unless callback.respond_to?(:call)
      @delivery_callback = callback
      @delivery_callback_arity = arity(callback)
    end

    # Init transactions
    # Run once per producer
    def init_transactions
      closed_producer_check(__method__)

      @native_kafka.with_inner do |inner|
        response_ptr = Rdkafka::Bindings.rd_kafka_init_transactions(inner, -1)

        Rdkafka::RdkafkaError.validate!(response_ptr) || true
      end
    end

    def begin_transaction
      closed_producer_check(__method__)

      @native_kafka.with_inner do |inner|
        response_ptr = Rdkafka::Bindings.rd_kafka_begin_transaction(inner)

        Rdkafka::RdkafkaError.validate!(response_ptr) || true
      end
    end

    def commit_transaction(timeout_ms = -1)
      closed_producer_check(__method__)

      @native_kafka.with_inner do |inner|
        response_ptr = Rdkafka::Bindings.rd_kafka_commit_transaction(inner, timeout_ms)

        Rdkafka::RdkafkaError.validate!(response_ptr) || true
      end
    end

    def abort_transaction(timeout_ms = -1)
      closed_producer_check(__method__)

      @native_kafka.with_inner do |inner|
        response_ptr = Rdkafka::Bindings.rd_kafka_abort_transaction(inner, timeout_ms)
        Rdkafka::RdkafkaError.validate!(response_ptr) || true
      end
    end

    # Sends provided offsets of a consumer to the transaction for collective commit
    #
    # @param consumer [Consumer] consumer that owns the given tpls
    # @param tpl [Consumer::TopicPartitionList]
    # @param timeout_ms [Integer] offsets send timeout
    # @note Use **only** in the context of an active transaction
    def send_offsets_to_transaction(consumer, tpl, timeout_ms = 5_000)
      closed_producer_check(__method__)

      return if tpl.empty?

      cgmetadata = consumer.consumer_group_metadata_pointer
      native_tpl = tpl.to_native_tpl

      @native_kafka.with_inner do |inner|
        response_ptr = Bindings.rd_kafka_send_offsets_to_transaction(inner, native_tpl, cgmetadata, timeout_ms)

        Rdkafka::RdkafkaError.validate!(response_ptr)
      end
    ensure
      if cgmetadata && !cgmetadata.null?
        Bindings.rd_kafka_consumer_group_metadata_destroy(cgmetadata)
      end

      Rdkafka::Bindings.rd_kafka_topic_partition_list_destroy(native_tpl) unless native_tpl.nil?
    end

    # Close this producer and wait for the internal poll queue to empty.
    def close
      return if closed?
      ObjectSpace.undefine_finalizer(self)

      @native_kafka.close do
        # We need to remove the topics references objects before we destroy the producer,
        # otherwise they would leak out
        @topics_refs_map.each_value do |refs|
          refs.each_value do |ref|
            Rdkafka::Bindings.rd_kafka_topic_destroy(ref)
          end
        end
      end

      @topics_refs_map.clear
    end

    # Whether this producer has closed
    def closed?
      @native_kafka.closed?
    end

    # Wait until all outstanding producer requests are completed, with the given timeout
    # in seconds. Call this before closing a producer to ensure delivery of all messages.
    #
    # @param timeout_ms [Integer] how long should we wait for flush of all messages
    # @return [Boolean] true if no more data and all was flushed, false in case there are still
    #   outgoing messages after the timeout
    #
    # @note We raise an exception for other errors because based on the librdkafka docs, there
    #   should be no other errors.
    #
    # @note For `timed_out` we do not raise an error to keep it backwards compatible
    def flush(timeout_ms=5_000)
      closed_producer_check(__method__)

      error = @native_kafka.with_inner do |inner|
        response = Rdkafka::Bindings.rd_kafka_flush(inner, timeout_ms)
        Rdkafka::RdkafkaError.build(response)
      end

      # Early skip not to build the error message
      return true unless error
      return false if error.code == :timed_out

      raise(error)
    end

    # Purges the outgoing queue and releases all resources.
    #
    # Useful when closing the producer with outgoing messages to unstable clusters or when for
    # any other reasons waiting cannot go on anymore. This purges both the queue and all the
    # inflight requests + updates the delivery handles statuses so they can be materialized into
    # `purge_queue` errors.
    def purge
      closed_producer_check(__method__)

      @native_kafka.with_inner do |inner|
        response = Bindings.rd_kafka_purge(
          inner,
          Bindings::RD_KAFKA_PURGE_F_QUEUE | Bindings::RD_KAFKA_PURGE_F_INFLIGHT
        )

        Rdkafka::RdkafkaError.validate!(response)
      end

      # Wait for the purge to affect everything
      sleep(0.001) until flush(100)

      true
    end

    # Partition count for a given topic.
    #
    # @param topic [String] The topic name.
    # @return [Integer] partition count for a given topic or `-1` if it could not be obtained.
    #
    # @note If 'allow.auto.create.topics' is set to true in the broker, the topic will be
    #   auto-created after returning nil.
    #
    # @note We cache the partition count for a given topic for given time. If statistics are
    #   enabled for any producer or consumer, it will take precedence over per instance fetching.
    #
    #   This prevents us in case someone uses `partition_key` from querying for the count with
    #   each message. Instead we query at most once every 30 seconds at most if we have a valid
    #   partition count or every 5 seconds in case we were not able to obtain number of partitions.
    def partition_count(topic)
      closed_producer_check(__method__)

      self.class.partitions_count_cache.get(topic) do
        topic_metadata = nil

        @native_kafka.with_inner do |inner|
          topic_metadata = ::Rdkafka::Metadata.new(inner, topic).topics&.first
        end

        topic_metadata ? topic_metadata[:partition_count] : -1
      end
    rescue Rdkafka::RdkafkaError => e
      # If the topic does not exist, it will be created or if not allowed another error will be
      # raised. We here return -1 so this can happen without early error happening on metadata
      # discovery.
      return -1 if e.code == :unknown_topic_or_part

      raise(e)
    end

    # Produces a message to a Kafka topic. The message is added to rdkafka's queue, call {DeliveryHandle#wait wait} on the returned delivery handle to make sure it is delivered.
    #
    # When no partition is specified the underlying Kafka library picks a partition based on the key. If no key is specified, a random partition will be used.
    # When a timestamp is provided this is used instead of the auto-generated timestamp.
    #
    # @param topic [String] The topic to produce to
    # @param payload [String,nil] The message's payload
    # @param key [String, nil] The message's key
    # @param partition [Integer,nil] Optional partition to produce to
    # @param partition_key [String, nil] Optional partition key based on which partition assignment can happen
    # @param timestamp [Time,Integer,nil] Optional timestamp of this message. Integer timestamp is in milliseconds since Jan 1 1970.
    # @param headers [Hash<String,String|Array<String>>] Optional message headers. Values can be either a single string or an array of strings to support duplicate headers per KIP-82
    # @param label [Object, nil] a label that can be assigned when producing a message that will be part of the delivery handle and the delivery report
    # @param topic_config [Hash] topic config for given message dispatch. Allows to send messages to topics with different configuration
    #
    # @return [DeliveryHandle] Delivery handle that can be used to wait for the result of producing this message
    #
    # @raise [RdkafkaError] When adding the message to rdkafka's queue failed
    def produce(
      topic:,
      payload: nil,
      key: nil,
      partition: nil,
      partition_key: nil,
      timestamp: nil,
      headers: nil,
      label: nil,
      topic_config: EMPTY_HASH
    )
      closed_producer_check(__method__)

      # Start by checking and converting the input

      # Get payload length
      payload_size = if payload.nil?
                       0
                     else
                       payload.bytesize
                     end

      # Get key length
      key_size = if key.nil?
                   0
                 else
                   key.bytesize
                 end

      topic_config_hash = topic_config.hash

      # Checks if we have the rdkafka topic reference object ready. It saves us on object
      # allocation and allows to use custom config on demand.
      set_topic_config(topic, topic_config, topic_config_hash) unless @topics_refs_map.dig(topic, topic_config_hash)
      topic_ref = @topics_refs_map.dig(topic, topic_config_hash)

      if partition_key
        partition_count = partition_count(topic)

        # Check if there are no overrides for the partitioner and use the default one only when
        # no per-topic is present.
        partitioner_name = @topics_configs.dig(topic, topic_config_hash, :partitioner) || @partitioner_name

        # If the topic is not present, set to -1
        partition = Rdkafka::Bindings.partitioner(partition_key, partition_count, partitioner_name) if partition_count.positive?
      end

      # If partition is nil, use -1 to let librdafka set the partition randomly or
      # based on the key when present.
      partition ||= -1

      # If timestamp is nil use 0 and let Kafka set one. If an integer or time
      # use it.
      raw_timestamp = if timestamp.nil?
                        0
                      elsif timestamp.is_a?(Integer)
                        timestamp
                      elsif timestamp.is_a?(Time)
                        (timestamp.to_i * 1000) + (timestamp.usec / 1000)
                      else
                        raise TypeError.new("Timestamp has to be nil, an Integer or a Time")
                      end

      delivery_handle = DeliveryHandle.new
      delivery_handle.label = label
      delivery_handle.topic = topic
      delivery_handle[:pending] = true
      delivery_handle[:response] = -1
      delivery_handle[:partition] = -1
      delivery_handle[:offset] = -1
      DeliveryHandle.register(delivery_handle)

      args = [
        :int, Rdkafka::Bindings::RD_KAFKA_VTYPE_RKT, :pointer, topic_ref,
        :int, Rdkafka::Bindings::RD_KAFKA_VTYPE_MSGFLAGS, :int, Rdkafka::Bindings::RD_KAFKA_MSG_F_COPY,
        :int, Rdkafka::Bindings::RD_KAFKA_VTYPE_VALUE, :buffer_in, payload, :size_t, payload_size,
        :int, Rdkafka::Bindings::RD_KAFKA_VTYPE_KEY, :buffer_in, key, :size_t, key_size,
        :int, Rdkafka::Bindings::RD_KAFKA_VTYPE_PARTITION, :int32, partition,
        :int, Rdkafka::Bindings::RD_KAFKA_VTYPE_TIMESTAMP, :int64, raw_timestamp,
        :int, Rdkafka::Bindings::RD_KAFKA_VTYPE_OPAQUE, :pointer, delivery_handle,
      ]

      if headers
        headers.each do |key0, value0|
          key = key0.to_s
          if value0.is_a?(Array)
            # Handle array of values per KIP-82
            value0.each do |value|
              value = value.to_s
              args << :int << Rdkafka::Bindings::RD_KAFKA_VTYPE_HEADER
              args << :string << key
              args << :pointer << value
              args << :size_t << value.bytesize
            end
          else
            # Handle single value
            value = value0.to_s
            args << :int << Rdkafka::Bindings::RD_KAFKA_VTYPE_HEADER
            args << :string << key
            args << :pointer << value
            args << :size_t << value.bytesize
          end
        end
      end

      args << :int << Rdkafka::Bindings::RD_KAFKA_VTYPE_END

      # Produce the message
      response = @native_kafka.with_inner do |inner|
        Rdkafka::Bindings.rd_kafka_producev(
          inner,
          *args
        )
      end

      # Raise error if the produce call was not successful
      if response != 0
        DeliveryHandle.remove(delivery_handle.to_ptr.address)
        Rdkafka::RdkafkaError.validate!(response)
      end

      delivery_handle
    end

    # Calls (if registered) the delivery callback
    #
    # @param delivery_report [Producer::DeliveryReport]
    # @param delivery_handle [Producer::DeliveryHandle]
    def call_delivery_callback(delivery_report, delivery_handle)
      return unless @delivery_callback

      case @delivery_callback_arity
      when 0
        @delivery_callback.call
      when 1
        @delivery_callback.call(delivery_report)
      else
        @delivery_callback.call(delivery_report, delivery_handle)
      end
    end

    # Figures out the arity of a given block/method
    #
    # @param callback [#call, Proc]
    # @return [Integer] arity of the provided block/method
    def arity(callback)
      return callback.arity if callback.respond_to?(:arity)

      callback.method(:call).arity
    end

    private

    # Ensures, no operations can happen on a closed producer
    #
    # @param method [Symbol] name of the method that invoked producer
    # @raise [Rdkafka::ClosedProducerError]
    def closed_producer_check(method)
      raise Rdkafka::ClosedProducerError.new(method) if closed?
    end
  end
end
