# frozen_string_literal: true

module Rdkafka
  # A producer for Kafka messages. To create a producer set up a {Config} and call {Config#producer producer} on that.
  class Producer
    include Helpers::Time

    # Cache partitions count for 30 seconds
    PARTITIONS_COUNT_TTL = 30

    private_constant :PARTITIONS_COUNT_TTL

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
    def initialize(native_kafka, partitioner_name)
      @native_kafka = native_kafka
      @partitioner_name = partitioner_name || "consistent_random"

      # Makes sure, that native kafka gets closed before it gets GCed by Ruby
      ObjectSpace.define_finalizer(self, native_kafka.finalizer)

      @_partitions_count_cache = Hash.new do |cache, topic|
        topic_metadata = nil

        @native_kafka.with_inner do |inner|
          topic_metadata = ::Rdkafka::Metadata.new(inner, topic).topics&.first
        end

        cache[topic] = [
          monotonic_now,
          topic_metadata ? topic_metadata[:partition_count] : nil
        ]
      end
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

    # Close this producer and wait for the internal poll queue to empty.
    def close
      return if closed?
      ObjectSpace.undefine_finalizer(self)
      @native_kafka.close
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

      code = nil

      @native_kafka.with_inner do |inner|
        code = Rdkafka::Bindings.rd_kafka_flush(inner, timeout_ms)
      end

      # Early skip not to build the error message
      return true if code.zero?

      error = Rdkafka::RdkafkaError.new(code)

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

      code = nil

      @native_kafka.with_inner do |inner|
        code = Bindings.rd_kafka_purge(
          inner,
          Bindings::RD_KAFKA_PURGE_F_QUEUE | Bindings::RD_KAFKA_PURGE_F_INFLIGHT
        )
      end

      code.zero? || raise(Rdkafka::RdkafkaError.new(code))

      # Wait for the purge to affect everything
      sleep(0.001) until flush(100)

      true
    end

    # Partition count for a given topic.
    #
    # @param topic [String] The topic name.
    # @return [Integer] partition count for a given topic
    #
    # @note If 'allow.auto.create.topics' is set to true in the broker, the topic will be
    #   auto-created after returning nil.
    #
    # @note We cache the partition count for a given topic for given time.
    #   This prevents us in case someone uses `partition_key` from querying for the count with
    #   each message. Instead we query once every 30 seconds at most
    def partition_count(topic)
      closed_producer_check(__method__)

      @_partitions_count_cache.delete_if do |_, cached|
        monotonic_now - cached.first > PARTITIONS_COUNT_TTL
      end

      @_partitions_count_cache[topic].last
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
    # @param headers [Hash<String,String>] Optional message headers
    #
    # @return [DeliveryHandle] Delivery handle that can be used to wait for the result of producing this message
    #
    # @raise [RdkafkaError] When adding the message to rdkafka's queue failed
    def produce(topic:, payload: nil, key: nil, partition: nil, partition_key: nil, timestamp: nil, headers: nil)
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

      if partition_key
        partition_count = partition_count(topic)
        # If the topic is not present, set to -1
        partition = Rdkafka::Bindings.partitioner(partition_key, partition_count, @partitioner_name) if partition_count
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
      delivery_handle[:pending] = true
      delivery_handle[:response] = -1
      delivery_handle[:partition] = -1
      delivery_handle[:offset] = -1
      DeliveryHandle.register(delivery_handle)

      args = [
        :int, Rdkafka::Bindings::RD_KAFKA_VTYPE_TOPIC, :string, topic,
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
          value = value0.to_s
          args << :int << Rdkafka::Bindings::RD_KAFKA_VTYPE_HEADER
          args << :string << key
          args << :pointer << value
          args << :size_t << value.bytes.size
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
        raise RdkafkaError.new(response)
      end

      delivery_handle
    end

    def call_delivery_callback(delivery_report, delivery_handle)
      return unless @delivery_callback

      args = [delivery_report, delivery_handle].take(@delivery_callback_arity)
      @delivery_callback.call(*args)
    end

    def arity(callback)
      return callback.arity if callback.respond_to?(:arity)

      callback.method(:call).arity
    end

    private

    def closed_producer_check(method)
      raise Rdkafka::ClosedProducerError.new(method) if closed?
    end
  end
end
