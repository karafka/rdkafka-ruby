# frozen_string_literal: true

module Rdkafka
  # Provides cluster metadata information
  class Metadata
    # @return [Array<Hash>] list of broker metadata
    attr_reader :brokers
    # @return [Array<Hash>] list of topic metadata
    attr_reader :topics

    # Errors upon which we retry the metadata fetch
    RETRIED_ERRORS = %i[
      timed_out
      leader_not_available
    ].freeze

    private_constant :RETRIED_ERRORS

    # Fetches metadata from the Kafka cluster
    #
    # @param native_client [FFI::Pointer] pointer to the native Kafka client
    # @param topic_name [String, nil] specific topic to fetch metadata for, or nil for all topics
    # @param timeout_ms [Integer] timeout in milliseconds
    # @raise [RdkafkaError] when metadata fetch fails
    def initialize(native_client, topic_name = nil, timeout_ms = Defaults::METADATA_TIMEOUT_MS)
      attempt = 0
      deadline = ::Process.clock_gettime(::Process::CLOCK_MONOTONIC) +
        Defaults::METADATA_RETRY_BUDGET_MS / 1_000.0

      begin
        attempt += 1
        fetch_metadata(native_client, topic_name, timeout_ms)
      rescue ::Rdkafka::RdkafkaError => e
        raise unless RETRIED_ERRORS.include?(e.code)
        raise if attempt > Defaults::METADATA_MAX_RETRIES

        # Stop once the wall-clock retry budget is spent, but only after at least
        # METADATA_MIN_ATTEMPTS tries so a slow broker (whose requests each consume the full
        # timeout) still gets a few tries rather than being cut off after one or two.
        raise if attempt >= Defaults::METADATA_MIN_ATTEMPTS &&
          ::Process.clock_gettime(::Process::CLOCK_MONOTONIC) >= deadline

        # Exponential backoff between attempts, capped so a long retry sequence cannot block for
        # minutes. The request timeout (`timeout_ms`) is intentionally left unchanged: it used to be
        # overwritten with the backoff value, which shrank the first retries below the configured
        # timeout (near-guaranteeing another timeout) and then inflated later ones to ~100s.
        backoff_ms = [
          (2**attempt) * Defaults::METADATA_RETRY_BACKOFF_BASE_MS,
          Defaults::METADATA_RETRY_BACKOFF_MAX_MS
        ].min

        sleep(backoff_ms / 1_000.0)

        retry
      end
    end

    private

    # Performs a single metadata fetch attempt and frees its native resources before returning.
    #
    # This is intentionally separate from the retry loop in {#initialize}: `retry` restarts the
    # `begin` block without running its `ensure`, so cleaning up here (rather than in an `ensure`
    # around the loop) guarantees every attempt destroys its own native topic handle and metadata
    # struct instead of leaking all but the last one.
    #
    # @param native_client [FFI::Pointer] pointer to the native Kafka client
    # @param topic_name [String, nil] specific topic to fetch metadata for, or nil for all topics
    # @param timeout_ms [Integer] timeout in milliseconds
    def fetch_metadata(native_client, topic_name, timeout_ms)
      native_topic = nil
      metadata_ptr = nil

      native_topic = Rdkafka::Bindings.rd_kafka_topic_new(native_client, topic_name, nil) if topic_name

      ptr = FFI::MemoryPointer.new(:pointer)

      # If topic_flag is 1, we request info about *all* topics in the cluster.  If topic_flag is 0,
      # we only request info about locally known topics (or a single topic if one is passed in).
      topic_flag = topic_name.nil? ? 1 : 0

      # Retrieve the Metadata
      result = Rdkafka::Bindings.rd_kafka_metadata(native_client, topic_flag, native_topic, ptr, timeout_ms)

      Rdkafka::RdkafkaError.validate!(result)

      # rd_kafka_metadata only allocates the struct on success, so we read the pointer to destroy
      # only after validate! has confirmed the call succeeded.
      metadata_ptr = ptr.read_pointer

      metadata_from_native(metadata_ptr)
    ensure
      Rdkafka::Bindings.rd_kafka_topic_destroy(native_topic) if native_topic
      Rdkafka::Bindings.rd_kafka_metadata_destroy(metadata_ptr) if metadata_ptr && !metadata_ptr.null?
    end

    # Extracts metadata from native pointer
    # @param ptr [FFI::Pointer] pointer to native metadata
    def metadata_from_native(ptr)
      metadata = Metadata.new(ptr)
      @brokers = Array.new(metadata[:brokers_count]) do |i|
        BrokerMetadata.new(metadata[:brokers_metadata] + (i * BrokerMetadata.size)).to_h
      end

      @topics = Array.new(metadata[:topics_count]) do |i|
        topic = TopicMetadata.new(metadata[:topics_metadata] + (i * TopicMetadata.size))

        RdkafkaError.validate!(topic[:rd_kafka_resp_err])

        partitions = Array.new(topic[:partition_count]) do |j|
          partition = PartitionMetadata.new(topic[:partitions_metadata] + (j * PartitionMetadata.size))
          RdkafkaError.validate!(partition[:rd_kafka_resp_err])
          partition.to_h
        end
        topic.to_h.merge!(partitions: partitions)
      end
    end

    # Base class for metadata FFI structs with hash conversion
    # @private
    class CustomFFIStruct < FFI::Struct
      # Converts struct to a hash
      # @return [Hash]
      def to_h
        members.each_with_object({}) do |mem, hsh|
          val = self.[](mem)
          next if val.is_a?(FFI::Pointer) || mem == :rd_kafka_resp_err

          hsh[mem] = self.[](mem)
        end
      end
    end

    # @private
    # FFI struct for rd_kafka_metadata_t
    class Metadata < CustomFFIStruct
      layout :brokers_count, :int,
        :brokers_metadata, :pointer,
        :topics_count, :int,
        :topics_metadata, :pointer,
        :broker_id, :int32,
        :broker_name, :string
    end

    # @private
    # FFI struct for rd_kafka_metadata_broker_t
    class BrokerMetadata < CustomFFIStruct
      layout :broker_id, :int32,
        :broker_name, :string,
        :broker_port, :int
    end

    # @private
    # FFI struct for rd_kafka_metadata_topic_t
    class TopicMetadata < CustomFFIStruct
      layout :topic_name, :string,
        :partition_count, :int,
        :partitions_metadata, :pointer,
        :rd_kafka_resp_err, :int
    end

    # @private
    # FFI struct for rd_kafka_metadata_partition_t
    class PartitionMetadata < CustomFFIStruct
      layout :partition_id, :int32,
        :rd_kafka_resp_err, :int,
        :leader, :int32,
        :replica_count, :int,
        :replicas, :pointer,
        :in_sync_replica_brokers, :int,
        :isrs, :pointer

      # The base `#to_h` skips FFI pointer members, which would drop the replica and in-sync
      # replica assignments entirely. We dereference those pointers here so the partition hash
      # exposes the broker ids backing the partition (needed e.g. to plan replication changes).
      #
      # @return [Hash{Symbol => Integer, Array<Integer>}] partition metadata:
      #   * +:partition_id+ (Integer) - partition id
      #   * +:leader+ (Integer) - broker id of the partition leader
      #   * +:replica_count+ (Integer) - number of assigned replicas
      #   * +:in_sync_replica_brokers+ (Integer) - number of in-sync replicas
      #   * +:replicas+ (Array<Integer>) - broker ids of the assigned replicas
      #   * +:isrs+ (Array<Integer>) - broker ids of the in-sync replicas
      def to_h
        super.merge(
          replicas: read_broker_ids(self[:replicas], self[:replica_count]),
          isrs: read_broker_ids(self[:isrs], self[:in_sync_replica_brokers])
        )
      end

      private

      # Reads `count` broker ids (int32) from a replicas/isrs pointer.
      # @param pointer [FFI::Pointer] pointer to the broker ids array
      # @param count [Integer] number of broker ids to read
      # @return [Array<Integer>] broker ids (empty when there are none)
      def read_broker_ids(pointer, count)
        return [] if count.zero? || pointer.null?

        pointer.read_array_of_int32(count)
      end
    end
  end
end
