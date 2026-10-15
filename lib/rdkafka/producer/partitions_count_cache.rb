# frozen_string_literal: true

module Rdkafka
  class Producer
    # Caching mechanism for Kafka topic partition counts to avoid frequent cluster queries
    #
    # This cache is designed to optimize the process of obtaining partition counts for topics.
    # It uses several strategies to minimize Kafka cluster queries:
    #
    # @note Design considerations:
    #
    # 1. Statistics-based updates
    #    When statistics callbacks are enabled (via `statistics.interval.ms`), we leverage
    #    this data to proactively update the partition counts cache. This approach costs
    #    approximately 0.02ms of processing time during each statistics interval (typically
    #    every 5 seconds) but eliminates the need for explicit blocking metadata queries.
    #
    # 2. Edge case handling
    #    If a user configures `statistics.interval.ms` much higher than the default cache TTL
    #    ({Defaults::PARTITIONS_COUNT_CACHE_TTL_MS}ms), the cache will still function correctly.
    #    When statistics updates don't occur frequently enough, the cache entries will expire
    #    naturally, triggering a blocking refresh when needed.
    #
    # 3. User configuration awareness
    #    The cache respects user-defined settings. If `topic.metadata.refresh.interval.ms` is
    #    set very high, the responsibility for potentially stale data falls on the user. This
    #    is an explicit design choice to honor user configuration preferences and align with
    #    librdkafka settings.
    #
    # 4. Process-wide efficiency
    #    Since this cache is shared across all Rdkafka producers and consumers within a process,
    #    having multiple clients improves overall efficiency. Each client contributes to keeping
    #    the cache updated, benefiting all other clients.
    #
    # 5. Thread-safety approach
    #    The implementation uses fine-grained locking with per-topic mutexes to minimize
    #    contention in multi-threaded environments while ensuring data consistency.
    #
    # 6. Topic recreation handling
    #    If a topic is deleted and recreated with fewer partitions, the cache keeps reporting the
    #    higher count only until the entry's TTL expires. The first refresh after expiry performs
    #    an authoritative metadata read and adopts the lower count. Within the TTL window a lower
    #    value is still ignored, so a transient or racy lower read cannot clobber a correct higher
    #    count.
    #
    # 7. Cluster outages
    #    If the refresh of an expired entry fails on a transient error (for example all brokers
    #    are unreachable), the last known count is served instead of raising, and the next refresh
    #    is attempted after {Defaults::PARTITIONS_COUNT_CACHE_STALE_BACKOFF_MS}. This lets
    #    `partition_key` produces keep buffering in librdkafka during an outage. Without a known
    #    count the error is raised. Only one caller refreshes an expired entry at a time; the
    #    others get the known count meanwhile.
    class PartitionsCountCache
      include Helpers::Time

      # Refresh errors after which the last known count is still served
      STALE_FALLBACK_ERRORS = %i[
        transport
        timed_out
        all_brokers_down
        resolve
      ].freeze

      private_constant :STALE_FALLBACK_ERRORS

      # Creates a new partition count cache
      #
      # @param ttl [Integer, nil] DEPRECATED: Use ttl_ms instead.
      #   Time-to-live in seconds for cached values. Will be removed in v1.0.0.
      # @param ttl_ms [Integer, nil] Time-to-live in milliseconds for cached values.
      #   Defaults to {Defaults::PARTITIONS_COUNT_CACHE_TTL_MS}.
      def initialize(ttl = :not_provided, ttl_ms: :not_provided)
        @counts = {}
        @mutex_hash = {}
        # Used only for @mutex_hash access to ensure thread-safety when creating new mutexes
        @mutex_for_hash = Mutex.new

        # Determine which TTL value to use
        if ttl != :not_provided && ttl_ms != :not_provided
          warn "DEPRECATION WARNING: Both ttl and ttl_ms were provided to PartitionsCountCache. " \
               "Using ttl_ms. The ttl parameter is deprecated and will be removed in v1.0.0."
          @ttl_ms = ttl_ms
        elsif ttl != :not_provided
          warn "DEPRECATION WARNING: ttl (seconds) parameter for PartitionsCountCache is deprecated. " \
               "Use ttl_ms (milliseconds) instead. This parameter will be removed in v1.0.0."
          @ttl_ms = (ttl * 1000).to_i
        elsif ttl_ms == :not_provided
          @ttl_ms = Defaults::PARTITIONS_COUNT_CACHE_TTL_MS
        else
          @ttl_ms = ttl_ms
        end
      end

      # Reads partition count for a topic with automatic refresh when expired
      #
      # This method will return the cached partition count if available and not expired.
      # If the value is expired or not available, it will execute the provided block
      # to fetch the current value from Kafka.
      #
      # @param topic [String] Kafka topic name
      # @yield Block that returns the current partition count when cache needs refreshing
      # @yieldreturn [Integer] Current partition count retrieved from Kafka
      # @return [Integer] Partition count for the topic
      # @raise [RdkafkaError] when the refresh fails and there is no last known count to serve, or
      #   when it fails on a non-transient error
      #
      # @note The implementation prioritizes read performance over write consistency
      #   since partition counts typically only increase during normal operation.
      def get(topic)
        current_info = @counts[topic]

        if current_info.nil?
          new_count = yield
          set(topic, new_count)

          return @counts[topic][1]
        end

        return refresh(topic, current_info) { yield } if expired?(current_info[0])

        current_info[1]
      end

      # Update partition count for a topic when needed
      #
      # This method updates the partition count for a topic in the cache.
      # It uses a mutex to ensure thread-safety during updates.
      #
      # @param topic [String] Kafka topic name
      # @param new_count [Integer] New partition count value
      #
      # @note We prioritize higher partition counts and only accept them when using
      #   a mutex to ensure consistency. This design decision is based on the fact that
      #   partition counts in Kafka only increase during normal operation.
      def set(topic, new_count)
        # First check outside mutex to avoid unnecessary locking
        current_info = @counts[topic]

        # Within the TTL window a lower value is treated as a stale/racy read and ignored, since
        # partition counts only grow during normal operation. Once the entry has expired a lower
        # value is an authoritative refresh (e.g. the topic was recreated with fewer partitions),
        # so we fall through and adopt it below.
        if current_info && new_count < current_info[1] && !expired?(current_info[0])
          refresh_timestamp(topic)

          return
        end

        # Only lock the specific topic mutex
        mutex_for(topic).synchronize do
          # Check again inside the lock as another thread might have updated
          current_info = @counts[topic]

          if current_info.nil?
            # Create new entry
            @counts[topic] = [monotonic_now_ms, new_count]
          elsif new_count > current_info[1] || expired?(current_info[0])
            # A higher count always wins; a lower count is accepted only when the existing entry
            # has expired, so a concurrent fresh higher value (which reset the timestamp) is never
            # clobbered by a stale lower one.
            current_info[0] = monotonic_now_ms
            current_info[1] = new_count
          else
            # Same or lower count within the TTL window: refresh the timestamp only
            current_info[0] = monotonic_now_ms
          end
        end
      end

      # @return [Hash] hash with ttls and partitions counts array
      def to_h
        @counts
      end

      private

      # Get or create a mutex for a specific topic
      #
      # This method ensures that each topic has its own mutex,
      # allowing operations on different topics to proceed in parallel.
      #
      # @param topic [String] Kafka topic name
      # @return [Mutex] Mutex for the specified topic
      #
      # @note We use a separate mutex (@mutex_for_hash) to protect the creation
      #   of new topic mutexes. This pattern allows fine-grained locking while
      #   maintaining thread-safety.
      def mutex_for(topic)
        mutex = @mutex_hash[topic]

        return mutex if mutex

        # Use a separate mutex to protect the creation of new topic mutexes
        @mutex_for_hash.synchronize do
          # Check again in case another thread created it
          @mutex_hash[topic] ||= Mutex.new
        end

        @mutex_hash[topic]
      end

      # Update the timestamp without acquiring the mutex
      #
      # This is an optimization that allows refreshing the TTL of existing entries
      # without the overhead of mutex acquisition.
      #
      # @param topic [String] Kafka topic name
      #
      # @note This method is safe for refreshing existing data regardless of count
      #   because it only updates the timestamp, which doesn't affect the correctness
      #   of concurrent operations.
      def refresh_timestamp(topic)
        current_info = @counts[topic]

        return unless current_info

        # Update the timestamp in-place
        current_info[0] = monotonic_now_ms
      end

      # Refreshes an expired entry. While the refresh runs, other callers see a fresh entry and get
      # the known count instead of running their own metadata request.
      #
      # The block performs an authoritative metadata read. We hand it to `set`, which adopts a
      # higher count always and a lower count once the entry has expired (e.g. the topic was
      # recreated with fewer partitions). We then return whatever `set` settled on so a concurrent
      # refresh that wrote a higher value still wins.
      #
      # @param topic [String] Kafka topic name
      # @param current_info [Array<Integer>] cached timestamp and partition count
      # @yieldreturn [Integer] Current partition count retrieved from Kafka
      # @return [Integer] Partition count for the topic
      # @raise [RdkafkaError] when the refresh fails on a non-transient error
      def refresh(topic, current_info)
        expired_at = current_info[0]
        # Outlasts a failed refresh even with a TTL shorter than one metadata request
        lease = monotonic_now_ms + Defaults::PARTITIONS_COUNT_CACHE_STALE_BACKOFF_MS
        current_info[0] = lease

        begin
          new_count = yield
        rescue RdkafkaError => e
          raise unless STALE_FALLBACK_ERRORS.include?(e.code)
        ensure
          # Hand the entry back as expired, unless a concurrent write refreshed it meanwhile
          current_info[0] = expired_at if current_info[0] == lease
        end

        return serve_stale(current_info) if new_count.nil?

        set(topic, new_count)

        @counts[topic][1]
      end

      # Keeps serving an expired entry after a failed refresh and schedules the next refresh
      # after {Defaults::PARTITIONS_COUNT_CACHE_STALE_BACKOFF_MS}
      #
      # @param current_info [Array<Integer>] cached timestamp and partition count
      # @return [Integer] last known partition count
      def serve_stale(current_info)
        current_info[0] = monotonic_now_ms - @ttl_ms + Defaults::PARTITIONS_COUNT_CACHE_STALE_BACKOFF_MS

        current_info[1]
      end

      # Check if a timestamp has expired based on the TTL
      #
      # @param timestamp [Integer] Monotonic timestamp in milliseconds to check
      # @return [Boolean] true if expired, false otherwise
      def expired?(timestamp)
        monotonic_now_ms - timestamp > @ttl_ms
      end
    end
  end
end
