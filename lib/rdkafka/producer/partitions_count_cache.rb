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
    #    (30 seconds), the cache will still function correctly. When statistics updates don't
    #    occur frequently enough, the cache entries will expire naturally, triggering a
    #    blocking refresh when needed.
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
    #    If a topic is deleted and recreated with fewer partitions, the cache will continue to
    #    report the higher count until either the TTL expires or the process is restarted. This
    #    design choice simplifies the implementation while relying on librdkafka's error handling
    #    for edge cases. In production environments, topic recreation with different partition
    #    counts is typically accompanied by application restarts to handle structural changes.
    #    This also aligns with the previous cache implementation.
    class PartitionsCountCache
      include Helpers::Time

      # Default time-to-live for cached partition counts in seconds
      #
      # @note This default was chosen to balance freshness of metadata with performance
      #   optimization. Most Kafka cluster topology changes are planned operations, making 30
      #   seconds a reasonable compromise.
      DEFAULT_TTL = 30

      # Creates a new partition count cache
      #
      # @param ttl [Integer] Time-to-live in seconds for cached values
      def initialize(ttl = DEFAULT_TTL)
        @counts = {}
        @mutex_hash = {}
        # Used only for @mutex_hash access to ensure thread-safety when creating new mutexes
        @mutex_for_hash = Mutex.new
        @ttl = ttl
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
      #
      # @note The implementation prioritizes read performance over write consistency
      #   since partition counts typically only increase during normal operation.
      def get(topic)
        current_info = @counts[topic]

        if current_info.nil? || expired?(current_info[0])
          new_count = yield

          if current_info.nil?
            # No existing data, create a new entry with mutex
            set(topic, new_count)

            return new_count
          else
            current_count = current_info[1]

            if new_count > current_count
              # Higher value needs mutex to update both timestamp and count
              set(topic, new_count)

              return new_count
            else
              # Same or lower value, just update timestamp without mutex
              refresh_timestamp(topic)

              return current_count
            end
          end
        end

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

        # For lower values, we don't update count but might need to refresh timestamp
        if current_info && new_count < current_info[1]
          refresh_timestamp(topic)

          return
        end

        # Only lock the specific topic mutex
        mutex_for(topic).synchronize do
          # Check again inside the lock as another thread might have updated
          current_info = @counts[topic]

          if current_info.nil?
            # Create new entry
            @counts[topic] = [monotonic_now, new_count]
          else
            current_count = current_info[1]

            if new_count > current_count
              # Update to higher count value
              current_info[0] = monotonic_now
              current_info[1] = new_count
            else
              # Same or lower count, update timestamp only
              current_info[0] = monotonic_now
            end
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
        current_info[0] = monotonic_now
      end

      # Check if a timestamp has expired based on the TTL
      #
      # @param timestamp [Float] Monotonic timestamp to check
      # @return [Boolean] true if expired, false otherwise
      def expired?(timestamp)
        monotonic_now - timestamp > @ttl
      end
    end
  end
end
