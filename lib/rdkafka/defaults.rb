# frozen_string_literal: true

module Rdkafka
  # Default timeout and timing values used throughout rdkafka-ruby.
  #
  # All timeout values can be overridden per-call via method parameters.
  # These constants provide a central place to understand and reference
  # the default values used across the library.
  #
  # @note These are rdkafka-ruby defaults, not librdkafka configuration options.
  #   For librdkafka options, see:
  #   https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
  #
  # @example Overriding a timeout per-call
  #   consumer.committed(timeout_ms: 5_000)  # Use 5 seconds instead of default 2 seconds
  #
  # @example Checking the default value
  #   Rdkafka::Defaults::CONSUMER_COMMITTED_TIMEOUT_MS  # => 2000
  module Defaults
    # Consumer timeouts (in milliseconds)

    # Default timeout for fetching committed offsets
    # @see Consumer#committed
    CONSUMER_COMMITTED_TIMEOUT_MS = 2_000

    # Default timeout for querying watermark offsets
    # @see Consumer#query_watermark_offsets
    CONSUMER_QUERY_WATERMARK_TIMEOUT_MS = 1_000

    # Default timeout for lag calculation watermark queries
    # @see Consumer#lag
    CONSUMER_LAG_TIMEOUT_MS = 1_000

    # Default timeout for offsets_for_times operation
    # @see Consumer#offsets_for_times
    CONSUMER_OFFSETS_FOR_TIMES_TIMEOUT_MS = 1_000

    # Default poll timeout for Consumer#each iterator
    # @see Consumer#each
    CONSUMER_POLL_TIMEOUT_MS = 250

    # Seek operation timeout (0 = non-blocking)
    # @see Consumer#seek_by
    CONSUMER_SEEK_TIMEOUT_MS = 0

    # Events poll timeout (0 = non-blocking/async)
    # @see Consumer#events_poll
    CONSUMER_EVENTS_POLL_TIMEOUT_MS = 0

    # Producer timeouts (in milliseconds)

    # Default timeout for producer flush operation
    # @see Producer#flush
    PRODUCER_FLUSH_TIMEOUT_MS = 5_000

    # Default flush timeout during purge operation
    # @see Producer#purge
    PRODUCER_PURGE_FLUSH_TIMEOUT_MS = 100

    # Metadata timeouts (in milliseconds)

    # Default timeout for metadata requests
    # @see Admin#metadata
    # @see Metadata#initialize
    METADATA_TIMEOUT_MS = 2_000

    # Handle wait timeouts (in milliseconds)

    # Default maximum wait timeout for async handles (delivery, admin operations)
    # @see AbstractHandle#wait
    HANDLE_WAIT_TIMEOUT_MS = 60_000

    # Native Kafka polling (in milliseconds)

    # Default poll timeout for producer/admin native polling thread
    # @see Config#producer
    # @see Config#admin
    NATIVE_KAFKA_POLL_TIMEOUT_MS = 100

    # Internal timing (in milliseconds)

    # Sleep interval during purge wait loop
    # @see Producer#purge
    PRODUCER_PURGE_SLEEP_INTERVAL_MS = 1

    # Sleep interval while waiting for operations to complete in NativeKafka#synchronize
    # @see NativeKafka#synchronize
    NATIVE_KAFKA_SYNCHRONIZE_SLEEP_INTERVAL_MS = 10

    # Base backoff factor for metadata retry in milliseconds (multiplied by 2^attempt)
    # @see Metadata#initialize
    METADATA_RETRY_BACKOFF_BASE_MS = 100

    # Cache settings (in milliseconds)

    # Default time-to-live for cached partition counts
    # @see Producer::PartitionsCountCache
    PARTITIONS_COUNT_CACHE_TTL_MS = 30_000

    # Configuration values (not time-based)

    # Maximum number of metadata fetch retry attempts
    # @see Metadata#initialize
    METADATA_MAX_RETRIES = 10
  end
end
