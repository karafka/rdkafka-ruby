# frozen_string_literal: true

module Rdkafka
  # Provides default timeout and configuration values used throughout the library.
  #
  # These constants standardize timing values across consumers, producers, and admin clients.
  # Values are specified in milliseconds (ms) unless otherwise noted.
  module Defaults
    # Consumer timeout for fetching committed offsets
    # @see Consumer#committed
    CONSUMER_COMMITTED_TIMEOUT_MS = 2_000

    # Consumer timeout for querying watermark offsets
    # @see Consumer#query_watermark_offsets
    CONSUMER_QUERY_WATERMARK_TIMEOUT_MS = 1_000

    # Consumer timeout for lag calculations
    # @see Consumer#lag
    CONSUMER_LAG_TIMEOUT_MS = 1_000

    # Consumer timeout for offset-by-timestamp lookups
    # @see Consumer#offsets_for_times
    CONSUMER_OFFSETS_FOR_TIMES_TIMEOUT_MS = 1_000

    # Consumer timeout for fetching the cluster id when it is not already cached from metadata
    # @see Consumer#cluster_id
    CONSUMER_CLUSTER_ID_TIMEOUT_MS = 1_000

    # Consumer timeout for poll operations (used in each iteration)
    # @see Consumer#each
    CONSUMER_POLL_TIMEOUT_MS = 250

    # Consumer timeout for seek operations (0 = non-blocking)
    # @see Consumer#seek_by
    CONSUMER_SEEK_TIMEOUT_MS = 0

    # Consumer timeout for events_poll (0 = non-blocking async)
    # @see Consumer#events_poll
    CONSUMER_EVENTS_POLL_TIMEOUT_MS = 0

    # Producer timeout for flush operations
    # @see Producer#flush
    PRODUCER_FLUSH_TIMEOUT_MS = 5_000

    # Producer timeout for flush during purge
    # @see Producer#purge
    PRODUCER_PURGE_FLUSH_TIMEOUT_MS = 100

    # Sleep interval used in producer purge loop
    # @see Producer#purge
    PRODUCER_PURGE_SLEEP_INTERVAL_MS = 1

    # Timeout for transactional send_offsets_to_transaction
    # @see Producer#send_offsets_to_transaction
    PRODUCER_SEND_OFFSETS_TIMEOUT_MS = 5_000

    # Default timeout for metadata requests
    # @see Metadata#initialize
    # @see Admin#metadata
    METADATA_TIMEOUT_MS = 2_000

    # Hard ceiling on metadata fetch attempts on transient errors (backstop; the retry budget
    # normally ends the loop first)
    # @see Metadata#initialize
    METADATA_MAX_RETRIES = 10

    # Minimum metadata fetch attempts before the retry budget may end the loop, so a slow broker
    # (whose requests each consume the full timeout) still gets a few tries
    # @see Metadata#initialize
    METADATA_MIN_ATTEMPTS = 3

    # Soft wall-clock budget for the whole metadata retry loop; past it (and past
    # METADATA_MIN_ATTEMPTS) the loop stops so a synchronous fetch cannot block the caller for long
    # @see Metadata#initialize
    METADATA_RETRY_BUDGET_MS = 5_000

    # Base backoff time for metadata retry (100ms = 0.1s)
    # @see Metadata#initialize
    METADATA_RETRY_BACKOFF_BASE_MS = 100

    # Maximum backoff time between metadata retries. Caps the exponential backoff so a long retry
    # sequence against an unhealthy cluster cannot block the calling thread for minutes.
    # @see Metadata#initialize
    METADATA_RETRY_BACKOFF_MAX_MS = 1_000

    # Default wait timeout for operation handles
    # @see AbstractHandle#wait
    HANDLE_WAIT_TIMEOUT_MS = 60_000

    # Polling interval for NativeKafka background thread
    # @see NativeKafka#initialize
    # @see Config#producer
    # @see Config#admin
    NATIVE_KAFKA_POLL_TIMEOUT_MS = 100

    # Sleep interval used in NativeKafka#synchronize wait loop
    # @see NativeKafka#synchronize
    NATIVE_KAFKA_SYNCHRONIZE_SLEEP_INTERVAL_MS = 10

    # TTL for partitions count cache entries (30 seconds)
    # @see Producer::PartitionsCountCache
    PARTITIONS_COUNT_CACHE_TTL_MS = 30_000
  end
end
