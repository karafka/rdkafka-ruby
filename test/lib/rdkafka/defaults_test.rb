# frozen_string_literal: true

require "test_helper"

class RdkafkaDefaultsTest < Minitest::Test
  def test_consumer_committed_timeout_ms
    assert_equal 2_000, Rdkafka::Defaults::CONSUMER_COMMITTED_TIMEOUT_MS
  end

  def test_consumer_query_watermark_timeout_ms
    assert_equal 1_000, Rdkafka::Defaults::CONSUMER_QUERY_WATERMARK_TIMEOUT_MS
  end

  def test_consumer_lag_timeout_ms
    assert_equal 1_000, Rdkafka::Defaults::CONSUMER_LAG_TIMEOUT_MS
  end

  def test_consumer_offsets_for_times_timeout_ms
    assert_equal 1_000, Rdkafka::Defaults::CONSUMER_OFFSETS_FOR_TIMES_TIMEOUT_MS
  end

  def test_consumer_poll_timeout_ms
    assert_equal 250, Rdkafka::Defaults::CONSUMER_POLL_TIMEOUT_MS
  end

  def test_producer_flush_timeout_ms
    assert_equal 5_000, Rdkafka::Defaults::PRODUCER_FLUSH_TIMEOUT_MS
  end

  def test_producer_purge_flush_timeout_ms
    assert_equal 100, Rdkafka::Defaults::PRODUCER_PURGE_FLUSH_TIMEOUT_MS
  end

  def test_metadata_timeout_ms
    assert_equal 2_000, Rdkafka::Defaults::METADATA_TIMEOUT_MS
  end

  def test_handle_wait_timeout_ms
    assert_equal 60_000, Rdkafka::Defaults::HANDLE_WAIT_TIMEOUT_MS
  end

  def test_native_kafka_poll_timeout_ms
    assert_equal 100, Rdkafka::Defaults::NATIVE_KAFKA_POLL_TIMEOUT_MS
  end

  def test_producer_purge_sleep_interval_ms
    assert_equal 1, Rdkafka::Defaults::PRODUCER_PURGE_SLEEP_INTERVAL_MS
  end

  def test_native_kafka_synchronize_sleep_interval_ms
    assert_equal 10, Rdkafka::Defaults::NATIVE_KAFKA_SYNCHRONIZE_SLEEP_INTERVAL_MS
  end

  def test_metadata_retry_backoff_base_ms
    assert_equal 100, Rdkafka::Defaults::METADATA_RETRY_BACKOFF_BASE_MS
  end

  def test_metadata_max_retries
    assert_equal 10, Rdkafka::Defaults::METADATA_MAX_RETRIES
  end

  def test_consumer_seek_timeout_ms
    assert_equal 0, Rdkafka::Defaults::CONSUMER_SEEK_TIMEOUT_MS
  end

  def test_consumer_events_poll_timeout_ms
    assert_equal 0, Rdkafka::Defaults::CONSUMER_EVENTS_POLL_TIMEOUT_MS
  end

  def test_partitions_count_cache_ttl_ms
    assert_equal 30_000, Rdkafka::Defaults::PARTITIONS_COUNT_CACHE_TTL_MS
  end

  def test_all_constants_are_defined
    constants = Rdkafka::Defaults.constants

    refute_empty constants
    expected = %i[
      CONSUMER_COMMITTED_TIMEOUT_MS
      CONSUMER_QUERY_WATERMARK_TIMEOUT_MS
      CONSUMER_LAG_TIMEOUT_MS
      CONSUMER_OFFSETS_FOR_TIMES_TIMEOUT_MS
      CONSUMER_POLL_TIMEOUT_MS
      PRODUCER_FLUSH_TIMEOUT_MS
      PRODUCER_PURGE_FLUSH_TIMEOUT_MS
      METADATA_TIMEOUT_MS
      HANDLE_WAIT_TIMEOUT_MS
      NATIVE_KAFKA_POLL_TIMEOUT_MS
      PRODUCER_PURGE_SLEEP_INTERVAL_MS
      NATIVE_KAFKA_SYNCHRONIZE_SLEEP_INTERVAL_MS
      METADATA_RETRY_BACKOFF_BASE_MS
      METADATA_MAX_RETRIES
      CONSUMER_SEEK_TIMEOUT_MS
      CONSUMER_EVENTS_POLL_TIMEOUT_MS
      PARTITIONS_COUNT_CACHE_TTL_MS
    ]

    expected.each do |const|
      assert_includes constants, const
    end
  end
end
