# frozen_string_literal: true

describe Rdkafka::Defaults do
  describe "consumer timeouts" do
    it "defines CONSUMER_COMMITTED_TIMEOUT_MS as 2000" do
      assert_equal 2_000, Rdkafka::Defaults::CONSUMER_COMMITTED_TIMEOUT_MS
    end

    it "defines CONSUMER_QUERY_WATERMARK_TIMEOUT_MS as 1000" do
      assert_equal 1_000, Rdkafka::Defaults::CONSUMER_QUERY_WATERMARK_TIMEOUT_MS
    end

    it "defines CONSUMER_LAG_TIMEOUT_MS as 1000" do
      assert_equal 1_000, Rdkafka::Defaults::CONSUMER_LAG_TIMEOUT_MS
    end

    it "defines CONSUMER_OFFSETS_FOR_TIMES_TIMEOUT_MS as 1000" do
      assert_equal 1_000, Rdkafka::Defaults::CONSUMER_OFFSETS_FOR_TIMES_TIMEOUT_MS
    end

    it "defines CONSUMER_POLL_TIMEOUT_MS as 250" do
      assert_equal 250, Rdkafka::Defaults::CONSUMER_POLL_TIMEOUT_MS
    end
  end

  describe "producer timeouts" do
    it "defines PRODUCER_FLUSH_TIMEOUT_MS as 5000" do
      assert_equal 5_000, Rdkafka::Defaults::PRODUCER_FLUSH_TIMEOUT_MS
    end

    it "defines PRODUCER_PURGE_FLUSH_TIMEOUT_MS as 100" do
      assert_equal 100, Rdkafka::Defaults::PRODUCER_PURGE_FLUSH_TIMEOUT_MS
    end
  end

  describe "metadata timeouts" do
    it "defines METADATA_TIMEOUT_MS as 2000" do
      assert_equal 2_000, Rdkafka::Defaults::METADATA_TIMEOUT_MS
    end
  end

  describe "handle timeouts" do
    it "defines HANDLE_WAIT_TIMEOUT_MS as 60000" do
      assert_equal 60_000, Rdkafka::Defaults::HANDLE_WAIT_TIMEOUT_MS
    end
  end

  describe "native kafka polling" do
    it "defines NATIVE_KAFKA_POLL_TIMEOUT_MS as 100" do
      assert_equal 100, Rdkafka::Defaults::NATIVE_KAFKA_POLL_TIMEOUT_MS
    end
  end

  describe "internal timing" do
    it "defines PRODUCER_PURGE_SLEEP_INTERVAL_MS as 1" do
      assert_equal 1, Rdkafka::Defaults::PRODUCER_PURGE_SLEEP_INTERVAL_MS
    end

    it "defines NATIVE_KAFKA_SYNCHRONIZE_SLEEP_INTERVAL_MS as 10" do
      assert_equal 10, Rdkafka::Defaults::NATIVE_KAFKA_SYNCHRONIZE_SLEEP_INTERVAL_MS
    end

    it "defines METADATA_RETRY_BACKOFF_BASE_MS as 100" do
      assert_equal 100, Rdkafka::Defaults::METADATA_RETRY_BACKOFF_BASE_MS
    end

    it "defines METADATA_MAX_RETRIES as 10" do
      assert_equal 10, Rdkafka::Defaults::METADATA_MAX_RETRIES
    end

    it "defines CONSUMER_SEEK_TIMEOUT_MS as 0" do
      assert_equal 0, Rdkafka::Defaults::CONSUMER_SEEK_TIMEOUT_MS
    end

    it "defines CONSUMER_EVENTS_POLL_TIMEOUT_MS as 0" do
      assert_equal 0, Rdkafka::Defaults::CONSUMER_EVENTS_POLL_TIMEOUT_MS
    end
  end

  describe "cache settings" do
    it "defines PARTITIONS_COUNT_CACHE_TTL_MS as 30000" do
      assert_equal 30_000, Rdkafka::Defaults::PARTITIONS_COUNT_CACHE_TTL_MS
    end
  end

  describe "immutability" do
    it "all constants are defined" do
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
end
