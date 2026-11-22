# frozen_string_literal: true

describe Rdkafka::Defaults do
  describe "consumer timeouts" do
    it "defines CONSUMER_COMMITTED_TIMEOUT_MS as 2000" do
      assert_equal(2_000, described_class::CONSUMER_COMMITTED_TIMEOUT_MS)
    end

    it "defines CONSUMER_QUERY_WATERMARK_TIMEOUT_MS as 1000" do
      assert_equal(1_000, described_class::CONSUMER_QUERY_WATERMARK_TIMEOUT_MS)
    end

    it "defines CONSUMER_LAG_TIMEOUT_MS as 1000" do
      assert_equal(1_000, described_class::CONSUMER_LAG_TIMEOUT_MS)
    end

    it "defines CONSUMER_OFFSETS_FOR_TIMES_TIMEOUT_MS as 1000" do
      assert_equal(1_000, described_class::CONSUMER_OFFSETS_FOR_TIMES_TIMEOUT_MS)
    end

    it "defines CONSUMER_POLL_TIMEOUT_MS as 250" do
      assert_equal(250, described_class::CONSUMER_POLL_TIMEOUT_MS)
    end
  end

  describe "producer timeouts" do
    it "defines PRODUCER_FLUSH_TIMEOUT_MS as 5000" do
      assert_equal(5_000, described_class::PRODUCER_FLUSH_TIMEOUT_MS)
    end

    it "defines PRODUCER_PURGE_FLUSH_TIMEOUT_MS as 100" do
      assert_equal(100, described_class::PRODUCER_PURGE_FLUSH_TIMEOUT_MS)
    end
  end

  describe "metadata timeouts" do
    it "defines METADATA_TIMEOUT_MS as 2000" do
      assert_equal(2_000, described_class::METADATA_TIMEOUT_MS)
    end
  end

  describe "handle timeouts" do
    it "defines HANDLE_WAIT_TIMEOUT_MS as 60000" do
      assert_equal(60_000, described_class::HANDLE_WAIT_TIMEOUT_MS)
    end
  end

  describe "native kafka polling" do
    it "defines NATIVE_KAFKA_POLL_TIMEOUT_MS as 100" do
      assert_equal(100, described_class::NATIVE_KAFKA_POLL_TIMEOUT_MS)
    end
  end

  describe "internal timing" do
    it "defines PRODUCER_PURGE_SLEEP_INTERVAL_MS as 1" do
      assert_equal(1, described_class::PRODUCER_PURGE_SLEEP_INTERVAL_MS)
    end

    it "defines METADATA_RETRY_BACKOFF_BASE_MS as 100" do
      assert_equal(100, described_class::METADATA_RETRY_BACKOFF_BASE_MS)
    end

    it "defines METADATA_MAX_RETRIES as 10" do
      assert_equal(10, described_class::METADATA_MAX_RETRIES)
    end

    it "defines CONSUMER_SEEK_TIMEOUT_MS as 0" do
      assert_equal(0, described_class::CONSUMER_SEEK_TIMEOUT_MS)
    end

    it "defines CONSUMER_EVENTS_POLL_TIMEOUT_MS as 0" do
      assert_equal(0, described_class::CONSUMER_EVENTS_POLL_TIMEOUT_MS)
    end
  end

  describe "cache settings" do
    it "defines PARTITION_COUNT_CACHE_TTL_MS as 30000" do
      assert_equal(30_000, described_class::PARTITION_COUNT_CACHE_TTL_MS)
    end
  end

  describe "deprecated constants" do
    # Enabled so a `deprecate_constant` warning would show; Ruby 3.3 has no Warning.categories,
    # so the test helper leaves deprecation warnings off
    before do
      @deprecated = Warning[:deprecated]
      Warning[:deprecated] = true
    end

    after { Warning[:deprecated] = @deprecated }

    it "keeps PARTITIONS_COUNT_CACHE_TTL_MS as an alias of PARTITION_COUNT_CACHE_TTL_MS without a warning" do
      Warning.expects(:warn).never

      assert_equal(described_class::PARTITION_COUNT_CACHE_TTL_MS, described_class::PARTITIONS_COUNT_CACHE_TTL_MS)
    end

    it "keeps PARTITIONS_COUNT_CACHE_TTL_MS defined" do
      assert_equal("constant", defined?(described_class::PARTITIONS_COUNT_CACHE_TTL_MS))
      assert_same(true, described_class.const_defined?(:PARTITIONS_COUNT_CACHE_TTL_MS))
      assert_includes(described_class.constants, :PARTITIONS_COUNT_CACHE_TTL_MS)
    end
  end

  describe "immutability" do
    it "all constants are frozen" do
      # Numeric constants are inherently immutable in Ruby, but let's verify
      # that they are properly defined as constants (not class variables)
      constants = described_class.constants

      refute_empty(constants)

      %i[
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
        METADATA_RETRY_BACKOFF_BASE_MS
        METADATA_MAX_RETRIES
        CONSUMER_SEEK_TIMEOUT_MS
        CONSUMER_EVENTS_POLL_TIMEOUT_MS
        PARTITION_COUNT_CACHE_TTL_MS
      ].each do |name|
        assert_includes(constants, name)
      end
    end
  end
end
