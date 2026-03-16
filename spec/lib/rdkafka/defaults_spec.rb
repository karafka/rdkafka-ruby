# frozen_string_literal: true

RSpec.describe Rdkafka::Defaults do
  describe "consumer timeouts" do
    it "defines CONSUMER_COMMITTED_TIMEOUT_MS as 2000" do
      expect(described_class::CONSUMER_COMMITTED_TIMEOUT_MS).to eq(2_000)
    end

    it "defines CONSUMER_QUERY_WATERMARK_TIMEOUT_MS as 1000" do
      expect(described_class::CONSUMER_QUERY_WATERMARK_TIMEOUT_MS).to eq(1_000)
    end

    it "defines CONSUMER_LAG_TIMEOUT_MS as 1000" do
      expect(described_class::CONSUMER_LAG_TIMEOUT_MS).to eq(1_000)
    end

    it "defines CONSUMER_OFFSETS_FOR_TIMES_TIMEOUT_MS as 1000" do
      expect(described_class::CONSUMER_OFFSETS_FOR_TIMES_TIMEOUT_MS).to eq(1_000)
    end

    it "defines CONSUMER_POLL_TIMEOUT_MS as 250" do
      expect(described_class::CONSUMER_POLL_TIMEOUT_MS).to eq(250)
    end
  end

  describe "producer timeouts" do
    it "defines PRODUCER_FLUSH_TIMEOUT_MS as 5000" do
      expect(described_class::PRODUCER_FLUSH_TIMEOUT_MS).to eq(5_000)
    end

    it "defines PRODUCER_PURGE_FLUSH_TIMEOUT_MS as 100" do
      expect(described_class::PRODUCER_PURGE_FLUSH_TIMEOUT_MS).to eq(100)
    end
  end

  describe "metadata timeouts" do
    it "defines METADATA_TIMEOUT_MS as 2000" do
      expect(described_class::METADATA_TIMEOUT_MS).to eq(2_000)
    end
  end

  describe "handle timeouts" do
    it "defines HANDLE_WAIT_TIMEOUT_MS as 60000" do
      expect(described_class::HANDLE_WAIT_TIMEOUT_MS).to eq(60_000)
    end
  end

  describe "native kafka polling" do
    it "defines NATIVE_KAFKA_POLL_TIMEOUT_MS as 100" do
      expect(described_class::NATIVE_KAFKA_POLL_TIMEOUT_MS).to eq(100)
    end
  end

  describe "internal timing" do
    it "defines PRODUCER_PURGE_SLEEP_INTERVAL_MS as 1" do
      expect(described_class::PRODUCER_PURGE_SLEEP_INTERVAL_MS).to eq(1)
    end

    it "defines NATIVE_KAFKA_SYNCHRONIZE_SLEEP_INTERVAL_MS as 10" do
      expect(described_class::NATIVE_KAFKA_SYNCHRONIZE_SLEEP_INTERVAL_MS).to eq(10)
    end

    it "defines METADATA_RETRY_BACKOFF_BASE_MS as 100" do
      expect(described_class::METADATA_RETRY_BACKOFF_BASE_MS).to eq(100)
    end

    it "defines METADATA_MAX_RETRIES as 10" do
      expect(described_class::METADATA_MAX_RETRIES).to eq(10)
    end

    it "defines CONSUMER_SEEK_TIMEOUT_MS as 0" do
      expect(described_class::CONSUMER_SEEK_TIMEOUT_MS).to eq(0)
    end

    it "defines CONSUMER_EVENTS_POLL_TIMEOUT_MS as 0" do
      expect(described_class::CONSUMER_EVENTS_POLL_TIMEOUT_MS).to eq(0)
    end
  end

  describe "cache settings" do
    it "defines PARTITIONS_COUNT_CACHE_TTL_MS as 30000" do
      expect(described_class::PARTITIONS_COUNT_CACHE_TTL_MS).to eq(30_000)
    end
  end

  describe "immutability" do
    it "all constants are frozen" do
      # Numeric constants are inherently immutable in Ruby, but let's verify
      # that they are properly defined as constants (not class variables)
      constants = described_class.constants
      expect(constants).not_to be_empty
      expect(constants).to include(
        :CONSUMER_COMMITTED_TIMEOUT_MS,
        :CONSUMER_QUERY_WATERMARK_TIMEOUT_MS,
        :CONSUMER_LAG_TIMEOUT_MS,
        :CONSUMER_OFFSETS_FOR_TIMES_TIMEOUT_MS,
        :CONSUMER_POLL_TIMEOUT_MS,
        :PRODUCER_FLUSH_TIMEOUT_MS,
        :PRODUCER_PURGE_FLUSH_TIMEOUT_MS,
        :METADATA_TIMEOUT_MS,
        :HANDLE_WAIT_TIMEOUT_MS,
        :NATIVE_KAFKA_POLL_TIMEOUT_MS,
        :PRODUCER_PURGE_SLEEP_INTERVAL_MS,
        :NATIVE_KAFKA_SYNCHRONIZE_SLEEP_INTERVAL_MS,
        :METADATA_RETRY_BACKOFF_BASE_MS,
        :METADATA_MAX_RETRIES,
        :CONSUMER_SEEK_TIMEOUT_MS,
        :CONSUMER_EVENTS_POLL_TIMEOUT_MS,
        :PARTITIONS_COUNT_CACHE_TTL_MS
      )
    end
  end
end
