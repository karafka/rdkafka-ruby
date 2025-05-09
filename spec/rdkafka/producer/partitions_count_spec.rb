# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Rdkafka::Producer::PartitionsCountCache do
  let(:default_ttl) { 1 } # Reduced from 30 to speed up tests
  let(:custom_ttl) { 0.5 } # Half the default TTL
  let(:cache) { described_class.new(default_ttl) }
  let(:custom_ttl_cache) { described_class.new(custom_ttl) }
  let(:topic) { "test_topic" }
  let(:topic2) { "test_topic2" }
  let(:partition_count) { 5 }
  let(:higher_partition_count) { 10 }
  let(:lower_partition_count) { 3 }
  let(:even_higher_partition_count) { 15 }

  describe "#initialize" do
    it "creates a cache with default TTL when no TTL is specified" do
      standard_cache = described_class.new
      expect(standard_cache).to be_a(described_class)
    end

    it "creates a cache with custom TTL when specified" do
      expect(custom_ttl_cache).to be_a(described_class)
    end
  end

  describe "#get" do
    context "when cache is empty" do
      it "yields to get the value and caches it" do
        block_called = false
        result = cache.get(topic) do
          block_called = true
          partition_count
        end

        expect(block_called).to be true
        expect(result).to eq(partition_count)

        # Verify caching by checking if block is called again
        second_block_called = false
        second_result = cache.get(topic) do
          second_block_called = true
          partition_count + 1 # Different value to ensure we get cached value
        end

        expect(second_block_called).to be false
        expect(second_result).to eq(partition_count)
      end
    end

    context "when cache has a value" do
      before do
        # Seed the cache with a value
        cache.get(topic) { partition_count }
      end

      it "returns cached value without yielding if not expired" do
        block_called = false
        result = cache.get(topic) do
          block_called = true
          partition_count + 1 # Different value to ensure we get cached one
        end

        expect(block_called).to be false
        expect(result).to eq(partition_count)
      end

      it "yields to get new value when TTL has expired" do
        # Wait for TTL to expire
        sleep(default_ttl + 0.1)

        block_called = false
        new_count = partition_count + 1
        result = cache.get(topic) do
          block_called = true
          new_count
        end

        expect(block_called).to be true
        expect(result).to eq(new_count)

        # Verify the new value is cached
        second_block_called = false
        second_result = cache.get(topic) do
          second_block_called = true
          new_count + 1 # Different value again
        end

        expect(second_block_called).to be false
        expect(second_result).to eq(new_count)
      end

      it "respects a custom TTL" do
        # Seed the custom TTL cache with a value
        custom_ttl_cache.get(topic) { partition_count }

        # Wait for custom TTL to expire but not default TTL
        sleep(custom_ttl + 0.1)

        # Custom TTL cache should refresh
        custom_block_called = false
        custom_result = custom_ttl_cache.get(topic) do
          custom_block_called = true
          higher_partition_count
        end

        expect(custom_block_called).to be true
        expect(custom_result).to eq(higher_partition_count)

        # Default TTL cache should not refresh yet
        default_block_called = false
        default_result = cache.get(topic) do
          default_block_called = true
          higher_partition_count
        end

        expect(default_block_called).to be false
        expect(default_result).to eq(partition_count)
      end
    end

    context "when new value is obtained" do
      before do
        # Seed the cache with initial value
        cache.get(topic) { partition_count }
      end

      it "updates cache when new value is higher than cached value" do
        # Wait for TTL to expire
        sleep(default_ttl + 0.1)

        # Get higher value
        result = cache.get(topic) { higher_partition_count }
        expect(result).to eq(higher_partition_count)

        # Verify it was cached
        second_result = cache.get(topic) { fail "Should not be called" }
        expect(second_result).to eq(higher_partition_count)
      end

      it "preserves higher cached value when new value is lower" do
        # First update to higher value
        sleep(default_ttl + 0.1)
        cache.get(topic) { higher_partition_count }

        # Then try to update to lower value
        sleep(default_ttl + 0.1)
        result = cache.get(topic) { lower_partition_count }

        expect(result).to eq(higher_partition_count)

        # and subsequent gets should return the previously cached higher value
        second_result = cache.get(topic) { fail "Should not be called" }
        expect(second_result).to eq(higher_partition_count)
      end

      it "handles multiple topics independently" do
        # Set up both topics with different values
        cache.get(topic) { partition_count }
        cache.get(topic2) { higher_partition_count }

        # Wait for TTL to expire
        sleep(default_ttl + 0.1)

        # Update first topic
        first_result = cache.get(topic) { even_higher_partition_count }
        expect(first_result).to eq(even_higher_partition_count)

        # Update second topic independently
        second_updated = higher_partition_count + 3
        second_result = cache.get(topic2) { second_updated }
        expect(second_result).to eq(second_updated)

        # Both topics should have their updated values
        expect(cache.get(topic) { fail "Should not be called" }).to eq(even_higher_partition_count)
        expect(cache.get(topic2) { fail "Should not be called" }).to eq(second_updated)
      end
    end
  end

  describe "#set" do
    context "when cache is empty" do
      it "adds a new entry to the cache" do
        cache.set(topic, partition_count)

        # Verify through get
        result = cache.get(topic) { fail "Should not be called" }
        expect(result).to eq(partition_count)
      end
    end

    context "when cache already has a value" do
      before do
        cache.set(topic, partition_count)
      end

      it "updates cache when new value is higher" do
        cache.set(topic, higher_partition_count)

        result = cache.get(topic) { fail "Should not be called" }
        expect(result).to eq(higher_partition_count)
      end

      it "keeps original value when new value is lower" do
        cache.set(topic, lower_partition_count)

        result = cache.get(topic) { fail "Should not be called" }
        expect(result).to eq(partition_count)
      end

      it "updates the timestamp even when keeping original value" do
        # Set initial value
        cache.set(topic, partition_count)

        # Wait until close to TTL expiring
        sleep(default_ttl - 0.2)

        # Set lower value (should update timestamp but not value)
        cache.set(topic, lower_partition_count)

        # Wait a bit more, but still under the full TTL if timestamp was refreshed
        sleep(0.3)

        # Should still be valid due to timestamp refresh
        result = cache.get(topic) { fail "Should not be called" }
        expect(result).to eq(partition_count)
      end
    end

    context "with concurrent access" do
      it "correctly handles simultaneous updates to the same topic" do
        # This test focuses on the final value after concurrent updates
        threads = []

        # Create 5 threads that all try to update the same topic with increasing values
        5.times do |i|
          threads << Thread.new do
            value = 10 + i  # Start at 10 to ensure all are higher than initial value
            cache.set(topic, value)
          end
        end

        # Wait for all threads to complete
        threads.each(&:join)

        # The highest value (14) should be stored and accessible through get
        result = cache.get(topic) { fail "Should not be called" }
        expect(result).to eq(14)
      end
    end
  end

  describe "TTL behavior" do
    it "treats entries as expired when they exceed TTL" do
      # Set initial value
      cache.get(topic) { partition_count }

      # Wait just under TTL
      sleep(default_ttl - 0.1)

      # Value should still be cached (block should not be called)
      result = cache.get(topic) { fail "Should not be called when cache is valid" }
      expect(result).to eq(partition_count)

      # Now wait to exceed TTL
      sleep(0.2) # Total sleep is now default_ttl + 0.1

      # Cache should be expired, block should be called
      block_called = false
      new_value = partition_count + 3
      result = cache.get(topic) do
        block_called = true
        new_value
      end

      expect(block_called).to be true
      expect(result).to eq(new_value)
    end
  end

  describe "comprehensive scenarios" do
    it "handles a full lifecycle of cache operations" do
      # 1. Initial cache miss, fetch and store
      result1 = cache.get(topic) { partition_count }
      expect(result1).to eq(partition_count)

      # 2. Cache hit
      result2 = cache.get(topic) { fail "Should not be called" }
      expect(result2).to eq(partition_count)

      # 3. Attempt to set lower value
      cache.set(topic, lower_partition_count)
      result3 = cache.get(topic) { fail "Should not be called" }
      # Should still return the higher original value
      expect(result3).to eq(partition_count)

      # 4. Set higher value
      cache.set(topic, higher_partition_count)
      result4 = cache.get(topic) { fail "Should not be called" }
      expect(result4).to eq(higher_partition_count)

      # 5. TTL expires, new value provided is lower
      sleep(default_ttl + 0.1)
      result5 = cache.get(topic) { lower_partition_count }
      # This returns the highest value
      expect(result5).to eq(higher_partition_count)

      # 6. But subsequent get should return the higher cached value
      result6 = cache.get(topic) { fail "Should not be called" }
      expect(result6).to eq(higher_partition_count)

      # 7. Set new highest value directly
      even_higher = higher_partition_count + 5
      cache.set(topic, even_higher)
      result7 = cache.get(topic) { fail "Should not be called" }
      expect(result7).to eq(even_higher)
    end

    it "handles multiple topics with different TTLs correctly" do
      # Set up initial values
      cache.get(topic) { partition_count }
      custom_ttl_cache.get(topic) { partition_count }

      # Wait past custom TTL but not default TTL
      sleep(custom_ttl + 0.1)

      # Default cache should NOT refresh (still within default TTL)
      default_result = cache.get(topic) { fail "Should not be called for default cache" }
      # Original value should be maintained
      expect(default_result).to eq(partition_count)

      # Custom TTL cache SHOULD refresh (past custom TTL)
      custom_cache_value = partition_count + 8
      custom_block_called = false
      custom_result = custom_ttl_cache.get(topic) do
        custom_block_called = true
        custom_cache_value
      end

      expect(custom_block_called).to be true
      expect(custom_result).to eq(custom_cache_value)

      # Now wait past default TTL
      sleep(default_ttl - custom_ttl + 0.1)

      # Now default cache should also refresh
      default_block_called = false
      new_default_value = partition_count + 10
      new_default_result = cache.get(topic) do
        default_block_called = true
        new_default_value
      end

      expect(default_block_called).to be true
      expect(new_default_result).to eq(new_default_value)
    end
  end
end
