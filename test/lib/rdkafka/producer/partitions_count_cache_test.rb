# frozen_string_literal: true

require_relative "../../../test_helper"

describe Rdkafka::Producer::PartitionsCountCache do
  before do
    @default_ttl_ms = 1_000 # Reduced from 30000 to speed up tests
    @custom_ttl_ms = 500    # Half the default TTL
    @cache = described_class.new(ttl_ms: @default_ttl_ms)
    @custom_ttl_cache = described_class.new(ttl_ms: @custom_ttl_ms)
    @topic = TestTopics.unique
    @topic2 = TestTopics.unique
    @partition_count = 5
    @higher_partition_count = 10
    @lower_partition_count = 3
    @even_higher_partition_count = 15
  end

  describe "#initialize" do
    it "creates a cache with default TTL when no TTL is specified" do
      standard_cache = described_class.new
      assert_kind_of described_class, standard_cache
    end

    it "creates a cache with custom TTL when specified" do
      assert_kind_of described_class, @custom_ttl_cache
    end

    context "backwards compatibility with ttl (seconds)" do
      it "works with old ttl parameter (emits deprecation warning to stderr)" do
        # Note: Deprecation warning is emitted but not tested here due to stderr capture complexity
        old_style_cache = described_class.new(1) # 1 second
        assert_kind_of described_class, old_style_cache
      end

      it "converts seconds to milliseconds correctly" do
        old_style_cache = described_class.new(2) # 2 seconds = 2000ms

        # Set a value and verify the TTL behavior
        old_style_cache.set(@topic, @partition_count)

        # Wait 1.5 seconds (should still be valid as TTL is 2 seconds)
        sleep(1.5)
        result = old_style_cache.get(@topic) { fail "Should not be called - cache should still be valid" }
        assert_equal @partition_count, result

        # Wait another 0.7 seconds (total 2.2 seconds, should be expired)
        sleep(0.7)
        block_called = false
        new_result = old_style_cache.get(@topic) do
          block_called = true
          @partition_count + 1
        end
        assert_equal true, block_called
        assert_equal @partition_count + 1, new_result
      end

      it "accepts both ttl and ttl_ms parameters" do
        cache_instance = described_class.new(1, ttl_ms: 1000)
        assert_kind_of described_class, cache_instance
      end

      it "uses ttl_ms when both parameters are provided" do
        # ttl: 10 would be 10000ms, but ttl_ms: 500 should take precedence
        both_params_cache = described_class.new(10, ttl_ms: 500)

        both_params_cache.set(@topic, @partition_count)

        # Wait 0.6 seconds (past 500ms TTL but not past 10 seconds)
        sleep(0.6)

        # Should be expired because ttl_ms: 500 takes precedence
        block_called = false
        both_params_cache.get(@topic) do
          block_called = true
          @partition_count + 1
        end

        assert_equal true, block_called
      end
    end
  end

  describe "#get" do
    context "when cache is empty" do
      it "yields to get the value and caches it" do
        block_called = false
        result = @cache.get(@topic) do
          block_called = true
          @partition_count
        end

        assert_equal true, block_called
        assert_equal @partition_count, result

        # Verify caching by checking if block is called again
        second_block_called = false
        second_result = @cache.get(@topic) do
          second_block_called = true
          @partition_count + 1 # Different value to ensure we get cached value
        end

        assert_equal false, second_block_called
        assert_equal @partition_count, second_result
      end
    end

    context "when cache has a value" do
      before do
        # Seed the cache with a value
        @cache.get(@topic) { @partition_count }
      end

      it "returns cached value without yielding if not expired" do
        block_called = false
        result = @cache.get(@topic) do
          block_called = true
          @partition_count + 1 # Different value to ensure we get cached one
        end

        assert_equal false, block_called
        assert_equal @partition_count, result
      end

      it "yields to get new value when TTL has expired" do
        # Wait for TTL to expire (convert ms to seconds)
        sleep(@default_ttl_ms / 1000.0 + 0.1)

        block_called = false
        new_count = @partition_count + 1
        result = @cache.get(@topic) do
          block_called = true
          new_count
        end

        assert_equal true, block_called
        assert_equal new_count, result

        # Verify the new value is cached
        second_block_called = false
        second_result = @cache.get(@topic) do
          second_block_called = true
          new_count + 1 # Different value again
        end

        assert_equal false, second_block_called
        assert_equal new_count, second_result
      end

      it "respects a custom TTL" do
        # Seed the custom TTL cache with a value
        @custom_ttl_cache.get(@topic) { @partition_count }

        # Wait for custom TTL to expire but not default TTL (convert ms to seconds)
        sleep(@custom_ttl_ms / 1000.0 + 0.1)

        # Custom TTL cache should refresh
        custom_block_called = false
        custom_result = @custom_ttl_cache.get(@topic) do
          custom_block_called = true
          @higher_partition_count
        end

        assert_equal true, custom_block_called
        assert_equal @higher_partition_count, custom_result

        # Default TTL cache should not refresh yet
        default_block_called = false
        default_result = @cache.get(@topic) do
          default_block_called = true
          @higher_partition_count
        end

        assert_equal false, default_block_called
        assert_equal @partition_count, default_result
      end
    end

    context "when new value is obtained" do
      before do
        # Seed the cache with initial value
        @cache.get(@topic) { @partition_count }
      end

      it "updates cache when new value is higher than cached value" do
        # Wait for TTL to expire (convert ms to seconds)
        sleep(@default_ttl_ms / 1000.0 + 0.1)

        # Get higher value
        result = @cache.get(@topic) { @higher_partition_count }
        assert_equal @higher_partition_count, result

        # Verify it was cached
        second_result = @cache.get(@topic) { fail "Should not be called" }
        assert_equal @higher_partition_count, second_result
      end

      it "preserves higher cached value when new value is lower" do
        # First update to higher value (convert ms to seconds)
        sleep(@default_ttl_ms / 1000.0 + 0.1)
        @cache.get(@topic) { @higher_partition_count }

        # Then try to update to lower value (convert ms to seconds)
        sleep(@default_ttl_ms / 1000.0 + 0.1)
        result = @cache.get(@topic) { @lower_partition_count }

        assert_equal @higher_partition_count, result

        # and subsequent gets should return the previously cached higher value
        second_result = @cache.get(@topic) { fail "Should not be called" }
        assert_equal @higher_partition_count, second_result
      end

      it "handles multiple topics independently" do
        # Set up both topics with different values
        @cache.get(@topic) { @partition_count }
        @cache.get(@topic2) { @higher_partition_count }

        # Wait for TTL to expire (convert ms to seconds)
        sleep(@default_ttl_ms / 1000.0 + 0.1)

        # Update first topic
        first_result = @cache.get(@topic) { @even_higher_partition_count }
        assert_equal @even_higher_partition_count, first_result

        # Update second topic independently
        second_updated = @higher_partition_count + 3
        second_result = @cache.get(@topic2) { second_updated }
        assert_equal second_updated, second_result

        # Both topics should have their updated values
        assert_equal @even_higher_partition_count, @cache.get(@topic) { fail "Should not be called" }
        assert_equal second_updated, @cache.get(@topic2) { fail "Should not be called" }
      end
    end
  end

  describe "#set" do
    context "when cache is empty" do
      it "adds a new entry to the cache" do
        @cache.set(@topic, @partition_count)

        # Verify through get
        result = @cache.get(@topic) { fail "Should not be called" }
        assert_equal @partition_count, result
      end
    end

    context "when cache already has a value" do
      before do
        @cache.set(@topic, @partition_count)
      end

      it "updates cache when new value is higher" do
        @cache.set(@topic, @higher_partition_count)

        result = @cache.get(@topic) { fail "Should not be called" }
        assert_equal @higher_partition_count, result
      end

      it "keeps original value when new value is lower" do
        @cache.set(@topic, @lower_partition_count)

        result = @cache.get(@topic) { fail "Should not be called" }
        assert_equal @partition_count, result
      end

      it "updates the timestamp even when keeping original value" do
        # Set initial value
        @cache.set(@topic, @partition_count)

        # Wait until close to TTL expiring (convert ms to seconds)
        sleep(@default_ttl_ms / 1000.0 - 0.2)

        # Set lower value (should update timestamp but not value)
        @cache.set(@topic, @lower_partition_count)

        # Wait a bit more, but still under the full TTL if timestamp was refreshed
        sleep(0.3)

        # Should still be valid due to timestamp refresh
        result = @cache.get(@topic) { fail "Should not be called" }
        assert_equal @partition_count, result
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
            @cache.set(@topic, value)
          end
        end

        # Wait for all threads to complete
        threads.each(&:join)

        # The highest value (14) should be stored and accessible through get
        result = @cache.get(@topic) { fail "Should not be called" }
        assert_equal 14, result
      end
    end
  end

  describe "TTL behavior" do
    it "treats entries as expired when they exceed TTL" do
      # Set initial value
      @cache.get(@topic) { @partition_count }

      # Wait just under TTL (convert ms to seconds)
      sleep(@default_ttl_ms / 1000.0 - 0.2)

      # Value should still be cached (block should not be called)
      result = @cache.get(@topic) { fail "Should not be called when cache is valid" }
      assert_equal @partition_count, result

      # Now wait to exceed TTL
      sleep(0.3) # Total sleep is now default_ttl_ms / 1000.0 + 0.1

      # Cache should be expired, block should be called
      block_called = false
      new_value = @partition_count + 3
      result = @cache.get(@topic) do
        block_called = true
        new_value
      end

      assert_equal true, block_called
      assert_equal new_value, result
    end
  end

  describe "comprehensive scenarios" do
    it "handles a full lifecycle of cache operations" do
      # 1. Initial cache miss, fetch and store
      result1 = @cache.get(@topic) { @partition_count }
      assert_equal @partition_count, result1

      # 2. Cache hit
      result2 = @cache.get(@topic) { fail "Should not be called" }
      assert_equal @partition_count, result2

      # 3. Attempt to set lower value
      @cache.set(@topic, @lower_partition_count)
      result3 = @cache.get(@topic) { fail "Should not be called" }
      # Should still return the higher original value
      assert_equal @partition_count, result3

      # 4. Set higher value
      @cache.set(@topic, @higher_partition_count)
      result4 = @cache.get(@topic) { fail "Should not be called" }
      assert_equal @higher_partition_count, result4

      # 5. TTL expires, new value provided is lower (convert ms to seconds)
      sleep(@default_ttl_ms / 1000.0 + 0.1)
      result5 = @cache.get(@topic) { @lower_partition_count }
      # This returns the highest value
      assert_equal @higher_partition_count, result5

      # 6. But subsequent get should return the higher cached value
      result6 = @cache.get(@topic) { fail "Should not be called" }
      assert_equal @higher_partition_count, result6

      # 7. Set new highest value directly
      even_higher = @higher_partition_count + 5
      @cache.set(@topic, even_higher)
      result7 = @cache.get(@topic) { fail "Should not be called" }
      assert_equal even_higher, result7
    end

    it "handles multiple topics with different TTLs correctly" do
      # Set up initial values
      @cache.get(@topic) { @partition_count }
      @custom_ttl_cache.get(@topic) { @partition_count }

      # Wait past custom TTL but not default TTL (convert ms to seconds)
      sleep(@custom_ttl_ms / 1000.0 + 0.1)

      # Default cache should NOT refresh (still within default TTL)
      default_result = @cache.get(@topic) { fail "Should not be called for default cache" }
      # Original value should be maintained
      assert_equal @partition_count, default_result

      # Custom TTL cache SHOULD refresh (past custom TTL)
      custom_cache_value = @partition_count + 8
      custom_block_called = false
      custom_result = @custom_ttl_cache.get(@topic) do
        custom_block_called = true
        custom_cache_value
      end

      assert_equal true, custom_block_called
      assert_equal custom_cache_value, custom_result

      # Now wait past default TTL (convert ms to seconds)
      sleep((@default_ttl_ms - @custom_ttl_ms) / 1000.0 + 0.1)

      # Now default cache should also refresh
      default_block_called = false
      new_default_value = @partition_count + 10
      new_default_result = @cache.get(@topic) do
        default_block_called = true
        new_default_value
      end

      assert_equal true, default_block_called
      assert_equal new_default_value, new_default_result
    end
  end
end
