# frozen_string_literal: true

describe Rdkafka::Producer::PartitionCountCache do
  before do
    @default_ttl_ms = 1_000 # Reduced from 30000 to speed up tests
    @custom_ttl_ms = 500 # Half the default TTL
    @cache = described_class.new(ttl_ms: @default_ttl_ms)
    @custom_ttl_cache = described_class.new(ttl_ms: @custom_ttl_ms)
    @topic = TestTopics.unique
    @topic2 = TestTopics.unique
    @partition_count = 5
    @higher_partition_count = 10
    @lower_partition_count = 3
    @even_higher_partition_count = 15
  end

  def stub_clock(*caches)
    @now = 0
    @clocked_caches = caches
    tick(0)
  end

  def tick(ms)
    @now += ms
    @clocked_caches.each { |clocked| clocked.stubs(:monotonic_now_ms).returns(@now) }
  end

  describe "#initialize" do
    it "creates a cache with default TTL when no TTL is specified" do
      standard_cache = described_class.new

      assert_kind_of(described_class, standard_cache)
    end

    it "creates a cache with custom TTL when specified" do
      assert_kind_of(described_class, @custom_ttl_cache)
    end

    context "backwards compatibility with ttl (seconds)" do
      it "works with old ttl parameter (emits deprecation warning to stderr)" do
        # The deprecation warning goes to stderr and is not asserted here
        old_style_cache = described_class.new(1) # 1 second

        assert_kind_of(described_class, old_style_cache)
      end

      it "converts seconds to milliseconds correctly" do
        old_style_cache = described_class.new(2) # 2 seconds = 2000ms

        # Set a value and verify the TTL behavior
        old_style_cache.set(@topic, @partition_count)

        # Wait 1.5 seconds (should still be valid as TTL is 2 seconds)
        sleep(1.5)
        result = old_style_cache.get(@topic) { flunk "Should not be called - cache should still be valid" }

        assert_equal(@partition_count, result)

        # Wait another 0.7 seconds (total 2.2 seconds, should be expired)
        sleep(0.7)
        block_called = false
        new_result = old_style_cache.get(@topic) do
          block_called = true
          @partition_count + 1
        end

        assert(block_called)
        assert_equal(@partition_count + 1, new_result)
      end

      it "accepts both ttl and ttl_ms parameters" do
        cache_instance = described_class.new(1, ttl_ms: 1000)

        assert_kind_of(described_class, cache_instance)
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

        assert(block_called)
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

        assert(block_called)
        assert_equal(@partition_count, result)

        # Verify caching by checking if block is called again
        second_block_called = false
        second_result = @cache.get(@topic) do
          second_block_called = true
          @partition_count + 1 # Different value to ensure we get cached value
        end

        refute(second_block_called)
        assert_equal(@partition_count, second_result)
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

        refute(block_called)
        assert_equal(@partition_count, result)
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

        assert(block_called)
        assert_equal(new_count, result)

        # Verify the new value is cached
        second_block_called = false
        second_result = @cache.get(@topic) do
          second_block_called = true
          new_count + 1 # Different value again
        end

        refute(second_block_called)
        assert_equal(new_count, second_result)
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

        assert(custom_block_called)
        assert_equal(@higher_partition_count, custom_result)

        # Default TTL cache should not refresh yet
        default_block_called = false
        default_result = @cache.get(@topic) do
          default_block_called = true
          @higher_partition_count
        end

        refute(default_block_called)
        assert_equal(@partition_count, default_result)
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

        assert_equal(@higher_partition_count, result)

        # Verify it was cached
        second_result = @cache.get(@topic) { flunk "Should not be called" }

        assert_equal(@higher_partition_count, second_result)
      end

      it "adopts a lower value once the cached entry has expired (topic recreated smaller)" do
        # First update to a higher value
        sleep(@default_ttl_ms / 1000.0 + 0.1)
        @cache.get(@topic) { @higher_partition_count }

        # After the TTL expires, an authoritative refresh returns a lower count and it is adopted
        sleep(@default_ttl_ms / 1000.0 + 0.1)
        result = @cache.get(@topic) { @lower_partition_count }

        assert_equal(@lower_partition_count, result)

        # and subsequent gets within the TTL return the adopted lower value
        second_result = @cache.get(@topic) { flunk "Should not be called" }

        assert_equal(@lower_partition_count, second_result)
      end

      it "keeps the higher value while the cached entry is still fresh" do
        sleep(@default_ttl_ms / 1000.0 + 0.1)
        @cache.get(@topic) { @higher_partition_count }

        # Within the TTL window get returns the cached value without invoking the block at all,
        # so a transient/racy lower read cannot displace the higher count.
        result = @cache.get(@topic) { flunk "Should not be called" }

        assert_equal(@higher_partition_count, result)
      end

      it "handles multiple topics independently" do
        # Set up both topics with different values
        @cache.get(@topic) { @partition_count }
        @cache.get(@topic2) { @higher_partition_count }

        # Wait for TTL to expire (convert ms to seconds)
        sleep(@default_ttl_ms / 1000.0 + 0.1)

        # Update first topic
        first_result = @cache.get(@topic) { @even_higher_partition_count }

        assert_equal(@even_higher_partition_count, first_result)

        # Update second topic independently
        second_updated = @higher_partition_count + 3
        second_result = @cache.get(@topic2) { second_updated }

        assert_equal(second_updated, second_result)

        # Both topics should have their updated values
        assert_equal(@even_higher_partition_count, @cache.get(@topic) { flunk "Should not be called" })
        assert_equal(second_updated, @cache.get(@topic2) { flunk "Should not be called" })
      end
    end
  end

  describe "#ttl_ms=" do
    before do
      stub_clock(@cache)
      @cache.get(@topic) { @partition_count }
    end

    it "exposes the ttl" do
      assert_equal(@default_ttl_ms, @cache.ttl_ms)
    end

    it "keeps cached entries and expires them with the new ttl" do
      @cache.ttl_ms = @default_ttl_ms * 2
      tick(@default_ttl_ms + 1)

      assert_equal(@partition_count, @cache.get(@topic) { flunk "Should not be called" })

      tick(@default_ttl_ms)

      assert_equal(@higher_partition_count, @cache.get(@topic) { @higher_partition_count })
    end

    it "expires entries sooner with a shorter ttl" do
      @cache.ttl_ms = @default_ttl_ms / 2
      tick((@default_ttl_ms / 2) + 1)

      assert_equal(@higher_partition_count, @cache.get(@topic) { @higher_partition_count })
    end

    it "applies to an entry served stale after a failed refresh" do
      stale_backoff_ms = Rdkafka::Defaults::PARTITION_COUNT_CACHE_STALE_BACKOFF_MS
      tick(@default_ttl_ms + 1)
      @cache.get(@topic) { raise Rdkafka::RdkafkaError.new(-195) }

      @cache.ttl_ms = @default_ttl_ms * 2
      tick(stale_backoff_ms + @default_ttl_ms)

      assert_equal(@partition_count, @cache.get(@topic) { flunk "Should not be called" })

      tick(1)

      assert_equal(@higher_partition_count, @cache.get(@topic) { @higher_partition_count })
    end

    [0, -1, 1.5, "1000", nil].each do |value|
      it "rejects #{value.inspect}" do
        assert_raises(ArgumentError) { @cache.ttl_ms = value }
        assert_equal(@default_ttl_ms, @cache.ttl_ms)
      end
    end
  end

  describe "#get when the refresh fails" do
    before do
      @transport_error = Rdkafka::RdkafkaError.new(-195)
      @stale_backoff_ms = Rdkafka::Defaults::PARTITION_COUNT_CACHE_STALE_BACKOFF_MS
      stub_clock(@cache)
      @cache.get(@topic) { @partition_count }
      tick(@default_ttl_ms + 1)
    end

    {
      transport: -195,
      timed_out: -185,
      all_brokers_down: -187,
      resolve: -193
    }.each do |code_name, code|
      it "serves the last known count on #{code_name}" do
        error = Rdkafka::RdkafkaError.new(code)

        assert_equal(code_name, error.code)
        assert_equal(@partition_count, @cache.get(@topic) { raise error })
      end
    end

    it "serves the known count to other callers while the refresh runs" do
      nested = nil

      @cache.get(@topic) do
        nested = @cache.get(@topic) { flunk "Should not be called" }
        raise @transport_error
      end

      assert_equal(@partition_count, nested)
    end

    it "keeps the full TTL of a write that lands during a failing refresh" do
      long_ttl_ms = @stale_backoff_ms * 3
      long_ttl_cache = described_class.new(ttl_ms: long_ttl_ms)
      @clocked_caches << long_ttl_cache
      tick(0)
      long_ttl_cache.get(@topic) { @partition_count }
      tick(long_ttl_ms + 1)

      long_ttl_cache.get(@topic) do
        long_ttl_cache.set(@topic, @partition_count)
        raise @transport_error
      end

      tick(@stale_backoff_ms + 1)

      assert_equal(@partition_count, long_ttl_cache.get(@topic) { flunk "Should not be called" })
    end

    it "refreshes again on the next call after a non-transient error" do
      error = Rdkafka::RdkafkaError.new(29) # topic_authorization_failed

      raised = assert_raises(Rdkafka::RdkafkaError) { @cache.get(@topic) { raise error } }

      assert_equal(error, raised)
      assert_equal(@higher_partition_count, @cache.get(@topic) { @higher_partition_count })
    end

    it "refreshes again on the next call after a non-Rdkafka error" do
      assert_raises(Timeout::Error) { @cache.get(@topic) { raise Timeout::Error } }
      assert_equal(@higher_partition_count, @cache.get(@topic) { @higher_partition_count })
    end

    it "keeps serving the known count while a refresh outlasts the TTL" do
      nested = nil

      @cache.get(@topic) do
        tick(@default_ttl_ms + 1)
        nested = @cache.get(@topic) { flunk "Should not be called" }
        @partition_count
      end

      assert_equal(@partition_count, nested)
    end

    it "adopts a lower count from a successful refresh" do
      assert_equal(@lower_partition_count, @cache.get(@topic) { @lower_partition_count })
    end

    it "does not refresh again until the stale backoff passes" do
      @cache.get(@topic) { raise @transport_error }

      tick(@stale_backoff_ms)

      assert_equal(@partition_count, @cache.get(@topic) { flunk "Should not be called" })

      tick(1)

      assert_equal(@higher_partition_count, @cache.get(@topic) { @higher_partition_count })
    end

    it "raises when there is no known count" do
      raised = assert_raises(Rdkafka::RdkafkaError) { @cache.get(@topic2) { raise @transport_error } }

      assert_equal(@transport_error, raised)
    end

    it "raises on a non-transient error" do
      error = Rdkafka::RdkafkaError.new(29) # topic_authorization_failed

      raised = assert_raises(Rdkafka::RdkafkaError) { @cache.get(@topic) { raise error } }

      assert_equal(error, raised)
    end
  end

  describe "#set" do
    context "when cache is empty" do
      it "adds a new entry to the cache" do
        @cache.set(@topic, @partition_count)

        # Verify through get
        result = @cache.get(@topic) { flunk "Should not be called" }

        assert_equal(@partition_count, result)
      end
    end

    context "when cache already has a value" do
      before do
        @cache.set(@topic, @partition_count)
      end

      it "updates cache when new value is higher" do
        @cache.set(@topic, @higher_partition_count)

        result = @cache.get(@topic) { flunk "Should not be called" }

        assert_equal(@higher_partition_count, result)
      end

      it "keeps original value when new value is lower" do
        @cache.set(@topic, @lower_partition_count)

        result = @cache.get(@topic) { flunk "Should not be called" }

        assert_equal(@partition_count, result)
      end

      it "adopts a lower value once the entry has expired" do
        # Within the TTL the lower value is ignored (above); once expired it is authoritative.
        sleep(@default_ttl_ms / 1000.0 + 0.1)
        @cache.set(@topic, @lower_partition_count)

        result = @cache.get(@topic) { flunk "Should not be called" }

        assert_equal(@lower_partition_count, result)
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
        result = @cache.get(@topic) { flunk "Should not be called" }

        assert_equal(@partition_count, result)
      end
    end

    context "with concurrent access" do
      it "correctly handles simultaneous updates to the same topic" do
        # This test focuses on the final value after concurrent updates
        threads = []

        # Create 5 threads that all try to update the same topic with increasing values
        5.times do |i|
          threads << Thread.new do
            value = 10 + i # Start at 10 to ensure all are higher than initial value
            @cache.set(@topic, value)
          end
        end

        # Wait for all threads to complete
        threads.each(&:join)

        # The highest value (14) should be stored and accessible through get
        result = @cache.get(@topic) { flunk "Should not be called" }

        assert_equal(14, result)
      end
    end
  end

  describe "concurrent access" do
    it "stays consistent when many threads set and get distinct and shared topics" do
      threads_count = 32
      topics_per_thread = 500
      shared_topics = Array.new(32) { |i| "shared-#{i}" }
      start = Queue.new
      shared_misses = Queue.new

      shared_topics.each_with_index { |shared_topic, i| @cache.set(shared_topic, i + 1) }

      threads = Array.new(threads_count) do |t|
        Thread.new do
          start.pop

          topics_per_thread.times do |i|
            shared_index = i % shared_topics.size

            @cache.get("own-#{t}-#{i}") { i + 1 }
            @cache.set(shared_topics[shared_index], shared_index + 1)

            # Shared topics are cached before the threads start, so a miss here is a lost read
            shared_topics.each_with_index do |shared_topic, j|
              @cache.get(shared_topic) do
                shared_misses << shared_topic
                j + 1
              end
            end
          end
        end
      end

      threads_count.times { start << true }
      threads.each(&:value)

      counts = @cache.to_h

      assert_equal(0, shared_misses.size)

      assert_equal(threads_count * topics_per_thread + shared_topics.size, counts.size)

      threads_count.times do |t|
        topics_per_thread.times do |i|
          assert_equal(i + 1, counts["own-#{t}-#{i}"][1])
        end
      end

      shared_topics.each_with_index do |shared_topic, i|
        assert_equal(i + 1, counts[shared_topic][1])
      end
    end

    {
      "get" => ->(cache, topic_name) { cache.get(topic_name) { -1 } },
      "set" => ->(cache, topic_name) { cache.set(topic_name, 1) }
    }.each do |method_name, call|
      it "stays usable when a Timeout interrupts #{method_name} in many threads" do
        if defined?(JRUBY_VERSION) && Gem::Version.new(JRUBY_VERSION) < Gem::Version.new("10.0.8.0")
          skip "Mutex#synchronize can leave the lock held on an interrupt before JRuby 10.0.8.0"
        end

        long_ttl_cache = described_class.new(ttl_ms: 600_000)
        topics = Array.new(100) { |i| "interrupted-#{i}" }
        deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 1
        errors = Queue.new

        topics.each { |topic_name| long_ttl_cache.set(topic_name, 1) }

        workers = Array.new(4) do
          Thread.new do
            while Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
              begin
                Timeout.timeout(0.00001) do
                  1_000.times { |i| call.call(long_ttl_cache, topics[i % topics.size]) }
                end
              rescue Timeout::Error
                nil
              end
            end
          rescue => e
            errors << e
          end
        end

        assert_predicate(workers.map { |worker| worker.join(5) }, :all?)
        assert_equal(0, errors.size)
        assert(Thread.new { call.call(long_ttl_cache, topics.first) }.join(1))
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
      result = @cache.get(@topic) { flunk "Should not be called when cache is valid" }

      assert_equal(@partition_count, result)

      # Now wait to exceed TTL
      sleep(0.3) # Total sleep is now default_ttl_ms / 1000.0 + 0.1

      # Cache should be expired, block should be called
      block_called = false
      new_value = @partition_count + 3
      result = @cache.get(@topic) do
        block_called = true
        new_value
      end

      assert(block_called)
      assert_equal(new_value, result)
    end
  end

  describe "comprehensive scenarios" do
    it "handles a full lifecycle of cache operations" do
      # 1. Initial cache miss, fetch and store
      result1 = @cache.get(@topic) { @partition_count }

      assert_equal(@partition_count, result1)

      # 2. Cache hit
      result2 = @cache.get(@topic) { flunk "Should not be called" }

      assert_equal(@partition_count, result2)

      # 3. Attempt to set lower value
      @cache.set(@topic, @lower_partition_count)
      result3 = @cache.get(@topic) { flunk "Should not be called" }

      # Should still return the higher original value
      assert_equal(@partition_count, result3)

      # 4. Set higher value
      @cache.set(@topic, @higher_partition_count)
      result4 = @cache.get(@topic) { flunk "Should not be called" }

      assert_equal(@higher_partition_count, result4)

      # 5. TTL expires, the authoritative value provided is now lower; it is adopted
      sleep(@default_ttl_ms / 1000.0 + 0.1)
      result5 = @cache.get(@topic) { @lower_partition_count }

      # After expiry a lower fresh value wins (e.g. the topic was recreated smaller)
      assert_equal(@lower_partition_count, result5)

      # 6. Subsequent get within the TTL returns the adopted lower value
      result6 = @cache.get(@topic) { flunk "Should not be called" }

      assert_equal(@lower_partition_count, result6)

      # 7. Set new highest value directly
      even_higher = @higher_partition_count + 5
      @cache.set(@topic, even_higher)
      result7 = @cache.get(@topic) { flunk "Should not be called" }

      assert_equal(even_higher, result7)
    end

    it "handles multiple topics with different TTLs correctly" do
      # Drive both caches from one controlled clock, so a runner stall cannot expire an entry
      stub_clock(@cache, @custom_ttl_cache)

      # Set up initial values
      @cache.get(@topic) { @partition_count }
      @custom_ttl_cache.get(@topic) { @partition_count }

      # Move past custom TTL but not default TTL
      tick(@custom_ttl_ms + 100)

      # Default cache should NOT refresh (still within default TTL)
      default_result = @cache.get(@topic) { flunk "Should not be called for default cache" }

      # Original value should be maintained
      assert_equal(@partition_count, default_result)

      # Custom TTL cache SHOULD refresh (past custom TTL)
      custom_cache_value = @partition_count + 8
      custom_block_called = false
      custom_result = @custom_ttl_cache.get(@topic) do
        custom_block_called = true
        custom_cache_value
      end

      assert(custom_block_called)
      assert_equal(custom_cache_value, custom_result)

      # Now move past default TTL
      tick(@default_ttl_ms - @custom_ttl_ms + 100)

      # Now default cache should also refresh
      default_block_called = false
      new_default_value = @partition_count + 10
      new_default_result = @cache.get(@topic) do
        default_block_called = true
        new_default_value
      end

      assert(default_block_called)
      assert_equal(new_default_value, new_default_result)
    end
  end
end
