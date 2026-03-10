# frozen_string_literal: true

describe Rdkafka::Producer::PartitionsCountCache do
  let(:default_ttl_ms) { 1_000 }
  let(:custom_ttl_ms) { 500 }
  let(:cache) { Rdkafka::Producer::PartitionsCountCache.new(ttl_ms: default_ttl_ms) }
  let(:custom_ttl_cache) { Rdkafka::Producer::PartitionsCountCache.new(ttl_ms: custom_ttl_ms) }
  let(:topic) { TestTopics.unique }
  let(:topic2) { TestTopics.unique }
  let(:partition_count) { 5 }
  let(:higher_partition_count) { 10 }
  let(:lower_partition_count) { 3 }
  let(:even_higher_partition_count) { 15 }

  describe "#initialize" do
    it "creates a cache with default TTL when no TTL is specified" do
      standard_cache = Rdkafka::Producer::PartitionsCountCache.new

      assert_kind_of Rdkafka::Producer::PartitionsCountCache, standard_cache
    end

    it "creates a cache with custom TTL when specified" do
      assert_kind_of Rdkafka::Producer::PartitionsCountCache, custom_ttl_cache
    end

    describe "backwards compatibility with ttl (seconds)" do
      it "works with old ttl parameter" do
        old_style_cache = Rdkafka::Producer::PartitionsCountCache.new(1)

        assert_kind_of Rdkafka::Producer::PartitionsCountCache, old_style_cache
      end

      it "converts seconds to milliseconds correctly" do
        old_style_cache = Rdkafka::Producer::PartitionsCountCache.new(2)

        old_style_cache.set(topic, partition_count)

        sleep(1.5)
        result = old_style_cache.get(topic) { raise "Should not be called - cache should still be valid" }

        assert_equal partition_count, result

        sleep(0.7)
        block_called = false
        new_result = old_style_cache.get(topic) do
          block_called = true
          partition_count + 1
        end

        assert block_called
        assert_equal partition_count + 1, new_result
      end

      it "accepts both ttl and ttl_ms parameters" do
        cache_instance = Rdkafka::Producer::PartitionsCountCache.new(1, ttl_ms: 1000)

        assert_kind_of Rdkafka::Producer::PartitionsCountCache, cache_instance
      end

      it "uses ttl_ms when both parameters are provided" do
        both_params_cache = Rdkafka::Producer::PartitionsCountCache.new(10, ttl_ms: 500)

        both_params_cache.set(topic, partition_count)

        sleep(0.6)

        block_called = false
        both_params_cache.get(topic) do
          block_called = true
          partition_count + 1
        end

        assert block_called
      end
    end
  end

  describe "#get" do
    describe "when cache is empty" do
      it "yields to get the value and caches it" do
        block_called = false
        result = cache.get(topic) do
          block_called = true
          partition_count
        end

        assert block_called
        assert_equal partition_count, result

        second_block_called = false
        second_result = cache.get(topic) do
          second_block_called = true
          partition_count + 1
        end

        refute second_block_called
        assert_equal partition_count, second_result
      end
    end

    describe "when cache has a value" do
      before do
        cache.get(topic) { partition_count }
      end

      it "returns cached value without yielding if not expired" do
        block_called = false
        result = cache.get(topic) do
          block_called = true
          partition_count + 1
        end

        refute block_called
        assert_equal partition_count, result
      end

      it "yields to get new value when TTL has expired" do
        sleep(default_ttl_ms / 1000.0 + 0.1)

        block_called = false
        new_count = partition_count + 1
        result = cache.get(topic) do
          block_called = true
          new_count
        end

        assert block_called
        assert_equal new_count, result

        second_block_called = false
        second_result = cache.get(topic) do
          second_block_called = true
          new_count + 1
        end

        refute second_block_called
        assert_equal new_count, second_result
      end

      it "respects a custom TTL" do
        custom_ttl_cache.get(topic) { partition_count }

        sleep(custom_ttl_ms / 1000.0 + 0.1)

        custom_block_called = false
        custom_result = custom_ttl_cache.get(topic) do
          custom_block_called = true
          higher_partition_count
        end

        assert custom_block_called
        assert_equal higher_partition_count, custom_result

        default_block_called = false
        default_result = cache.get(topic) do
          default_block_called = true
          higher_partition_count
        end

        refute default_block_called
        assert_equal partition_count, default_result
      end
    end

    describe "when new value is obtained" do
      before do
        cache.get(topic) { partition_count }
      end

      it "updates cache when new value is higher than cached value" do
        sleep(default_ttl_ms / 1000.0 + 0.1)

        result = cache.get(topic) { higher_partition_count }

        assert_equal higher_partition_count, result

        second_result = cache.get(topic) { raise "Should not be called" }

        assert_equal higher_partition_count, second_result
      end

      it "preserves higher cached value when new value is lower" do
        sleep(default_ttl_ms / 1000.0 + 0.1)
        cache.get(topic) { higher_partition_count }

        sleep(default_ttl_ms / 1000.0 + 0.1)
        result = cache.get(topic) { lower_partition_count }

        assert_equal higher_partition_count, result

        second_result = cache.get(topic) { raise "Should not be called" }

        assert_equal higher_partition_count, second_result
      end

      it "handles multiple topics independently" do
        cache.get(topic) { partition_count }
        cache.get(topic2) { higher_partition_count }

        sleep(default_ttl_ms / 1000.0 + 0.1)

        first_result = cache.get(topic) { even_higher_partition_count }

        assert_equal even_higher_partition_count, first_result

        second_updated = higher_partition_count + 3
        second_result = cache.get(topic2) { second_updated }

        assert_equal second_updated, second_result

        assert_equal even_higher_partition_count, cache.get(topic) { raise "Should not be called" }
        assert_equal second_updated, cache.get(topic2) { raise "Should not be called" }
      end
    end
  end

  describe "#set" do
    describe "when cache is empty" do
      it "adds a new entry to the cache" do
        cache.set(topic, partition_count)

        result = cache.get(topic) { raise "Should not be called" }

        assert_equal partition_count, result
      end
    end

    describe "when cache already has a value" do
      before do
        cache.set(topic, partition_count)
      end

      it "updates cache when new value is higher" do
        cache.set(topic, higher_partition_count)

        result = cache.get(topic) { raise "Should not be called" }

        assert_equal higher_partition_count, result
      end

      it "keeps original value when new value is lower" do
        cache.set(topic, lower_partition_count)

        result = cache.get(topic) { raise "Should not be called" }

        assert_equal partition_count, result
      end

      it "updates the timestamp even when keeping original value" do
        cache.set(topic, partition_count)

        sleep(default_ttl_ms / 1000.0 - 0.2)

        cache.set(topic, lower_partition_count)

        sleep(0.3)

        result = cache.get(topic) { raise "Should not be called" }

        assert_equal partition_count, result
      end
    end

    describe "with concurrent access" do
      it "correctly handles simultaneous updates to the same topic" do
        threads = []

        5.times do |i|
          threads << Thread.new do
            value = 10 + i
            cache.set(topic, value)
          end
        end

        threads.each(&:join)

        result = cache.get(topic) { raise "Should not be called" }

        assert_equal 14, result
      end
    end
  end

  describe "TTL behavior" do
    it "treats entries as expired when they exceed TTL" do
      cache.get(topic) { partition_count }

      sleep(default_ttl_ms / 1000.0 - 0.2)

      result = cache.get(topic) { raise "Should not be called when cache is valid" }

      assert_equal partition_count, result

      sleep(0.3)

      block_called = false
      new_value = partition_count + 3
      result = cache.get(topic) do
        block_called = true
        new_value
      end

      assert block_called
      assert_equal new_value, result
    end
  end

  describe "comprehensive scenarios" do
    it "handles a full lifecycle of cache operations" do
      result1 = cache.get(topic) { partition_count }

      assert_equal partition_count, result1

      result2 = cache.get(topic) { raise "Should not be called" }

      assert_equal partition_count, result2

      cache.set(topic, lower_partition_count)
      result3 = cache.get(topic) { raise "Should not be called" }

      assert_equal partition_count, result3

      cache.set(topic, higher_partition_count)
      result4 = cache.get(topic) { raise "Should not be called" }

      assert_equal higher_partition_count, result4

      sleep(default_ttl_ms / 1000.0 + 0.1)
      result5 = cache.get(topic) { lower_partition_count }

      assert_equal higher_partition_count, result5

      result6 = cache.get(topic) { raise "Should not be called" }

      assert_equal higher_partition_count, result6

      even_higher = higher_partition_count + 5
      cache.set(topic, even_higher)
      result7 = cache.get(topic) { raise "Should not be called" }

      assert_equal even_higher, result7
    end

    it "handles multiple topics with different TTLs correctly" do
      cache.get(topic) { partition_count }
      custom_ttl_cache.get(topic) { partition_count }

      sleep(custom_ttl_ms / 1000.0 + 0.1)

      default_result = cache.get(topic) { raise "Should not be called for default cache" }

      assert_equal partition_count, default_result

      custom_cache_value = partition_count + 8
      custom_block_called = false
      custom_result = custom_ttl_cache.get(topic) do
        custom_block_called = true
        custom_cache_value
      end

      assert custom_block_called
      assert_equal custom_cache_value, custom_result

      sleep((default_ttl_ms - custom_ttl_ms) / 1000.0 + 0.1)

      default_block_called = false
      new_default_value = partition_count + 10
      new_default_result = cache.get(topic) do
        default_block_called = true
        new_default_value
      end

      assert default_block_called
      assert_equal new_default_value, new_default_result
    end
  end
end
