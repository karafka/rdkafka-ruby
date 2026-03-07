# frozen_string_literal: true

require "test_helper"

class PartitionsCountCacheTest < Minitest::Test
  def setup
    super
    @default_ttl_ms = 1_000
    @custom_ttl_ms = 500
    @cache = Rdkafka::Producer::PartitionsCountCache.new(ttl_ms: @default_ttl_ms)
    @custom_ttl_cache = Rdkafka::Producer::PartitionsCountCache.new(ttl_ms: @custom_ttl_ms)
    @topic = TestTopics.unique
    @topic2 = TestTopics.unique
    @partition_count = 5
    @higher_partition_count = 10
    @lower_partition_count = 3
    @even_higher_partition_count = 15
  end

  def test_creates_cache_with_default_ttl
    standard_cache = Rdkafka::Producer::PartitionsCountCache.new

    assert_kind_of Rdkafka::Producer::PartitionsCountCache, standard_cache
  end

  def test_creates_cache_with_custom_ttl
    assert_kind_of Rdkafka::Producer::PartitionsCountCache, @custom_ttl_cache
  end

  def test_backwards_compat_with_old_ttl_parameter
    old_style_cache = Rdkafka::Producer::PartitionsCountCache.new(1)

    assert_kind_of Rdkafka::Producer::PartitionsCountCache, old_style_cache
  end

  def test_backwards_compat_converts_seconds_to_milliseconds
    old_style_cache = Rdkafka::Producer::PartitionsCountCache.new(2)

    old_style_cache.set(@topic, @partition_count)

    sleep(1.5)
    result = old_style_cache.get(@topic) { raise "Should not be called - cache should still be valid" }

    assert_equal @partition_count, result

    sleep(0.7)
    block_called = false
    new_result = old_style_cache.get(@topic) do
      block_called = true
      @partition_count + 1
    end

    assert block_called
    assert_equal @partition_count + 1, new_result
  end

  def test_accepts_both_ttl_and_ttl_ms
    cache_instance = Rdkafka::Producer::PartitionsCountCache.new(1, ttl_ms: 1000)

    assert_kind_of Rdkafka::Producer::PartitionsCountCache, cache_instance
  end

  def test_ttl_ms_takes_precedence_over_ttl
    both_params_cache = Rdkafka::Producer::PartitionsCountCache.new(10, ttl_ms: 500)

    both_params_cache.set(@topic, @partition_count)

    sleep(0.6)

    block_called = false
    both_params_cache.get(@topic) do
      block_called = true
      @partition_count + 1
    end

    assert block_called
  end

  def test_get_yields_and_caches_on_miss
    block_called = false
    result = @cache.get(@topic) do
      block_called = true
      @partition_count
    end

    assert block_called
    assert_equal @partition_count, result

    second_block_called = false
    second_result = @cache.get(@topic) do
      second_block_called = true
      @partition_count + 1
    end

    refute second_block_called
    assert_equal @partition_count, second_result
  end

  def test_get_returns_cached_value_without_yielding
    @cache.get(@topic) { @partition_count }

    block_called = false
    result = @cache.get(@topic) do
      block_called = true
      @partition_count + 1
    end

    refute block_called
    assert_equal @partition_count, result
  end

  def test_get_yields_when_ttl_expired
    @cache.get(@topic) { @partition_count }

    sleep(@default_ttl_ms / 1000.0 + 0.1)

    block_called = false
    new_count = @partition_count + 1
    result = @cache.get(@topic) do
      block_called = true
      new_count
    end

    assert block_called
    assert_equal new_count, result

    second_block_called = false
    second_result = @cache.get(@topic) do
      second_block_called = true
      new_count + 1
    end

    refute second_block_called
    assert_equal new_count, second_result
  end

  def test_respects_custom_ttl
    # Seed both caches
    @cache.get(@topic) { @partition_count }
    @custom_ttl_cache.get(@topic) { @partition_count }

    sleep(@custom_ttl_ms / 1000.0 + 0.1)

    custom_block_called = false
    custom_result = @custom_ttl_cache.get(@topic) do
      custom_block_called = true
      @higher_partition_count
    end

    assert custom_block_called
    assert_equal @higher_partition_count, custom_result

    default_block_called = false
    default_result = @cache.get(@topic) do
      default_block_called = true
      @higher_partition_count
    end

    refute default_block_called
    assert_equal @partition_count, default_result
  end

  def test_updates_cache_when_new_value_higher
    @cache.get(@topic) { @partition_count }

    sleep(@default_ttl_ms / 1000.0 + 0.1)

    result = @cache.get(@topic) { @higher_partition_count }

    assert_equal @higher_partition_count, result

    second_result = @cache.get(@topic) { raise "Should not be called" }

    assert_equal @higher_partition_count, second_result
  end

  def test_preserves_higher_cached_value
    @cache.get(@topic) { @partition_count }

    sleep(@default_ttl_ms / 1000.0 + 0.1)
    @cache.get(@topic) { @higher_partition_count }

    sleep(@default_ttl_ms / 1000.0 + 0.1)
    result = @cache.get(@topic) { @lower_partition_count }

    assert_equal @higher_partition_count, result

    second_result = @cache.get(@topic) { raise "Should not be called" }

    assert_equal @higher_partition_count, second_result
  end

  def test_handles_multiple_topics_independently
    @cache.get(@topic) { @partition_count }
    @cache.get(@topic2) { @higher_partition_count }

    sleep(@default_ttl_ms / 1000.0 + 0.1)

    first_result = @cache.get(@topic) { @even_higher_partition_count }

    assert_equal @even_higher_partition_count, first_result

    second_updated = @higher_partition_count + 3
    second_result = @cache.get(@topic2) { second_updated }

    assert_equal second_updated, second_result

    assert_equal @even_higher_partition_count, @cache.get(@topic) { raise "Should not be called" }
    assert_equal second_updated, @cache.get(@topic2) { raise "Should not be called" }
  end

  def test_set_adds_new_entry
    @cache.set(@topic, @partition_count)

    result = @cache.get(@topic) { raise "Should not be called" }

    assert_equal @partition_count, result
  end

  def test_set_updates_when_higher
    @cache.set(@topic, @partition_count)
    @cache.set(@topic, @higher_partition_count)

    result = @cache.get(@topic) { raise "Should not be called" }

    assert_equal @higher_partition_count, result
  end

  def test_set_keeps_original_when_lower
    @cache.set(@topic, @partition_count)
    @cache.set(@topic, @lower_partition_count)

    result = @cache.get(@topic) { raise "Should not be called" }

    assert_equal @partition_count, result
  end

  def test_set_updates_timestamp_even_when_keeping_value
    @cache.set(@topic, @partition_count)

    sleep(@default_ttl_ms / 1000.0 - 0.2)

    @cache.set(@topic, @lower_partition_count)

    sleep(0.3)

    result = @cache.get(@topic) { raise "Should not be called" }

    assert_equal @partition_count, result
  end

  def test_concurrent_updates
    threads = []

    5.times do |i|
      threads << Thread.new do
        value = 10 + i
        @cache.set(@topic, value)
      end
    end

    threads.each(&:join)

    result = @cache.get(@topic) { raise "Should not be called" }

    assert_equal 14, result
  end

  def test_ttl_expiration_behavior
    @cache.get(@topic) { @partition_count }

    sleep(@default_ttl_ms / 1000.0 - 0.2)

    result = @cache.get(@topic) { raise "Should not be called when cache is valid" }

    assert_equal @partition_count, result

    sleep(0.3)

    block_called = false
    new_value = @partition_count + 3
    result = @cache.get(@topic) do
      block_called = true
      new_value
    end

    assert block_called
    assert_equal new_value, result
  end

  def test_full_lifecycle
    result1 = @cache.get(@topic) { @partition_count }

    assert_equal @partition_count, result1

    result2 = @cache.get(@topic) { raise "Should not be called" }

    assert_equal @partition_count, result2

    @cache.set(@topic, @lower_partition_count)
    result3 = @cache.get(@topic) { raise "Should not be called" }

    assert_equal @partition_count, result3

    @cache.set(@topic, @higher_partition_count)
    result4 = @cache.get(@topic) { raise "Should not be called" }

    assert_equal @higher_partition_count, result4

    sleep(@default_ttl_ms / 1000.0 + 0.1)
    result5 = @cache.get(@topic) { @lower_partition_count }

    assert_equal @higher_partition_count, result5

    result6 = @cache.get(@topic) { raise "Should not be called" }

    assert_equal @higher_partition_count, result6

    even_higher = @higher_partition_count + 5
    @cache.set(@topic, even_higher)
    result7 = @cache.get(@topic) { raise "Should not be called" }

    assert_equal even_higher, result7
  end

  def test_multiple_topics_with_different_ttls
    @cache.get(@topic) { @partition_count }
    @custom_ttl_cache.get(@topic) { @partition_count }

    sleep(@custom_ttl_ms / 1000.0 + 0.1)

    default_result = @cache.get(@topic) { raise "Should not be called for default cache" }

    assert_equal @partition_count, default_result

    custom_cache_value = @partition_count + 8
    custom_block_called = false
    custom_result = @custom_ttl_cache.get(@topic) do
      custom_block_called = true
      custom_cache_value
    end

    assert custom_block_called
    assert_equal custom_cache_value, custom_result

    sleep((@default_ttl_ms - @custom_ttl_ms) / 1000.0 + 0.1)

    default_block_called = false
    new_default_value = @partition_count + 10
    new_default_result = @cache.get(@topic) do
      default_block_called = true
      new_default_value
    end

    assert default_block_called
    assert_equal new_default_value, new_default_result
  end
end
