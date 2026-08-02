# frozen_string_literal: true

# This integration test verifies that `TopicPartitionList#to_native_tpl` does not leak the native
# topic-partition list when population fails partway through.
#
# `to_native_tpl` allocates a native list with `rd_kafka_topic_partition_list_new` and only returns
# it (for the caller to destroy) on success. If population raised partway - e.g. a non-string
# metadata value, or an offset FFI cannot coerce to int64 - the half-built list was never returned
# and never destroyed, leaking it. Destruction is fully manual (the old doc comment claiming GC
# handles it was wrong). The list is now destroyed before the error propagates.
#
# We repeatedly build a list whose partition has a non-integer offset (which makes
# `rd_kafka_topic_partition_list_set_offset` raise) and assert RSS stays flat. No broker needed.
#
# Exit codes:
# - 0: no significant memory growth (or growth not measurable on this platform)
# - 1: RSS grew (the native list still leaks)

require "rdkafka"

$stdout.sync = true

ITERATIONS = 200_000

# A non-integer offset makes set_offset raise mid-population, after the native list was allocated.
def trigger_failed_build
  list = Rdkafka::Consumer::TopicPartitionList.new
  list.add_topic_and_partitions_with_offsets("to-native-tpl-probe", 0 => "not-an-offset")
  list.to_native_tpl
rescue TypeError
  nil
end

def rss_kb
  File.read("/proc/self/status")[/VmRSS:\s+(\d+)/, 1].to_i
end

measurable = File.exist?("/proc/self/status")

# Settle the Ruby heap before sampling RSS. `GC.compact` defragments the heap so freed pages can be
# released, which keeps the baseline and final samples from drifting purely due to fragmentation.
def settle_heap
  GC.start
  GC.compact if GC.respond_to?(:compact)
end

# Warm up so the malloc arena / Ruby heap settle before we measure.
20_000.times { trigger_failed_build }
settle_heap
before = measurable ? rss_kb : 0

ITERATIONS.times { trigger_failed_build }

settle_heap
after = measurable ? rss_kb : 0

if measurable
  delta = after - before
  puts "RSS delta after #{ITERATIONS} failed to_native_tpl builds: #{delta} KB"

  # When fixed this is essentially zero. Leaking the native list is ~30 MB at this iteration count.
  # RSS is noisy (allocator arenas, heap fragmentation) and can drift a few MB even without a leak,
  # so we use a 15 MB ceiling: comfortably above the observed noise floor yet well under the ~30 MB
  # a real leak produces.
  if delta > 15_000
    warn "FAIL: RSS grew #{delta} KB over #{ITERATIONS} failed builds - the native list still leaks"
    exit(1)
  end
else
  puts "RSS not measurable on this platform; exercised #{ITERATIONS} failed builds without crashing"
end

puts "PASS: a failed to_native_tpl build did not leak the native topic-partition list"
exit(0)
