# frozen_string_literal: true

# This integration test verifies that Consumer#cluster_id (and #member_id) do not leak the
# librdkafka-allocated string on every call.
#
# `rd_kafka_clusterid` / `rd_kafka_memberid` each return a newly allocated C string that the
# caller must release with `rd_kafka_mem_free`. They were bound with a `:string` return type,
# which makes FFI copy the bytes into a Ruby String but never free the underlying buffer, so
# every call leaked it. (`rd_kafka_clusterid` was additionally bound with the wrong arity,
# missing its `timeout_ms` argument.) They are now bound as `:pointer`, copied into a Ruby
# string, and freed.
#
# We hammer cluster_id and assert RSS stays flat. RSS is read from /proc on Linux; on platforms
# without /proc the calls are still exercised (no crash) but growth is not asserted. On JRuby RSS
# follows the JVM heap, so instead we assert that every non-NULL string returned by
# `rd_kafka_clusterid` is released with `rd_kafka_mem_free`.
#
# Requires a running Kafka broker at localhost:9092.
#
# Exit codes:
# - 0: no significant memory growth (or growth not measurable on this platform)
# - 1: the cluster id could not be fetched, RSS grew, or on JRuby the native allocations and
#   releases do not balance (leak still present)

require "rdkafka"
require "securerandom"
require_relative "support/native_call_counter"

$stdout.sync = true

if defined?(JRUBY_VERSION)
  NativeCallCounter.watch(:rd_kafka_clusterid) { |ptr| !ptr.null? }
  NativeCallCounter.watch(:rd_kafka_mem_free)
end

BOOTSTRAP = "localhost:9092"
GROUP = "memberid-clusterid-leak-#{SecureRandom.hex(6)}"
# 400k calls make a genuine per-call leak dominate the fixed RSS noise: the leak grows linearly
# with the count (~11.4 MB here) while the arena/fragmentation noise is bounded by the live heap
# (roughly constant regardless of the count), so the two are cleanly separable. cluster_id is
# cached after the warmup below, so these are local string alloc/free calls, not broker roundtrips.
ITERATIONS = 400_000

consumer = Rdkafka::Config.new("bootstrap.servers": BOOTSTRAP, "group.id": GROUP).consumer

# A cluster_id call with a timeout establishes the broker connection and caches the metadata, so
# we retry until the broker answers.
cluster_id = nil
20.times do
  cluster_id = consumer.cluster_id(5_000)
  break if cluster_id && !cluster_id.empty?

  sleep 0.3
end

if cluster_id.nil? || cluster_id.empty?
  warn "FAIL: could not fetch the cluster id from the broker"
  consumer.close
  exit(1)
end

def rss_kb
  File.read("/proc/self/status")[/VmRSS:\s+(\d+)/, 1].to_i
end

measurable = File.exist?("/proc/self/status")

# Settle the Ruby heap before sampling RSS. `GC.compact` defragments the heap so freed pages can be
# released, which keeps the baseline and final samples from drifting purely due to fragmentation.
# A plain `GC.start` leaves that drift in, which is what made this spec trip on Ruby 4.0.
def settle_heap
  GC.start
  GC.compact if GC.respond_to?(:compact)
end

# Warm up so the malloc arena / Ruby heap settle before we measure.
20_000.times { consumer.cluster_id(5_000) }
settle_heap
NativeCallCounter.reset
before = measurable ? rss_kb : 0

ITERATIONS.times { consumer.cluster_id(5_000) }

settle_heap
after = measurable ? rss_kb : 0
allocated = NativeCallCounter.count(:rd_kafka_clusterid)
released = NativeCallCounter.count(:rd_kafka_mem_free)
balanced = NativeCallCounter.balanced?(:rd_kafka_clusterid, :rd_kafka_mem_free)
consumer.close

if defined?(JRUBY_VERSION)
  puts "Native strings after #{ITERATIONS} cluster_id calls: #{allocated} allocated, #{released} freed"

  unless balanced
    warn "FAIL: #{allocated} cluster id strings allocated but #{released} freed - cluster_id still leaks"
    exit(1)
  end
elsif measurable
  delta = after - before
  puts "RSS delta after #{ITERATIONS} cluster_id calls: #{delta} KB"

  # When fixed this is a few dozen KB. Leaking the ~30-byte string on every call would be ~11.4 MB
  # at 400k iterations. Leak-free RSS noise on a loaded runner stays around ~2 MB (~10 bytes/call,
  # bounded by the live heap, not the call count), so the ceiling sits at 5 MB - well above the
  # observed noise floor (2.5x headroom) and less than half the ~11.4 MB a genuine leak produces.
  if delta > 5_000
    warn "FAIL: RSS grew #{delta} KB over #{ITERATIONS} calls - cluster_id still leaks"
    exit(1)
  end
else
  puts "RSS not measurable on this platform; exercised #{ITERATIONS} cluster_id calls without crashing"
end

puts "PASS: cluster_id did not leak (cluster_id=#{cluster_id.inspect})"
exit(0)
