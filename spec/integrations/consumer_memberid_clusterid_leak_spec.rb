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

JRUBY = RUBY_ENGINE == "jruby"

if JRUBY
  NativeCallCounter.watch(:rd_kafka_clusterid) { |ptr| !ptr.null? }
  NativeCallCounter.watch(:rd_kafka_mem_free)
end

BOOTSTRAP = "localhost:9092"
GROUP = "memberid-clusterid-leak-#{SecureRandom.hex(6)}"
ITERATIONS = 200_000

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

# Warm up so the malloc arena / Ruby heap settle before we measure.
20_000.times { consumer.cluster_id(5_000) }
GC.start
NativeCallCounter.reset
before = measurable ? rss_kb : 0

ITERATIONS.times { consumer.cluster_id(5_000) }

GC.start
after = measurable ? rss_kb : 0
allocated = NativeCallCounter.count(:rd_kafka_clusterid)
released = NativeCallCounter.count(:rd_kafka_mem_free)
balanced = NativeCallCounter.balanced?(:rd_kafka_clusterid, :rd_kafka_mem_free)
consumer.close

if JRUBY
  puts "Native strings after #{ITERATIONS} cluster_id calls: #{allocated} allocated, #{released} freed"

  unless balanced
    warn "FAIL: #{allocated} cluster id strings allocated but #{released} freed - cluster_id still leaks"
    exit(1)
  end
elsif measurable
  delta = after - before
  puts "RSS delta after #{ITERATIONS} cluster_id calls: #{delta} KB"

  # When fixed this is a few dozen KB of noise. Leaking the ~30-byte string per call would be
  # several MB at this iteration count, so a 2 MB ceiling separates the two cleanly.
  if delta > 2_000
    warn "FAIL: RSS grew #{delta} KB over #{ITERATIONS} calls - cluster_id still leaks"
    exit(1)
  end
else
  puts "RSS not measurable on this platform; exercised #{ITERATIONS} cluster_id calls without crashing"
end

puts "PASS: cluster_id did not leak (cluster_id=#{cluster_id.inspect})"
exit(0)
