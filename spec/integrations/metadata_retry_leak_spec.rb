# frozen_string_literal: true

# This integration test verifies that retrying a metadata fetch does not leak the native
# metadata struct allocated by each attempt.
#
# `Metadata#initialize` retries on `timed_out` / `leader_not_available`. The retry path used
# `retry` from a method-level `rescue`, which restarts the `begin` block WITHOUT running its
# `ensure`, so every attempt but the last leaked its `rd_kafka_metadata` struct (and its
# `rd_kafka_topic_new` reference). The `leader_not_available` case is the dangerous one: it is
# raised while parsing, after `rd_kafka_metadata` has already allocated the whole struct. The
# fetch now frees each attempt's native resources before the retry loop runs.
#
# Triggering `leader_not_available` from a real broker is timing dependent (it happens during
# leader election), so we deterministically force the parse step to raise it twice per call while
# letting the REAL `rd_kafka_metadata` allocate a struct on each of the three attempts. We then
# hammer the path and assert RSS stays flat. RSS is read from /proc on Linux; on platforms
# without /proc the path is still exercised (no crash) but growth is not asserted.
#
# Requires a running Kafka broker at localhost:9092.
#
# Exit codes:
# - 0: no significant memory growth (or growth not measurable on this platform)
# - 1: RSS grew (leak still present)

require "rdkafka"

$stdout.sync = true

BOOTSTRAP = "localhost:9092"
TOPIC = "metadata-retry-leak-probe"
ITERATIONS = 20_000

# Force two retried (leader_not_available) failures per Metadata fetch from the parse step. This
# runs only after rd_kafka_metadata has already allocated the struct, so it exercises the exact
# path that leaked the struct before the fix. `sleep` is overridden to a no-op so the retry
# backoff is instant and the loop stays fast.
module ForceMetadataRetry
  def metadata_from_native(ptr)
    @attempts = (@attempts || 0) + 1
    raise Rdkafka::RdkafkaError.new(5) if @attempts <= 2 # 5 => leader_not_available

    super
  end

  def sleep(*) = nil
end
Rdkafka::Metadata.prepend(ForceMetadataRetry)

def rss_kb
  File.read("/proc/self/status")[/VmRSS:\s+(\d+)/, 1].to_i
end

measurable = File.exist?("/proc/self/status")

admin = Rdkafka::Config.new("bootstrap.servers": BOOTSTRAP).admin

# Each call retries twice (allocating a real struct per attempt) before the real parse runs; the
# final attempt may raise (e.g. unknown topic) but the leak path has already been exercised, so we
# only care that the call ran, not its result.
def probe(admin)
  admin.metadata(TOPIC)
rescue Rdkafka::RdkafkaError
  nil
end

# Warm up so the broker connection is established and the malloc arena / Ruby heap settle.
2_000.times { probe(admin) }
GC.start
before = measurable ? rss_kb : 0

ITERATIONS.times { probe(admin) }

GC.start
after = measurable ? rss_kb : 0
admin.close

if measurable
  delta = after - before
  puts "RSS delta after #{ITERATIONS} retried metadata fetches: #{delta} KB"

  # When fixed this is essentially zero. Leaking two structs per call is ~10 MB at this iteration
  # count, so a 3 MB ceiling separates the two cleanly.
  if delta > 3_000
    warn "FAIL: RSS grew #{delta} KB over #{ITERATIONS} retried fetches - metadata struct still leaks"
    exit(1)
  end
else
  puts "RSS not measurable on this platform; exercised #{ITERATIONS} retried fetches without crashing"
end

puts "PASS: retried metadata fetch did not leak the native metadata struct"
exit(0)
