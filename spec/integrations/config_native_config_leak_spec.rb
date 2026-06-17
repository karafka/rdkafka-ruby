# frozen_string_literal: true

# This integration test verifies that a failed client creation does not leak the native
# librdkafka configuration (`rd_kafka_conf_t`).
#
# `Config#native_config` allocates a conf with `rd_kafka_conf_new` and then applies each option
# with `rd_kafka_conf_set`. When an option is rejected it raised `ConfigError` while the freshly
# allocated conf was still mid-build, without ever calling `rd_kafka_conf_destroy` - so every
# failed creation leaked the whole conf (a few KB each). The conf is now destroyed before the
# error propagates.
#
# We hammer the failing-creation path and assert RSS stays flat. No broker is needed: the conf is
# rejected before any network access. RSS is read from /proc on Linux; on platforms without /proc
# the path is still exercised (no crash) but growth is not asserted.
#
# Exit codes:
# - 0: no significant memory growth (or growth not measurable on this platform)
# - 1: RSS grew (leak still present)

require "rdkafka"

$stdout.sync = true

ITERATIONS = 50_000

# An unknown property is rejected by rd_kafka_conf_set, which makes native_config raise
# ConfigError while the conf is still being built.
def trigger_failed_creation
  Rdkafka::Config.new("totally.invalid.property": "x").consumer
rescue Rdkafka::Config::ConfigError
  nil
end

def rss_kb
  File.read("/proc/self/status")[/VmRSS:\s+(\d+)/, 1].to_i
end

measurable = File.exist?("/proc/self/status")

# Warm up so the malloc arena / Ruby heap settle before we measure.
5_000.times { trigger_failed_creation }
GC.start
before = measurable ? rss_kb : 0

ITERATIONS.times { trigger_failed_creation }

GC.start
after = measurable ? rss_kb : 0

if measurable
  delta = after - before
  puts "RSS delta after #{ITERATIONS} failed creations: #{delta} KB"

  # When fixed this is well under 1 MB of noise. Leaking the conf (~3 KB) per call is ~150 MB at
  # this iteration count, so a 25 MB ceiling separates the two cleanly.
  if delta > 25_000
    warn "FAIL: RSS grew #{delta} KB over #{ITERATIONS} failed creations - the conf still leaks"
    exit(1)
  end
else
  puts "RSS not measurable on this platform; exercised #{ITERATIONS} failed creations without crashing"
end

puts "PASS: failed client creation did not leak the native config"
exit(0)
