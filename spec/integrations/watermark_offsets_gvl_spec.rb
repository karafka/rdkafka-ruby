# frozen_string_literal: true

# This integration test verifies that `Consumer#query_watermark_offsets` releases the GVL while it
# waits on its broker round-trip, instead of freezing every other Ruby thread in the process.
#
# `rd_kafka_query_watermark_offsets` is a synchronous network call. It was the only such call
# attached without `blocking: true`, so FFI held the GVL for the whole `timeout_ms` wait - starving
# every other Ruby thread (including producer polling threads, delaying delivery reports).
# `Consumer#lag` calls it once per partition, multiplying the stall. The binding is now `blocking:
# true` like the neighboring `rd_kafka_offsets_for_times`.
#
# We point at an unreachable broker so the call blocks for its full timeout, run a counter loop on a
# background thread, and check that the thread kept making progress during the blocked call. On MRI
# without the fix the background thread advances ~0 (GVL held); with it, hundreds of millions. On
# JRuby there is no GVL so the thread always progresses (the call was never a problem there).
#
# No broker is required (the point is that the broker is unreachable).
#
# Exit codes:
# - 0: the background thread kept running during the blocked call (GVL released)
# - 1: the background thread was starved (GVL held for the whole call)

require "rdkafka"

$stdout.sync = true

TIMEOUT_MS = 2_000

consumer = Rdkafka::Config.new(
  "bootstrap.servers": "127.0.0.1:9999", # nothing listening -> the query blocks until timeout
  "group.id": "watermark-gvl-probe"
).consumer

counter = 0
stop = false
worker = Thread.new do
  until stop
    counter += 1
  end
end

# Let the worker spin up, then snapshot its progress just before the blocking call.
sleep(0.2)
before = counter

started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
begin
  consumer.query_watermark_offsets("watermark-gvl-probe", 0, TIMEOUT_MS)
rescue Rdkafka::RdkafkaError
  nil
end
elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started

stop = true
worker.join
consumer.close

advanced = counter - before
puts "Blocked for #{elapsed.round(2)}s; background thread advanced #{advanced} iterations"

# With the GVL released the worker runs millions of iterations during the wait; while the GVL is
# held it advances ~0. A 1,000,000 floor separates the two with an enormous margin.
if advanced < 1_000_000
  warn "FAIL: background thread advanced only #{advanced} - query_watermark_offsets held the GVL"
  exit(1)
end

puts "PASS: query_watermark_offsets released the GVL during its broker round-trip"
exit(0)
