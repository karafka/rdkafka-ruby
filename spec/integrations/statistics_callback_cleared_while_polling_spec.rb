# frozen_string_literal: true

# This integration test verifies that clearing the global statistics callback while a producer's
# polling thread delivers statistics does not kill the process.
#
# The statistics callback runs on the polling thread, which has `abort_on_exception = true`. It
# used to read `Rdkafka::Config.statistics_callback` twice, once to check it and once to call it.
# When another thread set it to `nil` in between, the call raised `NoMethodError` and the whole
# process aborted. On JRuby this happened within a second; on MRI the GVL hid it.
#
# Requires a running Kafka broker at localhost:9092.
#
# Exit codes:
# - 0: the process survived the toggling and statistics were delivered
# - 1: no statistics were delivered (or the process aborted from the polling thread)

require "rdkafka"

$stdout.sync = true

BOOTSTRAP = "localhost:9092"
DURATION = 3

calls = 0
callback = ->(_stats) { calls += 1 }
Rdkafka::Config.statistics_callback = callback

producer = Rdkafka::Config.new(
  "bootstrap.servers": BOOTSTRAP,
  "statistics.interval.ms": 10
).producer

deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + DURATION

begin
  while Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
    Rdkafka::Config.statistics_callback = nil
    Rdkafka::Config.statistics_callback = callback
  end
ensure
  producer.close
end

if calls.zero?
  warn "FAIL: no statistics were delivered in #{DURATION} s"
  exit(1)
end

puts "PASS: the process survived clearing the statistics callback during #{calls} deliveries"
exit(0)
