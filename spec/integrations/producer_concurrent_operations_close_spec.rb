# frozen_string_literal: true

# This integration test verifies that a producer used from many threads at once still counts its
# operations in progress exactly, so `Producer#close` returns once the threads are done.
#
# `NativeKafka#with_inner` counts the operations that use the native handle, and `#close` waits for
# that count to reach zero before it destroys the handle. The increment and the decrement ran under
# two different mutexes. On JRuby, which has no GVL, `+=` on the counter is not atomic, so
# concurrent updates were lost: the count drifted away from zero and `close` waited forever (or,
# with a count drifted low, could destroy the handle while another thread still used it). MRI's
# GVL hid the race.
#
# Threads call `Producer#queue_size` in a loop behind a start barrier. After they finish, the count
# must be zero and `close` must return within a timeout.
#
# No broker is required: `queue_size` reads only the local queue.
#
# Exit codes:
# - 0: the count is zero and `close` returned
# - 1: the count drifted or `close` did not return

require "rdkafka"

$stdout.sync = true

THREADS = 16
RUN_FOR = 3
CLOSE_TIMEOUT = 10

producer = Rdkafka::Config.new(
  "bootstrap.servers": "127.0.0.1:9999",
  "message.timeout.ms": 1_000
).producer

barrier = Queue.new
stop = false
calls = Array.new(THREADS, 0)

workers = Array.new(THREADS) do |i|
  Thread.new do
    barrier.pop

    until stop
      producer.queue_size
      calls[i] += 1
    end
  end
end

THREADS.times { barrier << true }
sleep(RUN_FOR)
stop = true
workers.each(&:join)

operations_in_progress = producer.instance_variable_get(:@native_kafka).instance_variable_get(:@operations_in_progress)
closer = Thread.new { producer.close }
closed = !closer.join(CLOSE_TIMEOUT).nil?

puts "Calls: #{calls.sum}, operations in progress after join: #{operations_in_progress}, closed: #{closed}"

unless operations_in_progress.zero? && closed
  warn "FAIL: the operations in progress count drifted or close did not return"
  # A normal exit runs the producer finalizer, which waits for the stuck close
  exit!(1)
end

puts "PASS: the operations in progress count stayed exact and close returned"
exit(0)
