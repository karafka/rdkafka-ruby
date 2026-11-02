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
# Threads call `Producer#queue_size` in a loop behind a start barrier. After they finish, `close`
# must return within a timeout: a count drifted either way never reaches zero, so `close` hangs.
#
# No broker is required: `queue_size` reads only the local queue.
#
# Exit codes:
# - 0: `close` returned
# - 1: `close` did not return

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

closer = Thread.new { producer.close }
closed = !closer.join(CLOSE_TIMEOUT).nil?

puts "Calls: #{calls.sum}, closed: #{closed}"

unless closed
  warn "FAIL: close did not return, the operations in progress count drifted"
  # A normal exit runs the producer finalizer, which waits for the stuck close
  exit!(1)
end

puts "PASS: close returned, the operations in progress count stayed exact"
exit(0)
