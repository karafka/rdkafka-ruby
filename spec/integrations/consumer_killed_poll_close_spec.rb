# frozen_string_literal: true

# This integration test verifies that `Consumer#close` returns after a thread was killed while it
# waited inside `Consumer#poll`.
#
# `NativeKafka#with_inner` counts the operations that use the native handle, and `#close` waits for
# that count to reach zero before it destroys the handle. JRuby cannot stop a blocking native call,
# so it delivered the kill when the call returned, at the lock the decrement took in the `ensure`:
# the decrement was skipped, the count stayed at 1 and `close` waited forever. MRI delivers the kill
# inside the native call, so it was not affected.
#
# A thread calls `Consumer#poll` with a long timeout and is killed while it waits. `close` must wait
# for that call to return and then finish within a timeout.
#
# No broker is required: `poll` waits on the local queue.
#
# Exit codes:
# - 0: `close` returned
# - 1: `close` did not return

require "rdkafka"

$stdout.sync = true

POLL_TIMEOUT_MS = 2_000
CLOSE_TIMEOUT = 15

consumer = Rdkafka::Config.new(
  "bootstrap.servers": "127.0.0.1:9999",
  "group.id": "consumer-killed-poll-close"
).consumer

poller = Thread.new { consumer.poll(POLL_TIMEOUT_MS) }

sleep(0.5)
poller.kill
poller.join

closer = Thread.new { consumer.close }
closed = !closer.join(CLOSE_TIMEOUT).nil?

unless closed
  warn "FAIL: close did not return after a thread was killed inside poll"
  # A normal exit runs the consumer finalizer, which waits for the stuck close
  exit!(1)
end

puts "PASS: close returned after a thread was killed inside poll"
exit(0)
