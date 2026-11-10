# frozen_string_literal: true

# This integration test verifies that `Producer#close` returns after signal trap handlers raised
# while the main thread used the producer.
#
# `NativeKafka#with_inner` counts the operations that use the native handle, and `#close` waits for
# that count to reach zero. `Thread.handle_interrupt` does not defer trap handlers: they run on the
# main thread under the mask, so a raise there could skip the decrement and `close` waited forever.
#
# The main thread calls `Producer#queue_size` in a loop. Another process sends it a signal every
# 0.5 ms (a thread would get the GVL too rarely), and the trap handler raises. After that, `close`
# must return within a timeout.
#
# The run starts once the first signal arrived: the sender is a new Ruby process, and JRuby boots it
# in longer than `RUN_FOR`. JRuby runs each trap handler in its own thread and raises the error in
# the main thread later.
#
# No broker is required: `queue_size` reads only the local queue.
#
# Exit codes:
# - 0: `close` returned
# - 1: `close` did not return

require "rdkafka"

RUN_FOR = 2
SENDER_BOOT_TIMEOUT = 60
CLOSE_TIMEOUT = 10

Trapped = Class.new(StandardError)

producer = Rdkafka::Config.new(
  "bootstrap.servers": "127.0.0.1:9999",
  "message.timeout.ms": 1_000
).producer

raised = 0
signaled = false
# Holds at most one token, so no raise lands in the `rescue`
token = Queue.new

trap("USR1") do
  signaled = true

  begin
    token.pop(true)
  rescue ThreadError
    next
  end

  raise Trapped
end

sender = spawn(RbConfig.ruby, "-e", "loop { Process.kill('USR1', #{Process.pid}); sleep(0.0005) }")

boot_deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + SENDER_BOOT_TIMEOUT
sleep(0.01) until signaled || Process.clock_gettime(Process::CLOCK_MONOTONIC) > boot_deadline

deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + RUN_FOR

begin
  token << true

  producer.queue_size while Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline

  begin
    token.pop(true)
  rescue ThreadError
    sleep
  end
rescue Trapped
  raised += 1
  retry
end

Process.kill("KILL", sender)
Process.wait(sender)
trap("USR1", "IGNORE")

exit(1) if raised.zero?

closer = Thread.new { producer.close }

# A normal exit runs the producer finalizer, which waits for the stuck close
exit!(1) if closer.join(CLOSE_TIMEOUT).nil?
