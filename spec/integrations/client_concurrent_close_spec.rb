# frozen_string_literal: true

# This integration test verifies that many threads can close one client at the same time: every
# `close` call returns `nil`, none raises, and the client is closed when each `close` returns.
#
# An app can close one client twice at once, e.g. its own shutdown thread and the `at_exit` hook of
# `Rdkafka::Clients.close_all`. A late closer waits for the access lock while the first one closes
# the client, so it must not use the destroyed native handle after it gets the lock.
#
# For a producer, a consumer and an admin, 16 threads call `close` behind a start barrier, 50 times
# each with a new client.
#
# No broker is required: closing does not need a connection.
#
# Exit codes:
# - 0: every `close` returned `nil` with its client closed
# - 1: a `close` raised, returned something else or returned before its client was closed

require "rdkafka"

THREADS = 16
ITERATIONS = 50

CONFIGS = {
  producer: { "bootstrap.servers": "127.0.0.1:9999" },
  consumer: { "bootstrap.servers": "127.0.0.1:9999", "group.id": "client-concurrent-close" },
  admin: { "bootstrap.servers": "127.0.0.1:9999" }
}.freeze

CONFIGS.each do |type, config|
  ITERATIONS.times do
    client = Rdkafka::Config.new(config).public_send(type)
    barrier = Queue.new

    closers = Array.new(THREADS) do
      Thread.new do
        barrier.pop
        [client.close, client.closed?]
      rescue => e
        e
      end
    end

    THREADS.times { barrier << true }

    exit(1) unless closers.map(&:value).all?([nil, true])
  end
end

exit(0)
