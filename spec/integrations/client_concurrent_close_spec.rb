# frozen_string_literal: true

# This integration test verifies that many threads can close one client at the same time: every
# `close` call returns `nil`, none raises, and the client ends closed.
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
# - 0: every `close` returned `nil` and every client is closed
# - 1: a `close` raised or returned something else, or a client is not closed

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
        client.close
      rescue => e
        e
      end
    end

    THREADS.times { barrier << true }

    exit(1) unless closers.map(&:value).all?(nil)
    exit(1) unless client.closed?
  end
end

exit(0)
