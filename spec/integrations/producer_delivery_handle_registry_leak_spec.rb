# frozen_string_literal: true

# This integration test verifies that a `produce` call which fails after the delivery handle has
# been registered does not orphan that handle in the process-global registry.
#
# `Producer#produce` registers the delivery handle in `AbstractHandle::REGISTRY` (a process-global
# Hash that survives producer close) and previously only removed it again when `rd_kafka_producev`
# returned a non-zero code. Any exception raised between registration and that check - a concurrent
# close making `with_inner` raise `ClosedInnerError`, or a header value whose `#to_s` raises - left
# the handle in the registry forever. `produce` now removes the handle on any such failure.
#
# We force the failure deterministically with a header value whose `#to_s` raises (this happens
# after the handle is registered but before the message is enqueued) and assert the registry does
# not grow. No broker delivery is needed.
#
# Exit codes:
# - 0: the registry did not grow (handles cleaned up on failure)
# - 1: the registry grew (handles still leak)

require "rdkafka"

$stdout.sync = true

ITERATIONS = 25_000

# A header value whose #to_s raises, to make produce fail after the delivery handle is registered.
class ExplodingValue
  def to_s
    raise "boom"
  end
end

producer = Rdkafka::Config.new("bootstrap.servers": "localhost:9092").producer

registry = Rdkafka::AbstractHandle::REGISTRY
before = registry.size

ITERATIONS.times do
  producer.produce(topic: "registry-leak-probe", payload: "x", headers: { "h" => ExplodingValue.new })
rescue RuntimeError
  nil
end

after = registry.size
producer.close

growth = after - before
puts "Registry growth after #{ITERATIONS} failed produces: #{growth}"

# When fixed the failed produces leave nothing behind. Before the fix every call orphaned its
# handle, so the registry would grow by ITERATIONS; a small ceiling separates the two cleanly.
if growth > 100
  warn "FAIL: registry grew by #{growth} over #{ITERATIONS} failed produces - delivery handles leak"
  exit(1)
end

puts "PASS: failed produce did not orphan delivery handles in the registry"
exit(0)
