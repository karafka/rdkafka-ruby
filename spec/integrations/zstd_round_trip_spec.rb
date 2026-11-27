# frozen_string_literal: true

# This integration test verifies that messages produced with `compression.codec=zstd` are
# compressed with the bundled zstd and consumed back byte for byte.
#
# The payloads cross zstd's 64 KiB block and buffer sizes (empty, 65535, 65536, 131073 and over
# 1 MiB) and many small ones share batches. The producer `tx_bytes` statistic proves that zstd was
# applied: it is far below the payload size.
#
# Uses librdkafka's mock cluster, so it needs no Kafka broker.
#
# Exit codes:
# - 0: every payload came back unchanged and the produced bytes were compressed
# - 1: otherwise

require_relative "support/mock_cluster"
require "securerandom"

TOPIC = "it-zstd-round-trip-#{SecureRandom.hex(6)}"
WAIT_MS = 30_000

# Random words make compressible payloads that still carry many literals
WORDS = Array.new(256) { SecureRandom.alphanumeric(rand(3..12)) }.freeze

def payload(size)
  text = +""
  text << WORDS.sample << " " while text.bytesize < size
  text.byteslice(0, size).b
end

payloads = [0, 1, 65_535, 65_536, 65_537, 131_073, 1_500_000].map { |size| payload(size) }
payloads += Array.new(300) { payload(rand(10..2_000)) }

stats = Queue.new
Rdkafka::Config.statistics_callback = ->(event) { stats << event if event["type"] == "producer" }

received = []
tx_bytes = 0

MockCluster.run do |cluster|
  cluster.create_topic(TOPIC, 1)

  producer = cluster.config(
    "compression.codec": "zstd",
    "message.max.bytes": 2_000_000,
    "linger.ms": 20,
    "statistics.interval.ms": 100
  ).producer

  payloads
    .each_with_index
    .map { |payload, index| producer.produce(topic: TOPIC, payload: payload, key: index.to_s) }
    .each { |handle| handle.wait(max_wait_timeout_ms: WAIT_MS) }

  stats.clear
  tx_bytes = stats.pop(timeout: WAIT_MS / 1_000)&.fetch("tx_bytes") || 0

  consumer = cluster.config(
    "group.id": TOPIC,
    "auto.offset.reset": "earliest",
    "fetch.message.max.bytes": 2_000_000
  ).consumer
  consumer.subscribe(TOPIC)

  deadline = Time.now + (WAIT_MS / 1_000)

  while received.size < payloads.size && Time.now < deadline
    message = consumer.poll(500)
    received << message.payload.to_s.b if message
  end
ensure
  producer&.close
  consumer&.close
end

exit(1) unless received == payloads

# Without compression the produced bytes exceed the payload size
exit(1) unless tx_bytes.positive? && tx_bytes < payloads.sum(&:bytesize) / 2
