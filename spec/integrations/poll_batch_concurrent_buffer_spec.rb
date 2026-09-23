# frozen_string_literal: true

# This integration test verifies that calling `poll_batch` concurrently from several threads on a
# single consumer is memory-safe.
#
# `poll_batch`/`poll_batch_nb` used to fetch into a single per-consumer scratch buffer
# (`@batch_buffer`). With two threads polling the same consumer, one thread's
# `rd_kafka_consume_batch_queue` overwrites the message pointers the other thread is still
# iterating and calling `rd_kafka_message_destroy` on - a double free / use-after-free (the process
# usually crashes with a malloc abort or SIGSEGV), while the overwritten pointers leak. The buffer
# is now fiber-local, so each thread gets its own.
#
# We pre-produce a batch of messages and then hammer `poll_batch` from several threads at once. On
# the buggy code this crashes the process; with the fix it drains cleanly.
#
# Requires a running Kafka broker at localhost:9092.
#
# Exit codes:
# - 0: concurrent batch polling completed without corruption
# - 1: messages could not be produced/consumed in time (inconclusive)
# - non-zero crash signal: the shared-buffer double free / use-after-free (bug present)

require "rdkafka"
require "securerandom"

$stdout.sync = true

BOOTSTRAP = "localhost:9092"
TOPIC = "it-poll-batch-concurrent-#{SecureRandom.hex(6)}"
PARTITIONS = 6
MESSAGES = 20_000
THREADS = 6
DEADLINE_S = 60

admin = Rdkafka::Config.new("bootstrap.servers": BOOTSTRAP).admin
begin
  admin.create_topic(TOPIC, PARTITIONS, 1).wait(max_wait_timeout_ms: 15_000)
rescue Rdkafka::RdkafkaError
  nil
ensure
  admin.close
end

producer = Rdkafka::Config.new("bootstrap.servers": BOOTSTRAP).producer
handles = MESSAGES.times.map do |i|
  producer.produce(topic: TOPIC, payload: "payload-#{i}", key: "key-#{i}", partition: i % PARTITIONS)
end
handles.each { |h| h.wait(max_wait_timeout_ms: 30_000) }
producer.close

consumer = Rdkafka::Config.new(
  "bootstrap.servers": BOOTSTRAP,
  "group.id": "it-poll-batch-concurrent-#{SecureRandom.hex(6)}",
  "auto.offset.reset": "earliest"
).consumer
consumer.subscribe(TOPIC)

# Queue is thread-safe for concurrent pushes and size reads across the polling threads.
consumed = Queue.new
started_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)

# Several threads polling the SAME consumer concurrently - the exact shape that corrupted the
# shared buffer. Small max_items makes the buffer turn over quickly, widening the race.
threads = THREADS.times.map do
  Thread.new do
    loop do
      break if Process.clock_gettime(Process::CLOCK_MONOTONIC) - started_at > DEADLINE_S

      batch = consumer.poll_batch(200, max_items: 20)
      batch.each { |item| consumed << item if item.is_a?(Rdkafka::Consumer::Message) }

      break if consumed.size >= MESSAGES
    end
  end
end

threads.each(&:join)
total = consumed.size
consumer.close

puts "Consumed #{total} messages across #{THREADS} concurrent poll_batch threads"

if total < MESSAGES
  warn "INCONCLUSIVE: only #{total}/#{MESSAGES} consumed before the deadline (broker too slow?)"
  exit(1)
end

puts "PASS: concurrent poll_batch did not corrupt the shared buffer"
exit(0)
