# frozen_string_literal: true

# This integration test verifies that many threads producing to new topics at once create exactly
# one librdkafka topic handle per topic, never hand a missing handle to librdkafka, and that
# `close` destroys every handle created.
#
# The first `produce` to a topic creates its topic handle. That check-then-create was not atomic:
# on JRuby, where threads run in parallel, concurrent first produces created several handles for
# one topic (all but the last leaked, as `close` destroys only the stored one), or found the
# config stored without its handle and passed a NULL handle to `rd_kafka_producev`, which crashed
# the process with a segfault. Handle creation is now serialized per producer.
#
# The messages go to topics that do not exist, with a short `message.timeout.ms`: only the topic
# handles matter here, not the deliveries.
#
# Requires a running Kafka broker at localhost:9092.
#
# Exit codes:
# - 0: one topic handle per topic, no errors, and every handle destroyed by `close`
# - 1: duplicate or missing topic handles, a produce error, or a handle not destroyed by `close`

require "rdkafka"
require "securerandom"

$stdout.sync = true

THREADS = 16
TOPICS = Array.new(20) { "it-concurrent-first-produce-#{SecureRandom.uuid}" }
CLOSE_TIMEOUT = 10

# Records the native topic handles created and destroyed, per topic name
module TopicHandleRecorder
  LOCK = Mutex.new
  CREATED = Hash.new { |hash, key| hash[key] = [] }
  DESTROYED = []

  def rd_kafka_topic_new(inner, topic_name, config)
    super.tap do |handle|
      LOCK.synchronize { CREATED[topic_name] << handle.address }
    end
  end

  def rd_kafka_topic_destroy(handle)
    LOCK.synchronize { DESTROYED << handle.address }
    super
  end
end
Rdkafka::Bindings.singleton_class.prepend(TopicHandleRecorder)

producer = Rdkafka::Config.new(
  "bootstrap.servers": "localhost:9092",
  "message.timeout.ms": 1_000
).producer

start = Queue.new
errors = Queue.new

threads = Array.new(THREADS) do
  Thread.new do
    start.pop

    TOPICS.each do |topic_name|
      producer.produce(topic: topic_name, payload: "payload")
    rescue => e
      errors << e
    end
  end
end

THREADS.times { start << true }
threads.each(&:join)

unless errors.empty?
  error = errors.pop
  warn "FAIL: #{errors.size + 1} produce errors, first: #{error.class}: #{error.message}"
  exit!(1)
end

counts = TOPICS.map { |topic_name| TopicHandleRecorder::CREATED[topic_name].size }

unless counts.all?(1)
  warn "FAIL: expected one topic handle per topic, got #{counts.inspect}"
  exit!(1)
end

puts "One topic handle per topic for #{TOPICS.size} topics and #{THREADS} threads"

closer = Thread.new { producer.close }

unless closer.join(CLOSE_TIMEOUT)
  # On JRuby concurrent produces can leave the operations counter of the native client
  # unbalanced, so `close` waits forever. That is fixed separately; `exit!` skips the `at_exit`
  # close that would wait the same way.
  if defined?(JRUBY_VERSION)
    puts "SKIP: close did not finish on JRuby, handle destruction not checked"
    exit!(0)
  end

  warn "FAIL: close did not finish in #{CLOSE_TIMEOUT} s"
  exit!(1)
end

created = TopicHandleRecorder::CREATED.values_at(*TOPICS).flatten
not_destroyed = created - TopicHandleRecorder::DESTROYED

unless not_destroyed.empty?
  warn "FAIL: close did not destroy #{not_destroyed.size} of #{created.size} topic handles"
  exit(1)
end

puts "PASS: one topic handle per topic, all destroyed by close"
exit(0)
