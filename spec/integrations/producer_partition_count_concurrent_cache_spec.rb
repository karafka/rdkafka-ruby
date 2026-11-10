# frozen_string_literal: true

# This integration test verifies that the process-wide partition count cache stays consistent when
# many threads call `Producer#partition_count` at once, for distinct and shared topics.
#
# Readers of the cache took no lock and inserts for different topics ran in parallel. On JRuby,
# where threads run truly in parallel, the cache lost topics and `partition_count` raised
# `NoMethodError`.
#
# 1. 16 threads start together behind a barrier.
# 2. Each looks up 20 topics of its own and 8 topics shared by all threads. None of them exists,
#    so each lookup runs a real metadata request and caches the result.
# 3. After all threads finish, the cache must hold every topic with the count the threads got.
#
# Requires a running Kafka broker at localhost:9092.
#
# Exit codes:
# - 0: no lookup raised and the cache holds every topic with the right count
# - 1: a lookup raised, or the cache lost a topic or holds a wrong count

require "rdkafka"
require "securerandom"
require "logger"

Rdkafka::Config.logger = Logger.new(nil)

THREADS = 16
TOPICS_PER_THREAD = 20
PREFIX = "partition-count-concurrent-#{SecureRandom.hex(6)}"
SHARED_TOPICS = Array.new(8) { |i| "#{PREFIX}-shared-#{i}" }

Rdkafka::Producer.partition_count_cache = Rdkafka::Producer::PartitionCountCache.new

producer = Rdkafka::Config.new("bootstrap.servers": "localhost:9092").producer
start = Queue.new
results = Queue.new
errors = Queue.new

threads = Array.new(THREADS) do |t|
  Thread.new do
    start.pop

    TOPICS_PER_THREAD.times do |i|
      [
        "#{PREFIX}-#{t}-#{i}",
        SHARED_TOPICS[i % SHARED_TOPICS.size]
      ].each do |topic|
        results << [topic, producer.partition_count(topic)]
      end
    rescue => e
      errors << e
    end
  end
end

THREADS.times { start << true }
threads.each(&:join)

begin
  exit(1) unless errors.empty?

  counts = Rdkafka::Producer.partition_count_cache.to_h
  expected_size = THREADS * TOPICS_PER_THREAD + SHARED_TOPICS.size

  exit(1) unless counts.size == expected_size

  until results.empty?
    topic, count = results.pop
    cached = counts[topic]

    exit(1) unless cached && cached[1] == count
  end

  exit(0)
ensure
  producer.close
end
