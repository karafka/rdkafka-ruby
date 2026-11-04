# frozen_string_literal: true

# This integration test verifies that an offset commit listener with `#on_offsets_commit_failed`
# gets the commits made after the consumer lost its assignment.
#
# A consumer that does not poll within `max.poll.interval.ms` leaves the group. Its commits fail
# from then on, because the broker no longer knows the member. The failure must reach the listener
# for sync and async commits alike, with the error also set on the committed partition.
#
# Requires a running Kafka broker at localhost:9092.
#
# Scenarios:
# 1. Consume a message, then do not poll until the consumer leaves the group.
# 2. Commit its offset synchronously: the commit raises and the listener gets the same error.
# 3. Commit its offset asynchronously: the listener gets a lost assignment error.
#
# Exit codes:
# - 0: both failed commits were reported
# - 1: a failed commit is missing or reported with a wrong error

require "rdkafka"
require "securerandom"

$stdout.sync = true

BOOTSTRAP = "localhost:9092"
TOPIC = "it-offset-commit-failed-listener-lost-#{SecureRandom.uuid}"
TIMEOUT = 30
MAX_POLL_INTERVAL_MS = 6_000
# The code depends on how far the broker got with removing the member
LOST_ASSIGNMENT_CODES = %i[assignment_lost unknown_member_id illegal_generation].freeze

# Collects the failed commits
class FailedCommitListener
  attr_reader :results

  def initialize
    @results = Queue.new
  end

  def on_offsets_commit_failed(error, list)
    @results << [error, list]
  end
end

def now
  Process.clock_gettime(Process::CLOCK_MONOTONIC)
end

def wait_for_failed_commit(consumer, listener)
  deadline = now + TIMEOUT

  while listener.results.empty? && now < deadline
    begin
      consumer.poll(100)
    rescue Rdkafka::RdkafkaError => e
      # The first poll after the consumer left the group reports it
      raise unless e.code == :max_poll_exceeded
    end
  end

  if listener.results.empty?
    warn "FAIL: no failed commit reported in #{TIMEOUT} s"
    exit(1)
  end

  listener.results.pop
end

def partition_errors(list)
  list.to_h.fetch(TOPIC, []).map { |partition| Rdkafka::RdkafkaError.new(partition.err).code }
end

admin = Rdkafka::Config.new("bootstrap.servers": BOOTSTRAP).admin
admin.create_topic(TOPIC, 1, 1).wait

# The topic must be in the metadata before we produce to it
deadline = now + TIMEOUT
begin
  admin.metadata(TOPIC)
rescue Rdkafka::RdkafkaError
  raise if now > deadline

  sleep(0.1)
  retry
end

producer = Rdkafka::Config.new("bootstrap.servers": BOOTSTRAP).producer
producer.produce(topic: TOPIC, payload: "payload", partition: 0).wait

listener = FailedCommitListener.new
config = Rdkafka::Config.new(
  "bootstrap.servers": BOOTSTRAP,
  "group.id": "it-offset-commit-failed-listener-lost-#{SecureRandom.uuid}",
  "auto.offset.reset": "earliest",
  "enable.auto.commit": false,
  "max.poll.interval.ms": MAX_POLL_INTERVAL_MS,
  "session.timeout.ms": MAX_POLL_INTERVAL_MS
)
config.consumer_offset_commit_listener = listener
consumer = config.consumer

begin
  consumer.subscribe(TOPIC)

  message = nil
  deadline = now + TIMEOUT
  message = consumer.poll(100) while message.nil? && now < deadline

  if message.nil?
    warn "FAIL: no message consumed in #{TIMEOUT} s"
    exit(1)
  end

  # 1. Exceed the max poll interval, so the consumer leaves the group
  sleep((MAX_POLL_INTERVAL_MS / 1_000.0) + 3)

  # Leaving the group drops the stored offsets, so commit an explicit list
  list = Rdkafka::Consumer::TopicPartitionList.new
  list.add_topic_and_partitions_with_offsets(TOPIC, 0 => message.offset + 1)

  # 2. A sync commit
  sync_error = nil

  begin
    consumer.commit(list, false)
  rescue Rdkafka::RdkafkaError => e
    sync_error = e
  end

  unless sync_error && LOST_ASSIGNMENT_CODES.include?(sync_error.code)
    warn "FAIL: sync commit after losing the assignment gave #{sync_error.inspect}"
    exit(1)
  end

  error, reported = wait_for_failed_commit(consumer, listener)

  unless error.code == sync_error.code && partition_errors(reported) == [sync_error.code]
    warn "FAIL: sync commit failed with #{sync_error.code}, but reported #{error.inspect} " \
      "with partition errors #{partition_errors(reported).inspect}"
    exit(1)
  end

  puts "Failed sync commit reported #{error.code}"

  # 3. An async commit
  consumer.commit(list, true)

  error, reported = wait_for_failed_commit(consumer, listener)

  unless LOST_ASSIGNMENT_CODES.include?(error.code) && partition_errors(reported) == [error.code]
    warn "FAIL: async commit reported #{error.inspect} " \
      "with partition errors #{partition_errors(reported).inspect}"
    exit(1)
  end

  puts "Failed async commit reported #{error.code}"

  unless listener.results.empty?
    warn "FAIL: also reported #{listener.results.pop.first.inspect}"
    exit(1)
  end
ensure
  consumer.close
  producer.close
  admin.delete_topic(TOPIC).wait
  admin.close
end

puts "PASS: commits after losing the assignment were reported"
exit(0)
