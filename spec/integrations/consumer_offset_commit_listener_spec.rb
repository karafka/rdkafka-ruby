# frozen_string_literal: true

# This integration test verifies that a consumer offset commit listener gets the result of every
# async commit, so a commit that the broker rejects reaches Ruby.
#
# An async `Consumer#commit` returns once the commit is scheduled. Without an offset commit
# callback librdkafka only logs a failed commit (`COMMITFAIL`) and nothing reaches Ruby. With
# `Config#consumer_offset_commit_listener` set, librdkafka serves each commit result when the
# consumer polls (or closes) and the listener gets the error (or nil) and the committed offsets.
#
# Requires a running Kafka broker at localhost:9092.
#
# Scenarios:
# 1. Consume a message and commit its offset asynchronously: the listener gets no error and the
#    committed offset.
# 2. Commit asynchronously an offset of a topic that does not exist: the listener gets
#    `unknown_topic_or_part`, also as the error of that partition.
#
# Exit codes:
# - 0: the listener got both commit results
# - 1: a commit result is missing or wrong

require "rdkafka"
require "securerandom"

$stdout.sync = true

BOOTSTRAP = "localhost:9092"
TOPIC = "it-offset-commit-listener-#{SecureRandom.uuid}"
MISSING_TOPIC = "it-offset-commit-listener-missing-#{SecureRandom.uuid}"
TIMEOUT = 30

# Collects the commit results
class CommitListener
  attr_reader :results

  def initialize
    @results = Queue.new
  end

  def on_offsets_committed(error, list)
    @results << [error, list]
  end
end

def now
  Process.clock_gettime(Process::CLOCK_MONOTONIC)
end

def wait_for_commit_result(consumer, listener)
  deadline = now + TIMEOUT
  consumer.poll(100) while listener.results.empty? && now < deadline

  return listener.results.pop unless listener.results.empty?

  warn "FAIL: no commit result in #{TIMEOUT} s"
  exit(1)
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

listener = CommitListener.new
config = Rdkafka::Config.new(
  "bootstrap.servers": BOOTSTRAP,
  "group.id": "it-offset-commit-listener-#{SecureRandom.uuid}",
  "auto.offset.reset": "earliest",
  "enable.auto.commit": false,
  "enable.auto.offset.store": false
)
config.consumer_offset_commit_listener = listener
consumer = config.consumer

begin
  # 1. A successful async commit of a consumed message
  consumer.subscribe(TOPIC)

  message = nil
  deadline = now + TIMEOUT
  message = consumer.poll(100) while message.nil? && now < deadline

  if message.nil?
    warn "FAIL: no message consumed in #{TIMEOUT} s"
    exit(1)
  end

  consumer.store_offset(message)
  consumer.commit(nil, true)

  error, list = wait_for_commit_result(consumer, listener)
  offsets = list.to_h.fetch(TOPIC, []).map(&:offset)

  unless error.nil? && offsets == [message.offset + 1]
    warn "FAIL: successful commit reported #{error.inspect} with offsets #{offsets.inspect}"
    exit(1)
  end

  puts "Scenario 1: async commit of offset #{offsets.first} reported no error"

  # 2. A failed async commit
  missing = Rdkafka::Consumer::TopicPartitionList.new
  missing.add_topic_and_partitions_with_offsets(MISSING_TOPIC, 0 => 1)
  consumer.commit(missing, true)

  error, list = wait_for_commit_result(consumer, listener)
  partition_errors = list.to_h.fetch(MISSING_TOPIC, []).map { |partition| Rdkafka::RdkafkaError.new(partition.err).code }

  unless error&.code == :unknown_topic_or_part && partition_errors == [:unknown_topic_or_part]
    warn "FAIL: failed commit reported #{error.inspect} with partition errors #{partition_errors.inspect}"
    exit(1)
  end

  puts "Scenario 2: failed async commit reported #{error.code}"
ensure
  consumer.close
  producer.close
  admin.delete_topic(TOPIC).wait
  admin.close
end

puts "PASS: the offset commit listener got the result of each async commit"
exit(0)
