# frozen_string_literal: true

# This integration test verifies that an offset commit listener with only
# `#on_offsets_commit_failed` gets only the failed commits, and that the commit offsets are not
# converted to Ruby for the other commits.
#
# librdkafka passes an entry for every assigned partition to the offset commit callback, also for
# a commit with nothing new (`no_offset`). Converting it on every commit costs time and
# allocations that grow with the assignment, so a listener that only wants failures should not
# pay for it.
#
# Requires a running Kafka broker at localhost:9092.
#
# Scenarios:
# 1. Consume a message and commit its offset synchronously: not reported.
# 2. Commit asynchronously with nothing new stored (`no_offset`): not reported.
# 3. Commit asynchronously an offset of a topic that does not exist: reported with
#    `unknown_topic_or_part`, also as the error of that partition.
# The commit offsets are converted to Ruby once, for scenario 3 only.
#
# Exit codes:
# - 0: only the failed commit was reported
# - 1: a commit was reported when it should not be, or the failed commit is missing or wrong

require "rdkafka"
require "securerandom"

$stdout.sync = true

BOOTSTRAP = "localhost:9092"
TOPIC = "it-offset-commit-failed-listener-#{SecureRandom.uuid}"
MISSING_TOPIC = "it-offset-commit-failed-listener-missing-#{SecureRandom.uuid}"
TIMEOUT = 30

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

# Counts the conversions of native commit offsets to Ruby
module FromNativeTplCounter
  class << self
    attr_accessor :count
  end

  self.count = 0

  def from_native_tpl(*)
    FromNativeTplCounter.count += 1
    super
  end
end

Rdkafka::Consumer::TopicPartitionList.singleton_class.prepend(FromNativeTplCounter)

def now
  Process.clock_gettime(Process::CLOCK_MONOTONIC)
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
  "group.id": "it-offset-commit-failed-listener-#{SecureRandom.uuid}",
  "auto.offset.reset": "earliest",
  "enable.auto.commit": false,
  "enable.auto.offset.store": false
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

  # Assignment also converts a list, so count only from the commits on
  FromNativeTplCounter.count = 0

  # 1. A successful sync commit
  consumer.store_offset(message)
  consumer.commit(nil, false)

  # 2. An async commit with nothing new stored
  consumer.commit(nil, true)

  # 3. A failed async commit
  missing = Rdkafka::Consumer::TopicPartitionList.new
  missing.add_topic_and_partitions_with_offsets(MISSING_TOPIC, 0 => 1)
  consumer.commit(missing, true)

  deadline = now + TIMEOUT
  consumer.poll(100) while listener.results.empty? && now < deadline

  if listener.results.empty?
    warn "FAIL: no failed commit reported in #{TIMEOUT} s"
    exit(1)
  end

  error, list = listener.results.pop
  partition_errors = list.to_h.fetch(MISSING_TOPIC, []).map { |partition| Rdkafka::RdkafkaError.new(partition.err).code }

  unless error.code == :unknown_topic_or_part && partition_errors == [:unknown_topic_or_part]
    warn "FAIL: failed commit reported #{error.inspect} with partition errors #{partition_errors.inspect}"
    exit(1)
  end

  # Results are served in order, so the other commits were served before the failed one
  unless listener.results.empty?
    warn "FAIL: also reported #{listener.results.pop.first.inspect}"
    exit(1)
  end

  # The list built for the reported failure is the only conversion
  unless FromNativeTplCounter.count == 1
    warn "FAIL: commit offsets were converted #{FromNativeTplCounter.count} times, expected 1"
    exit(1)
  end

  puts "Failed async commit reported #{error.code}; successful and no_offset commits not reported"
ensure
  consumer.close
  producer.close
  admin.delete_topic(TOPIC).wait
  admin.close
end

puts "PASS: only the failed commit was reported"
exit(0)
