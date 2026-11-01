# frozen_string_literal: true

# This integration test shows how to compute the lag of another consumer group with
# `Admin#list_consumer_group_offsets` and `Admin#list_offsets`, and verifies it end to end.
#
# The group has a live member while the admin reads its committed offsets. The admin must not
# join the group: the member keeps its assignment and gets no rebalance.
#
# Requires a running Kafka broker at localhost:9092.
#
# Exit codes:
# - 0: the offsets and lag were read correctly without disturbing the group
# - 1: an offset or the lag was wrong, or the group was rebalanced

require "rdkafka"
require "securerandom"

$stdout.sync = true

PARTITIONS = 2
MESSAGES_PER_PARTITION = 10
COMMITTED = { 0 => 4, 1 => 7 }.freeze

# A broker that just started may not know the group coordinator yet
def list_consumer_group_offsets_settled(admin, group_name, topic_partition_list = nil)
  deadline = Time.now + 30

  begin
    admin.list_consumer_group_offsets(group_name, topic_partition_list).wait(max_wait_timeout_ms: 15_000)
  rescue Rdkafka::RdkafkaError => e
    coordinator_errors = %i[coordinator_load_in_progress not_coordinator coordinator_not_available]
    raise unless coordinator_errors.include?(e.code) && Time.now < deadline

    sleep(0.5)
    retry
  end
end

# Counts every assignment the live member receives
class AssignmentCounter
  attr_reader :count

  def initialize
    @count = 0
  end

  def on_partitions_assigned(_list)
    @count += 1
  end

  def on_partitions_revoked(_list)
    nil
  end
end

topic_name = "it-list-cg-offsets-#{SecureRandom.hex(6)}"
group_name = "it-list-cg-offsets-#{SecureRandom.hex(6)}"

admin = Rdkafka::Config.new("bootstrap.servers": "localhost:9092").admin
producer = Rdkafka::Config.new("bootstrap.servers": "localhost:9092").producer

counter = AssignmentCounter.new
consumer_config = Rdkafka::Config.new(
  "bootstrap.servers": "localhost:9092",
  "group.id": group_name,
  "enable.auto.commit": false,
  "auto.offset.reset": "earliest"
)
consumer_config.consumer_rebalance_listener = counter
consumer = consumer_config.consumer

begin
  admin.create_topic(topic_name, PARTITIONS, 1).wait(max_wait_timeout_ms: 15_000)

  PARTITIONS.times do |partition|
    MESSAGES_PER_PARTITION.times do |i|
      producer.produce(topic: topic_name, payload: "m#{i}", partition: partition).wait(max_wait_timeout_ms: 15_000)
    end
  end

  consumer.subscribe(topic_name)

  deadline = Time.now + 30
  consumer.poll(100) while consumer.assignment.empty? && Time.now < deadline
  exit(1) if consumer.assignment.empty?

  tpl = Rdkafka::Consumer::TopicPartitionList.new
  tpl.add_topic_and_partitions_with_offsets(topic_name, COMMITTED)
  consumer.commit(tpl)

  assignments_before = counter.count

  report = list_consumer_group_offsets_settled(admin, group_name)
  committed = report.partitions.to_h { |part| [part[:partition], part[:offset]] }

  end_offsets = admin
    .list_offsets({ topic_name => Array.new(PARTITIONS) { |p| { partition: p, offset: :latest } } })
    .wait(max_wait_timeout_ms: 15_000)
    .offsets
    .to_h { |info| [info[:partition], info[:offset]] }

  lag = committed.to_h { |partition, offset| [partition, end_offsets.fetch(partition) - offset] }

  exit(1) unless report.group_name == group_name
  exit(1) unless committed == COMMITTED
  exit(1) unless lag == COMMITTED.transform_values { |offset| MESSAGES_PER_PARTITION - offset }

  # Give a rebalance, if any was triggered, the time to reach the member
  deadline = Time.now + 5
  consumer.poll(100) while Time.now < deadline

  exit(1) unless counter.count == assignments_before
  exit(1) if consumer.assignment.empty?
ensure
  consumer.close
  producer.close

  begin
    admin.delete_topic(topic_name).wait(max_wait_timeout_ms: 15_000)
  rescue Rdkafka::RdkafkaError
    nil
  end

  admin.close
end

exit(0)
