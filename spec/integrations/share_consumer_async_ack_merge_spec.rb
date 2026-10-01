# frozen_string_literal: true

# This integration test verifies that share consumer acknowledgements from several commit_async
# calls, queued while an earlier async acknowledgement request is still in flight, reach the broker
# in ascending offset order.
#
# librdkafka merges every async commit issued for a partition while a request is in flight into
# the batch pending for that partition. Without sorting the merged entries, acknowledging
# interleaved offsets across commits (e.g. every fourth record per commit_async, as multiple
# threads acknowledging records of the same partition do) produces a non-ascending batch that the
# broker rejects as a whole with INVALID_REQUEST, losing the acknowledgements until their
# acquisition locks expire. The bundled librdkafka is patched to sort merged batches
# (dist/patches/rdkafka_share_async_ack_sort.patch).
#
# Covers a single-partition topic and a two-partition topic, where every commit_async sweeps up
# the pending acknowledgements of both partitions.
#
# Requires a running Kafka broker with share groups enabled at 127.0.0.1:9092.
#
# Exit codes:
# - 0: All acknowledgements were accepted in ascending order (test passes)
# - 1: The broker rejected an acknowledgement batch, or a batch was not ascending

require "rdkafka"
require "securerandom"

$stdout.sync = true

BOOTSTRAP = "127.0.0.1:9092"
MESSAGES_PER_PARTITION = 20
# Number of commit_async calls the delivered records are spread across
SLICES = 4

def assert(condition, message)
  return if condition

  puts "FAILED: #{message}"
  exit 1
end

def run_scenario(partitions)
  topic = "share-async-ack-merge-#{SecureRandom.hex(6)}"
  group = "share-async-ack-merge-group-#{SecureRandom.hex(4)}"

  admin = Rdkafka::Config.new("bootstrap.servers": BOOTSTRAP).admin
  admin.create_topic(topic, partitions, 1).wait(max_wait_timeout_ms: 15_000)
  # share.auto.offset.reset is a broker-side group config defaulting to latest; set it to
  # earliest before the group first attaches so the pre-produced records are delivered
  admin.incremental_alter_configs(
    [
      {
        resource_type: Rdkafka::Bindings::RD_KAFKA_RESOURCE_GROUP,
        resource_name: group,
        configs: [{ name: "share.auto.offset.reset", value: "earliest", op_type: 0 }]
      }
    ]
  ).wait(max_wait_timeout_ms: 15_000)
  admin.close

  producer = Rdkafka::Config.new("bootstrap.servers": BOOTSTRAP).producer
  handles = partitions.times.flat_map do |partition|
    MESSAGES_PER_PARTITION.times.map do |i|
      producer.produce(topic: topic, partition: partition, payload: "payload-#{partition}-#{i}")
    end
  end
  handles.each { |handle| handle.wait(max_wait_timeout_ms: 15_000) }
  producer.close

  consumer = Rdkafka::Config.new(
    "bootstrap.servers": BOOTSTRAP,
    "group.id": group,
    "share.acknowledgement.mode": "explicit"
  ).share_consumer

  results = Queue.new
  consumer.acknowledgement_commit_callback = ->(offsets, error) { results << [offsets, error] }
  consumer.subscribe(topic)

  # In explicit mode every delivered record must be acknowledged before the next poll. Each
  # batch is spread across SLICES commit_async calls so the offsets merged into the batch pending
  # for its partition interleave: only the first commit_async goes out immediately, the following
  # ones queue up behind it.
  messages = []
  60.times do
    batch = consumer.poll(1_000)
    next if batch.empty?

    batch.each do |message|
      assert(
        message.is_a?(Rdkafka::ShareConsumer::Message),
        "expected a ShareConsumer::Message, got #{message.class}: #{message}"
      )
    end

    slices = Hash.new { |hash, key| hash[key] = [] }
    batch.group_by(&:partition).each_value do |partition_messages|
      partition_messages.each_with_index { |message, i| slices[i % SLICES] << message }
    end

    slices.each_value do |slice|
      slice.each { |message| consumer.acknowledge(message, :accept) }
      consumer.commit_async
    end

    messages.concat(batch)
    break if messages.size >= partitions * MESSAGES_PER_PARTITION
  end

  consumer.commit_sync

  assert(
    messages.map(&:partition).uniq.sort == partitions.times.to_a,
    "expected records from all #{partitions} partition(s), got #{messages.map(&:partition).tally.inspect}"
  )

  expected = messages.map { |message| [message.partition, message.offset] }.sort
  acknowledged = []
  errors = []

  20.times do
    consumer.poll(500)

    until results.empty?
      offsets, error = results.pop
      errors << [error, offsets] if error

      offsets.each do |partition_offsets|
        assert(
          partition_offsets[:offsets] == partition_offsets[:offsets].sort,
          "partition #{partition_offsets[:partition]} acknowledgements were not ascending: " \
          "#{partition_offsets[:offsets].inspect} (error: #{error.inspect})"
        )

        partition_offsets[:offsets].each do |offset|
          acknowledged << [partition_offsets[:partition], offset]
        end
      end
    end

    break if errors.any? || acknowledged.size >= expected.size
  end

  assert(
    errors.empty?,
    "broker rejected acknowledgements: #{errors.map { |error, offsets| "#{error.code} #{offsets.inspect}" }.join("; ")}"
  )
  assert(
    acknowledged.sort == expected,
    "expected acknowledgements for #{expected.inspect}, got #{acknowledged.sort.inspect}"
  )

  consumer.close

  puts "#{partitions} partition(s): #{expected.size} records acknowledged, #{SLICES} commit_async calls per batch"
end

run_scenario(1)
run_scenario(2)

puts "share consumer async acknowledgement merge OK"
