# frozen_string_literal: true

# This integration test verifies that unsubscribing a share consumer does not leave records it
# acquired locked until their acquisition lock (30s by default) expires:
# - records acquired by a ShareFetch still in flight when unsubscribing reach another member on
#   their first delivery, right away
# - in explicit mode, delivered records that were not acknowledged are released to another member
#   and the consumer can poll again after subscribing again
# - in implicit mode, delivered records are accepted on unsubscribe and not redelivered
# - after subscribing again, records are fetched and acknowledged without errors
#
# librdkafka only leaves the group on unsubscribe; the share sessions, whose close makes the broker
# release the member's acquisitions, are closed on unsubscribe by the bundled librdkafka patch
# (dist/patches/rdkafka_share_unsubscribe_release.patch).
#
# Requires a running Kafka broker with share groups enabled at 127.0.0.1:9092.
#
# Exit codes:
# - 0: All assertions hold (test passes)
# - 1: An assertion failed

require "rdkafka"
require "securerandom"

$stdout.sync = true

BOOTSTRAP = "127.0.0.1:9092"
# Well below the default 30s acquisition lock
RELEASE_DEADLINE = 10

def assert(condition, message)
  return if condition

  puts "FAILED: #{message}"
  exit 1
end

def setup_topic(prefix)
  topic = "#{prefix}-#{SecureRandom.hex(6)}"
  group = "#{prefix}-group-#{SecureRandom.hex(4)}"

  admin = Rdkafka::Config.new("bootstrap.servers": BOOTSTRAP).admin
  admin.create_topic(topic, 1, 1).wait(max_wait_timeout_ms: 15_000)
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

  [topic, group]
end

def produce(producer, topic, count, prefix)
  count.times do |i|
    producer.produce(topic: topic, payload: "#{prefix}-#{i}").wait(max_wait_timeout_ms: 15_000)
  end
end

def share_consumer(group, mode)
  Rdkafka::Config.new(
    "bootstrap.servers": BOOTSTRAP,
    "group.id": group,
    "share.acknowledgement.mode": mode
  ).share_consumer
end

def first_batch(consumer)
  30.times do
    batch = consumer.poll(500)
    return batch unless batch.empty?
  end

  []
end

# Lets a member that has nothing to fetch yet join the group: share group membership and
# assignment come with the heartbeats driven by poll
def join(consumer)
  started = Time.now
  consumer.poll(500) while Time.now - started < 6
end

# Polls until count records arrived or the deadline passed, acknowledging them in explicit mode
def drain(consumer, count, deadline, explicit: true)
  started = Time.now
  received = []

  while received.size < count && Time.now - started < deadline
    consumer.poll(500).each do |message|
      received << message
      consumer.acknowledge(message, :accept) if explicit
    end
  end

  [received, Time.now - started]
end

producer = Rdkafka::Config.new("bootstrap.servers": BOOTSTRAP).producer

# Records acquired by a fetch in flight when unsubscribing
topic, group = setup_topic("share-unsub-inflight")
produce(producer, topic, 1, "first")

member = share_consumer(group, "explicit")
member.subscribe(topic)
first = first_batch(member)
assert(first.size == 1, "expected the first record, got #{first.size}")
first.each { |message| member.acknowledge(message, :accept) }
member.commit_sync
# Nothing is left to fetch, so a long-polling ShareFetch stays in flight
2.times { member.poll(500) }

other = share_consumer(group, "explicit")
other.subscribe(topic)
2.times { other.poll(300) }

member.unsubscribe
produce(producer, topic, 20, "payload")

received, elapsed = drain(other, 20, 25)
counts = received.map(&:delivery_count).tally
assert(
  received.size == 20 && counts == { 1 => 20 },
  "expected 20 records on their first delivery, got #{received.size} (delivery counts #{counts}) " \
  "in #{elapsed.round(1)}s"
)
assert(elapsed < RELEASE_DEADLINE, "records reached the other member only after #{elapsed.round(1)}s")
puts "in-flight fetch: #{received.size} records on first delivery in #{elapsed.round(1)}s"

member.close
other.close

# Explicit mode: delivered records that were not acknowledged
topic, group = setup_topic("share-unsub-explicit")
produce(producer, topic, 5, "unacked")

member = share_consumer(group, "explicit")
member.subscribe(topic)
unacked = first_batch(member)
assert(!unacked.empty?, "expected records before unsubscribing")

other = share_consumer(group, "explicit")
other.subscribe(topic)
join(other)

member.unsubscribe
received, elapsed = drain(other, unacked.size, 25)
assert(
  received.map(&:offset).sort == unacked.map(&:offset).sort,
  "expected the #{unacked.size} unacknowledged records to be released to the other member, " \
  "got #{received.map(&:offset).inspect} in #{elapsed.round(1)}s"
)
assert(elapsed < RELEASE_DEADLINE, "unacknowledged records were released only after #{elapsed.round(1)}s")
other.commit_sync
other.close

# The records are gone from this member, so polling after subscribing again must not fail on
# them not being acknowledged
member.subscribe(topic)
produce(producer, topic, 1, "explicit-after")
begin
  received, = drain(member, 1, 20)
rescue Rdkafka::RdkafkaError => e
  assert(false, "poll after subscribing again failed: #{e.message}")
end
assert(
  received.map(&:payload) == ["explicit-after-0"],
  "expected the new record after subscribing again, got #{received.map(&:payload).inspect}"
)
member.commit_sync
member.close
puts "explicit mode: #{unacked.size} unacknowledged records released in #{elapsed.round(1)}s"

# Implicit mode: delivered records are accepted on unsubscribe
topic, group = setup_topic("share-unsub-implicit")
produce(producer, topic, 5, "implicit")

member = share_consumer(group, "implicit")
member.subscribe(topic)
delivered = first_batch(member)
assert(!delivered.empty?, "expected records before unsubscribing")

other = share_consumer(group, "implicit")
other.subscribe(topic)
join(other)

member.unsubscribe
member.close
received, = drain(other, 1, RELEASE_DEADLINE, explicit: false)
assert(
  received.empty?,
  "accepted records must not be redelivered, got #{received.map(&:payload).inspect}"
)
other.close
puts "implicit mode: #{delivered.size} delivered records accepted on unsubscribe"

# Subscribing again after unsubscribing
topic, group = setup_topic("share-unsub-resubscribe")
member = share_consumer(group, "explicit")
callback_errors = []
member.acknowledgement_commit_callback = ->(_offsets, error) { callback_errors << error if error }

3.times do |round|
  member.subscribe(topic)
  produce(producer, topic, 5, "round-#{round}")

  received, elapsed = drain(member, 5, 30)
  assert(
    received.map(&:payload).sort == 5.times.map { |i| "round-#{round}-#{i}" },
    "round #{round}: expected 5 records, got #{received.map(&:payload).inspect} in #{elapsed.round(1)}s"
  )
  assert(
    received.map(&:delivery_count).all?(1),
    "round #{round}: expected first deliveries, got #{received.map(&:delivery_count).inspect}"
  )

  results = member.commit_sync
  results&.to_h&.each do |result_topic, partitions|
    partitions.each do |partition|
      assert(
        partition.err == Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR,
        "round #{round}: commit_sync reported error #{partition.err} for #{result_topic}/#{partition.partition}"
      )
    end
  end

  member.unsubscribe
end

5.times { member.events_poll(200) }
assert(callback_errors.empty?, "acknowledgement commit callback reported errors: #{callback_errors.inspect}")
member.close
puts "subscribe after unsubscribe: 3 rounds fetched and acknowledged"

producer.close

puts "share consumer unsubscribe release OK"
