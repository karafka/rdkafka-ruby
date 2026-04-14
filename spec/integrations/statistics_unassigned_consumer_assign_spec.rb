# frozen_string_literal: true

# This integration test verifies that with statistics.unassigned.include=false
# and the assign() API (manual partition assignment), only the explicitly
# assigned partitions appear in statistics — not all topic partitions.
#
# A consumer manually assigns 10 out of 1000 partitions. With the filter
# enabled, only those 10 partitions should appear in the statistics because
# the remaining 990 have fetch_state=NONE.
#
# Requires a running Kafka broker at localhost:9092.
#
# Exit codes:
# - 0: Only assigned partitions are present in filtered stats (test passes)
# - 1: Extra or missing partitions reported (test fails)

require "rdkafka"
require "securerandom"
require "json"

$stdout.sync = true

BOOTSTRAP = "localhost:9092"
TOPIC = "stats-integration-assign-#{SecureRandom.hex(6)}"
PARTITIONS = 1_000
ASSIGNED_PARTITIONS = (0..9).to_a

admin = Rdkafka::Config.new("bootstrap.servers": BOOTSTRAP).admin
admin.create_topic(TOPIC, PARTITIONS, 1).wait(max_wait_timeout_ms: 15_000)

10.times do
  admin.metadata(TOPIC)
  break
rescue Rdkafka::RdkafkaError
  sleep 0.5
end

# --- Consumer using assign() with filter enabled ---
filtered_stats = []
Rdkafka::Config.statistics_callback = ->(published) { filtered_stats << published }

filtered_consumer = Rdkafka::Config.new(
  "bootstrap.servers": BOOTSTRAP,
  "group.id": "stats-assign-filtered-#{SecureRandom.hex(4)}",
  "auto.offset.reset": "earliest",
  "statistics.interval.ms": 100,
  "statistics.unassigned.include": false
).consumer

tpl = Rdkafka::Consumer::TopicPartitionList.new
tpl.add_topic(TOPIC, ASSIGNED_PARTITIONS)
filtered_consumer.assign(tpl)

(60 * 20).times do
  break if filtered_stats.any? { |s|
    partitions = (s["topics"][TOPIC] || {}).fetch("partitions", {})
    partitions.keys.any? { |k| k != "-1" }
  }
  begin
    filtered_consumer.poll(50)
  rescue Rdkafka::RdkafkaError
    nil
  end
end

filtered_consumer.close
Rdkafka::Config.statistics_callback = nil

# --- Consumer using assign() without filter (baseline) ---
unfiltered_stats = []
Rdkafka::Config.statistics_callback = ->(published) { unfiltered_stats << published }

unfiltered_consumer = Rdkafka::Config.new(
  "bootstrap.servers": BOOTSTRAP,
  "group.id": "stats-assign-unfiltered-#{SecureRandom.hex(4)}",
  "auto.offset.reset": "earliest",
  "statistics.interval.ms": 100,
  "statistics.unassigned.include": true
).consumer

unfiltered_tpl = Rdkafka::Consumer::TopicPartitionList.new
unfiltered_tpl.add_topic(TOPIC, ASSIGNED_PARTITIONS)
unfiltered_consumer.assign(unfiltered_tpl)

(60 * 20).times do
  break if unfiltered_stats.any? { |s|
    (s["topics"][TOPIC] || {}).fetch("partitions", {}).size > 100
  }
  begin
    unfiltered_consumer.poll(50)
  rescue Rdkafka::RdkafkaError
    nil
  end
end

unfiltered_consumer.close
Rdkafka::Config.statistics_callback = nil

# --- Cleanup ---
begin
  admin.delete_topic(TOPIC).wait(max_wait_timeout_ms: 15_000)
rescue Rdkafka::RdkafkaError
  nil
end
admin.close

# --- Results ---
filtered_stat = filtered_stats.reverse.find do |s|
  partitions = (s["topics"][TOPIC] || {}).fetch("partitions", {})
  partitions.keys.any? { |k| k != "-1" }
end

unfiltered_stat = unfiltered_stats.reverse.find do |s|
  (s["topics"][TOPIC] || {}).fetch("partitions", {}).size > 100
end

if filtered_stat.nil?
  puts "FAIL: No filtered stats with partition data found"
  exit(1)
end

filtered_partitions = filtered_stat["topics"][TOPIC]["partitions"]
filtered_ids = filtered_partitions.keys.reject { |k| k == "-1" }.map(&:to_i).sort
filtered_count = filtered_ids.size

puts
puts "Consumer with assign() — #{ASSIGNED_PARTITIONS.size}/#{PARTITIONS} partitions assigned:"
puts "  Filter enabled:  #{filtered_count} partitions reported (#{filtered_ids.inspect})"

if unfiltered_stat
  unfiltered_partitions = unfiltered_stat["topics"][TOPIC]["partitions"]
  unfiltered_count = unfiltered_partitions.keys.count { |k| k != "-1" }
  puts "  Filter disabled: #{unfiltered_count} partitions reported"
  puts "  JSON size filtered:   #{JSON.generate(filtered_stat).bytesize} bytes"
  puts "  JSON size unfiltered: #{JSON.generate(unfiltered_stat).bytesize} bytes"
end
puts

if filtered_count != ASSIGNED_PARTITIONS.size
  puts "FAIL: Expected #{ASSIGNED_PARTITIONS.size} partitions, got #{filtered_count}"
  exit(1)
end

if filtered_ids != ASSIGNED_PARTITIONS
  puts "FAIL: Expected partitions #{ASSIGNED_PARTITIONS.inspect}, got #{filtered_ids.inspect}"
  exit(1)
end

if unfiltered_stat
  unfiltered_count = unfiltered_stat["topics"][TOPIC]["partitions"].keys.count { |k| k != "-1" }
  if unfiltered_count < PARTITIONS
    puts "FAIL: Unfiltered baseline should report all #{PARTITIONS} partitions, got #{unfiltered_count}"
    exit(1)
  end
end

puts "PASS: Only #{filtered_count} assigned partitions present in filtered stats (out of #{PARTITIONS} total)"
exit(0)
