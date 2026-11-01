# frozen_string_literal: true

# This integration test shows what `Admin#list_consumer_group_offsets` returns for a consumer group
# that does not exist, as a counterexample to `admin_list_consumer_group_offsets_spec.rb`.
#
# The call does not raise for an unknown group. Without a partition list it returns no partitions.
# With a partition list it returns every requested partition with a nil offset and no error, the
# same as for a group that has not committed. Reading the offsets does not create the group.
#
# Requires a running Kafka broker at localhost:9092.
#
# Exit codes:
# - 0: the unknown group gave no offsets and no error, and was not created
# - 1: the call raised, returned an offset or an error, or created the group

require "rdkafka"
require "securerandom"

$stdout.sync = true

PARTITIONS = 2

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

topic_name = "it-list-cg-offsets-unknown-#{SecureRandom.hex(6)}"
group_name = "it-list-cg-offsets-unknown-#{SecureRandom.hex(6)}"

admin = Rdkafka::Config.new("bootstrap.servers": "localhost:9092").admin

begin
  admin.create_topic(topic_name, PARTITIONS, 1).wait(max_wait_timeout_ms: 15_000)

  # Scenario 1: no partition list
  report = list_consumer_group_offsets_settled(admin, group_name)

  exit(1) unless report.group_name == group_name
  exit(1) unless report.partitions.empty?

  # Scenario 2: an explicit partition list of an existing topic
  tpl = Rdkafka::Consumer::TopicPartitionList.new
  tpl.add_topic(topic_name, PARTITIONS)

  report = list_consumer_group_offsets_settled(admin, group_name, tpl)

  exit(1) unless report.partitions.map { |part| part[:partition] }.sort == Array.new(PARTITIONS) { |p| p }
  exit(1) unless report.partitions.all? { |part| part[:offset].nil? }
  exit(1) unless report.partitions.all? { |part| part[:error].nil? }

  # Scenario 3: the reads did not create the group
  groups = admin.list_consumer_groups.wait(max_wait_timeout_ms: 15_000).groups.map { |group| group[:group_id] }

  exit(1) if groups.include?(group_name)
rescue Rdkafka::RdkafkaError
  exit(1)
ensure
  begin
    admin.delete_topic(topic_name).wait(max_wait_timeout_ms: 15_000)
  rescue Rdkafka::RdkafkaError
    nil
  end

  admin.close
end

exit(0)
