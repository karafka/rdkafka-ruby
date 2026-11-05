# frozen_string_literal: true

# This integration test verifies that code written against the deprecated `partitions_count` names
# keeps working: `Producer.partitions_count_cache=`, `Producer::PartitionsCountCache` and
# `Defaults::PARTITIONS_COUNT_CACHE_TTL_MS` resolve to their `partition_count` replacements and
# warn, and a cache installed through them serves `partition_key` produces.
#
# Requires a running Kafka broker at localhost:9092.
#
# Exit codes:
# - 0: the deprecated names worked, warned and the installed cache served the produce
# - 1: a deprecated name failed, did not warn or the installed cache was not used

require "rdkafka"
require "securerandom"
require "stringio"

TOPIC = "deprecated-partitions-count-names-#{SecureRandom.hex(6)}"

stderr = $stderr
$stderr = StringIO.new

begin
  cache = Rdkafka::Producer::PartitionsCountCache.new(
    ttl_ms: Rdkafka::Defaults::PARTITIONS_COUNT_CACHE_TTL_MS
  )
  Rdkafka::Producer.partitions_count_cache = cache
  installed = Rdkafka::Producer.partitions_count_cache
  warnings = $stderr.string
ensure
  $stderr = stderr
end

exit(1) unless cache.instance_of?(Rdkafka::Producer::PartitionCountCache)
exit(1) unless cache.ttl_ms == Rdkafka::Defaults::PARTITION_COUNT_CACHE_TTL_MS
exit(1) unless installed.equal?(cache)
exit(1) unless Rdkafka::Producer.partition_count_cache.equal?(cache)
exit(1) unless warnings.scan("DEPRECATION WARNING").size == 4

config = Rdkafka::Config.new("bootstrap.servers": "localhost:9092")
admin = config.admin
producer = config.producer

begin
  admin.create_topic(TOPIC, 3, 1).wait

  deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 10

  begin
    admin.metadata(TOPIC)
  rescue Rdkafka::RdkafkaError => e
    raise unless e.code == :unknown_topic_or_part
    raise if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline

    sleep(0.1)
    retry
  end

  producer.produce(topic: TOPIC, payload: "test", partition_key: "key").wait

  exit(1) unless cache.to_h.dig(TOPIC, 1) == 3
ensure
  producer.close
  admin.delete_topic(TOPIC).wait
  admin.close
end
