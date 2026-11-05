# frozen_string_literal: true

# This integration test verifies that code written against the deprecated `partitions_count` names
# keeps working: `Producer.partitions_count_cache=`, `Producer::PartitionsCountCache` and
# `Defaults::PARTITIONS_COUNT_CACHE_TTL_MS` resolve to their `partition_count` replacements and
# warn, the old constants stay defined, and a cache installed through them serves `partition_key`
# produces and statistics updates without any deprecation warning.
#
# Requires a running Kafka broker at localhost:9092.
#
# Exit codes:
# - 0: the deprecated names worked and warned, and produce and statistics did not warn
# - 1: a deprecated name failed or did not warn, the installed cache was not used, or
#   produce or statistics used a deprecated name

require "rdkafka"
require "securerandom"
require "stringio"

TOPIC = "deprecated-partitions-count-names-#{SecureRandom.hex(6)}"

# Constant deprecation warnings are shown only with deprecation warnings enabled
Warning[:deprecated] = true

exit(1) unless defined?(Rdkafka::Producer::PartitionsCountCache) == "constant"
exit(1) unless defined?(Rdkafka::Defaults::PARTITIONS_COUNT_CACHE_TTL_MS) == "constant"

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
exit(1) unless warnings.scan("DEPRECATION WARNING").size == 2
exit(1) unless warnings.include?("constant Rdkafka::Producer::PartitionsCountCache is deprecated")
exit(1) unless warnings.include?("constant Rdkafka::Defaults::PARTITIONS_COUNT_CACHE_TTL_MS is deprecated")

statistics = Queue.new
Rdkafka::Config.statistics_callback = ->(stats) { statistics << stats }

config = Rdkafka::Config.new(
  "bootstrap.servers": "localhost:9092",
  "statistics.interval.ms": 100
)
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

  $stderr = StringIO.new

  begin
    producer.produce(topic: TOPIC, payload: "test", partition_key: "key").wait

    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 10
    stats = nil

    until stats&.dig("topics", TOPIC) || Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
      stats = statistics.pop(timeout: 1)
    end

    hot_path_warnings = $stderr.string
  ensure
    $stderr = stderr
  end

  exit(1) unless stats&.dig("topics", TOPIC)
  exit(1) unless cache.to_h.dig(TOPIC, 1) == 3
  exit(1) if hot_path_warnings.include?("deprecated")
ensure
  Rdkafka::Config.statistics_callback = nil
  producer.close
  admin.delete_topic(TOPIC).wait
  admin.close
end
