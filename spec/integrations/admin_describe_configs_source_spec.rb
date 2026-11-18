# frozen_string_literal: true

# This integration test shows how `Admin#describe_configs` tells where each topic config value
# comes from, and verifies it end to end.
#
# `ConfigBindingResult#default` is set only for built-in defaults, so a value that a topic inherits
# from a broker-level override looks the same as a real topic override. `#source` and
# `#source_name` tell them apart, for each synonym too:
#
# - a value set on the topic: `DYNAMIC_TOPIC_CONFIG` (1)
# - a value inherited from a broker override: `DYNAMIC_BROKER_CONFIG` (2)
# - a built-in default: `DEFAULT_CONFIG` (5)
#
# The broker override is set with `incremental_alter_configs` on the broker resource, and removed
# at the end.
#
# Requires a running Kafka broker at localhost:9092.
#
# Exit codes:
# - 0: each config reported the expected source
# - 1: a config reported a wrong source, or the source was missing

require "rdkafka"
require "securerandom"

TOPIC = "it-describe-configs-source-#{SecureRandom.hex(6)}"
BROKER_CONFIG = "log.cleaner.delete.retention.ms"

def alter_broker_config(admin, broker_id, op_type, value = nil)
  admin.incremental_alter_configs(
    [
      {
        resource_type: Rdkafka::Bindings::RD_KAFKA_RESOURCE_BROKER,
        resource_name: broker_id.to_s,
        configs: [{ name: BROKER_CONFIG, value: value, op_type: op_type }]
      }
    ]
  ).wait(max_wait_timeout_ms: 15_000)
end

def describe_topic(admin)
  admin
    .describe_configs([{ resource_type: Rdkafka::Bindings::RD_KAFKA_RESOURCE_TOPIC, resource_name: TOPIC }])
    .wait(max_wait_timeout_ms: 15_000)
    .resources
    .first
    .configs
    .to_h { |config| [config.name, config] }
end

admin = Rdkafka::Config.new("bootstrap.servers": "localhost:9092").admin

broker_id = admin.describe_cluster.wait(max_wait_timeout_ms: 15_000).nodes.first[:id]

begin
  admin.create_topic(TOPIC, 1, 1, "retention.ms" => "123456").wait(max_wait_timeout_ms: 15_000)
  alter_broker_config(admin, broker_id, Rdkafka::Bindings::RD_KAFKA_ALTER_CONFIG_OP_TYPE_SET, "86400001")

  configs = nil
  deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 30

  # The topic and the broker override reach the broker that answers with a short delay
  loop do
    begin
      configs = describe_topic(admin)
      break if configs["delete.retention.ms"]&.value == "86400001"
    rescue Rdkafka::RdkafkaError => e
      raise unless e.code == :unknown_topic_or_part
    end

    exit(1) if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline

    sleep(0.5)
  end

  topic_override = configs.fetch("retention.ms")
  broker_inherited = configs.fetch("delete.retention.ms")
  built_in = configs.fetch("compression.type")

  exit(1) unless topic_override.source == Rdkafka::Bindings::RD_KAFKA_CONFIG_SOURCE_DYNAMIC_TOPIC_CONFIG
  exit(1) unless topic_override.source_name == "DYNAMIC_TOPIC_CONFIG"
  exit(1) unless topic_override.default.zero?

  exit(1) unless broker_inherited.source == Rdkafka::Bindings::RD_KAFKA_CONFIG_SOURCE_DYNAMIC_BROKER_CONFIG
  exit(1) unless broker_inherited.source_name == "DYNAMIC_BROKER_CONFIG"
  exit(1) unless broker_inherited.default.zero?

  exit(1) unless built_in.source == Rdkafka::Bindings::RD_KAFKA_CONFIG_SOURCE_DEFAULT_CONFIG
  exit(1) unless built_in.source_name == "DEFAULT_CONFIG"
  exit(1) unless built_in.default == 1

  synonyms = broker_inherited.synonyms.map { |synonym| [synonym.name, synonym.source_name] }

  exit(1) unless synonyms.include?([BROKER_CONFIG, "DYNAMIC_BROKER_CONFIG"])
  exit(1) unless synonyms.include?([BROKER_CONFIG, "DEFAULT_CONFIG"])
ensure
  alter_broker_config(admin, broker_id, Rdkafka::Bindings::RD_KAFKA_ALTER_CONFIG_OP_TYPE_DELETE)
  admin.delete_topic(TOPIC).wait(max_wait_timeout_ms: 15_000)
  admin.close
end

exit(0)
