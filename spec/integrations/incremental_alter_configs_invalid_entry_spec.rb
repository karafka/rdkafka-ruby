# frozen_string_literal: true

# This integration test verifies that `Admin#incremental_alter_configs` surfaces an entry that
# librdkafka rejects, instead of silently dropping it and reporting success.
#
# `rd_kafka_ConfigResource_add_incremental_config` returns a non-NULL `rd_kafka_error_t` for an
# invalid op_type, an empty/nil name, or a nil value on a non-delete op. That return value used to
# be ignored: the bad entry was dropped, the alter request was still sent and reported success
# (the user believed the config was applied), and the error object leaked. The call now raises and
# frees the error object.
#
# We pass a config entry with an invalid op_type. The rejection happens client-side, before any
# request is sent, so no broker round-trip is needed for the entry itself (we still need a broker
# to construct the admin client / send a real request in the unfixed case).
#
# Requires a running Kafka broker at localhost:9092.
#
# Exit codes:
# - 0: the call raised RdkafkaError for the rejected entry (entry surfaced, not dropped)
# - 1: the call returned without raising (entry silently dropped - bug still present)

require "rdkafka"

$stdout.sync = true

admin = Rdkafka::Config.new("bootstrap.servers": "localhost:9092").admin

resources_with_configs = [
  {
    resource_type: 2, # RD_KAFKA_RESOURCE_TOPIC
    resource_name: "incremental-alter-invalid-probe",
    configs: [
      {
        name: "delete.retention.ms",
        value: "1000",
        op_type: 99 # not a valid RD_KAFKA_ALTER_CONFIG_OP_TYPE_* (0..3)
      }
    ]
  }
]

begin
  admin.incremental_alter_configs(resources_with_configs)
  warn "FAIL: incremental_alter_configs silently accepted an invalid entry instead of raising"
  admin.close
  exit(1)
rescue Rdkafka::RdkafkaError => e
  admin.close
  puts "PASS: incremental_alter_configs raised for the rejected entry (#{e.code})"
  exit(0)
end
