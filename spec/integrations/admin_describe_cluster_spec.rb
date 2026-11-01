# frozen_string_literal: true

# This integration test shows how to describe a cluster with `Admin#describe_cluster` and verifies
# the result end to end.
#
# `describe_cluster` returns the cluster id, the current controller and every broker node (id,
# host, port and rack) without fetching any topic metadata. With
# `include_authorized_operations: true` it also returns the operations this client may perform on
# the cluster, as `Rdkafka::Bindings::RD_KAFKA_ACL_OPERATION_*` codes.
#
# Requires a running Kafka broker at localhost:9092.
#
# Exit codes:
# - 0: the cluster was described correctly
# - 1: a described value was missing or wrong

require "rdkafka"

$stdout.sync = true

admin = Rdkafka::Config.new("bootstrap.servers": "localhost:9092").admin

begin
  report = admin.describe_cluster.wait(max_wait_timeout_ms: 15_000)

  exit(1) if report.cluster_id.to_s.empty?
  exit(1) if report.nodes.empty?

  report.nodes.each do |node|
    exit(1) unless node[:id].is_a?(Integer)
    exit(1) if node[:host].to_s.empty?
    exit(1) unless node[:port].is_a?(Integer) && node[:port].positive?
  end

  exit(1) unless report.nodes.include?(report.controller)
  exit(1) unless report.authorized_operations.nil?

  report = admin.describe_cluster(include_authorized_operations: true).wait(max_wait_timeout_ms: 15_000)

  exit(1) unless report.authorized_operations.is_a?(Array) && !report.authorized_operations.empty?
ensure
  admin.close
end

exit(0)
