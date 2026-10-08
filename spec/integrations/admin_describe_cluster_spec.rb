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

def fail!(message)
  warn "FAIL: #{message}"
  exit(1)
end

admin = Rdkafka::Config.new("bootstrap.servers": "localhost:9092").admin

begin
  report = admin.describe_cluster.wait(max_wait_timeout_ms: 15_000)

  puts "Cluster id: #{report.cluster_id}"
  puts "Controller: #{report.controller.inspect}"
  report.nodes.each { |node| puts "Node: #{node[:id]} #{node[:host]}:#{node[:port]} rack=#{node[:rack].inspect}" }

  fail!("cluster_id is empty") if report.cluster_id.to_s.empty?
  fail!("no nodes described") if report.nodes.empty?

  report.nodes.each do |node|
    fail!("node has no id: #{node.inspect}") unless node[:id].is_a?(Integer)
    fail!("node has no host: #{node.inspect}") if node[:host].to_s.empty?
    fail!("node has no port: #{node.inspect}") unless node[:port].is_a?(Integer) && node[:port].positive?
  end

  fail!("controller #{report.controller.inspect} is not one of the nodes") unless report.nodes.include?(report.controller)
  fail!("authorized operations returned without being requested") unless report.authorized_operations.nil?

  report = admin.describe_cluster(include_authorized_operations: true).wait(max_wait_timeout_ms: 15_000)

  puts "Authorized operations: #{report.authorized_operations.inspect}"

  unless report.authorized_operations.is_a?(Array) && !report.authorized_operations.empty?
    fail!("authorized operations missing: #{report.authorized_operations.inspect}")
  end
ensure
  admin.close
end

puts "PASS: the cluster was described correctly"
exit(0)
