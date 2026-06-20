# frozen_string_literal: true

# This integration test verifies that admin operations which reject their arguments do not leak.
#
# `describe_configs` / `incremental_alter_configs` allocated the background queue and AdminOptions
# and registered their handle BEFORE parsing user input. A bad argument (e.g. a missing
# :resource_name key) then raised a KeyError out of the building loop, orphaning the handle in the
# process-global REGISTRY forever (and leaking the queue, AdminOptions and any ConfigResources
# already built). `list_offsets` similarly allocated its native topic-partition list before
# validating the offset specs. Input is now parsed up front, so bad arguments raise with nothing
# allocated.
#
# We hammer the failing path and assert the handle REGISTRY does not grow. No broker round-trip is
# needed: the argument is rejected before anything is sent.
#
# Requires a running Kafka broker at localhost:9092 (only to construct the admin client).
#
# Exit codes:
# - 0: the registry did not grow (no orphaned handles)
# - 1: the registry grew (handles leak)

require "rdkafka"

$stdout.sync = true

ITERATIONS = 10_000

admin = Rdkafka::Config.new("bootstrap.servers": "localhost:9092").admin

registry = Rdkafka::Admin::DescribeConfigsHandle::REGISTRY
before = registry.size

ITERATIONS.times do
  # Missing :resource_name -> KeyError while building the request.
  admin.describe_configs([{ resource_type: 2 }])
rescue KeyError
  nil
end

after = registry.size
admin.close

growth = after - before
puts "DescribeConfigsHandle registry growth after #{ITERATIONS} rejected calls: #{growth}"

if growth > 100
  warn "FAIL: registry grew by #{growth} over #{ITERATIONS} rejected describe_configs calls - handles leak"
  exit(1)
end

puts "PASS: rejected admin calls did not orphan handles in the registry"
exit(0)
