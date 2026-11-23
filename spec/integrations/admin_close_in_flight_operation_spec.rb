# frozen_string_literal: true

# This integration test verifies that `Admin#close` fails with `destroy` the admin operations
# whose requests are in flight, so `wait` on their handles returns at once.
#
# librdkafka drops the result of a request that is in flight when the client is destroyed. The
# background event callback never ran for such an operation, so its handle stayed pending and
# `wait` blocked until its timeout (forever with `max_wait_timeout_ms: nil`).
#
# Scenarios, each on its own mock cluster with the response to the request delayed, so the request
# is sent and waits for the response when the admin is closed:
# - offset_commit: `alter_consumer_group_offsets`
# - list_offsets: `list_offsets`
#
# No broker is required: it uses librdkafka's mock cluster.
#
# Exit codes:
# - 0: in every scenario `wait` raised `destroy` right after `close`
# - 1: in a scenario the request was not in flight at `close`, or `wait` did not raise `destroy`
#   right after it

require_relative "support/mock_cluster"

TOPIC = "it-admin-close-in-flight-#{rand(1_000_000)}"
DELAY_MS = 10_000
WAIT_MS = 2_000

SCENARIOS = {
  offset_commit: lambda do |admin|
    tpl = Rdkafka::Consumer::TopicPartitionList.new
    tpl.add_topic_and_partitions_with_offsets(TOPIC, 0 => 1)
    admin.alter_consumer_group_offsets("it-admin-close-in-flight", tpl)
  end,
  list_offsets: lambda do |admin|
    admin.list_offsets({ TOPIC => [{ partition: 0, offset: :latest }] })
  end
}.freeze

def in_flight?(cluster, api)
  deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 5

  while Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
    return true unless cluster.request_timestamps(api).empty?

    sleep(0.05)
  end

  false
end

SCENARIOS.each do |api, start|
  MockCluster.run do |cluster|
    cluster.create_topic(TOPIC, 1)
    cluster.start_request_tracking
    cluster.delay_requests(api, DELAY_MS)

    admin = cluster.config.admin

    begin
      handle = start.call(admin)

      exit(1) unless in_flight?(cluster, api)

      admin.close

      begin
        handle.wait(max_wait_timeout_ms: WAIT_MS)
        exit(1)
      rescue Rdkafka::RdkafkaError => e
        exit(1) unless e.code == :destroy
      rescue Rdkafka::AbstractHandle::WaitTimeoutError
        exit(1)
      end
    ensure
      admin.close
    end
  end
end
