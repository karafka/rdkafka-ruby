# frozen_string_literal: true

# This integration test verifies that `Admin#close` returns while an admin operation is still
# pending, and that the operation's handle then fails with `destroy`.
#
# `rd_kafka_destroy` fails the pending operation and runs the background event callback on the
# librdkafka background thread. That callback needs the GVL. `rd_kafka_destroy` was attached
# without `blocking: true`, so the closing thread held the GVL while it waited for the background
# thread, and the process froze.
#
# `create_topic` runs against an unreachable broker, so the request stays queued. The check runs
# in a child process that is killed when it does not finish in time (a frozen process cannot time
# itself out).
#
# No broker is required.
#
# Exit codes:
# - 0: `close` returned and the handle failed with `destroy`
# - 1: `close` did not return, or the handle did not fail with `destroy`

DEADLINE = 60
TOPIC = "it-admin-close-pending-#{rand(1_000_000)}"

if ENV["CHILD"]
  require "rdkafka"

  admin = Rdkafka::Config.new("bootstrap.servers": "127.0.0.1:9095").admin
  handle = admin.create_topic(TOPIC, 1, 1)

  sleep(0.5)

  admin.close

  begin
    handle.wait(max_wait_timeout_ms: 5_000)
    exit(1)
  rescue Rdkafka::RdkafkaError => e
    exit(1) unless e.code == :destroy
  end

  exit
end

pid = Process.spawn(
  { "CHILD" => "1" },
  RbConfig.ruby, "-I#{File.expand_path("../../lib", __dir__)}", __FILE__,
  out: File::NULL, err: File::NULL
)

deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + DEADLINE
status = nil

while status.nil? && Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
  _, status = Process.waitpid2(pid, Process::WNOHANG)
  sleep(0.1) if status.nil?
end

if status.nil?
  Process.kill(:KILL, pid)
  Process.wait(pid)
  exit(1)
end

exit(1) unless status.success?
