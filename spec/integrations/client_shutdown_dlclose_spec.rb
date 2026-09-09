# frozen_string_literal: true

# This integration test verifies that clients still alive at process exit are destroyed before
# librdkafka can be unmapped.
#
# `Rdkafka::Bindings` loads librdkafka through ffi, which holds the only `dlopen` reference to it.
# Ruby's shutdown sweep frees every object regardless of reachability, so it frees ffi's
# `DynamicLibrary` and `dlclose`s librdkafka while librdkafka's own threads are still parked
# inside that mapping. The first thread to wake returns into unmapped memory and the process dies
# with SIGSEGV. GC finalizers cannot prevent this on their own: the finalizer that closes a client
# and the free that unmaps the library belong to the same unordered sweep, which is why the crash
# is intermittent. The gem now closes registered clients from an `at_exit` hook, which runs before
# that sweep begins, while Ruby can still stop and join threads.
#
# Racing the sweep reproduces the crash only intermittently, so each child unmaps librdkafka
# itself instead. The unmap is registered as an `at_exit` hook before the gem is required:
# `at_exit` is LIFO, so it runs immediately after the gem's own cleanup hook and reproduces the
# shutdown ordering exactly, leaving thread lifetime as the only variable between the two modes.
#
#   leaked  - a bare `rd_kafka_new` handle with no Ruby owner, which no cleanup can reach. It
#             must crash; if it does not, the unmap is not working here and the managed result
#             would prove nothing, so the spec skips rather than passing.
#   managed - ordinary clients, closed by the gem's hook before the identical unmap.
#
# No broker is needed: librdkafka spawns its threads whether or not the cluster answers.
# Linux only: macOS has no equivalent unmap-on-exit path and musl's `dlclose` is a no-op, so both
# report a skip rather than a failure.
#
# Exit codes:
# - 0: every managed child survived, or the platform cannot unmap librdkafka (skip)
# - 1: a managed child crashed - live clients are reaching the shutdown sweep again

require "ffi"

CHILD_MODE = ENV["RDKAFKA_CHILD_MODE"]
RUNS = Integer(ENV.fetch("RUNS", "3"))
CLIENTS = Integer(ENV.fetch("CLIENTS", "4"))
WAIT_SECONDS = Float(ENV.fetch("WAIT_SECONDS", "5"))
BOOTSTRAP = "127.0.0.1:1"

# Bound on how many references may be holding librdkafka open. Dropping a fixed two would leave
# the library mapped if anything else took a reference, which would silently turn the canary green.
MAX_DLCLOSE = 8

# `dlclose` through libc directly: the reference that has to go is ffi's, not one of ours.
module Dl
  extend FFI::Library

  ffi_lib FFI::Library::LIBC
  attach_function :dlopen, [:string, :int], :pointer
  attach_function :dlclose, [:pointer], :int

  RTLD_LAZY = 0x1
  RTLD_NOLOAD = 0x4
end

def librdkafka_mapped?
  File.readlines("/proc/self/maps").grep(/librdkafka/).any?
end

def librdkafka_threads
  Dir.children("/proc/self/task").filter_map do |tid|
    name = File.read("/proc/self/task/#{tid}/comm").strip
    name if name.start_with?("rdk")
  rescue Errno::ENOENT
    nil
  end
end

def unmap_librdkafka(path)
  handle = Dl.dlopen(path, Dl::RTLD_LAZY | Dl::RTLD_NOLOAD)
  return false if handle.null?

  MAX_DLCLOSE.times do
    return true unless librdkafka_mapped?

    Dl.dlclose(handle)
  end

  !librdkafka_mapped?
end

# Starts librdkafka's threads without creating a Ruby object that owns them. Deliberately not
# built through `Rdkafka::Config`, so the handle is invisible to any shutdown cleanup the gem
# implements and the canary cannot be neutralised by a change to that cleanup.
def leak_native_handle
  config = Rdkafka::Bindings.rd_kafka_conf_new
  error = FFI::MemoryPointer.from_string(" " * 256)

  Rdkafka::Bindings.rd_kafka_conf_set(config, "bootstrap.servers", BOOTSTRAP, error, 256)
  Rdkafka::Bindings.rd_kafka_conf_set(config, "log_level", "0", error, 256)

  handle = Rdkafka::Bindings.rd_kafka_new(:rd_kafka_producer, config, error, 256)
  raise error.read_string if handle.null?

  handle
end

if CHILD_MODE
  $stdout.sync = true

  # Filled in after the require below. The hook has to be registered first so that LIFO ordering
  # places it after the gem's own cleanup, which is the point in shutdown being reproduced.
  librdkafka = {}

  at_exit do
    live = librdkafka_threads

    unless unmap_librdkafka(librdkafka.fetch(:path))
      puts "UNMAP_UNAVAILABLE"
      next
    end

    puts "unmapped with #{live.length} librdkafka thread(s) live: #{live.sort.join(", ")}"

    # Give a parked thread the chance to wake up and return into the hole.
    sleep WAIT_SECONDS

    puts "SURVIVED"
  end

  require "rdkafka"

  librdkafka[:path] = Rdkafka::Bindings
    .ffi_libraries
    .map(&:name)
    .find { |name| name.include?("librdkafka") }

  raise "librdkafka is not among the loaded ffi libraries" unless librdkafka[:path]

  if CHILD_MODE == "leaked"
    CLIENTS.times { leak_native_handle }
  else
    config = { "bootstrap.servers" => BOOTSTRAP, "log_level" => 0 }

    # Held in a global so they stay reachable: an unreferenced client could be collected and
    # closed by its finalizer before the hook runs, which would not exercise the hook at all.
    $clients = Array.new(CLIENTS) do |i|
      Rdkafka::Config.new(config.merge("group.id" => "shutdown-dlclose-#{i}")).consumer
    end
    $clients.concat(Array.new(CLIENTS) { Rdkafka::Config.new(config).producer })
  end

  # Let librdkafka spin up its main/broker threads.
  sleep 1

  puts "BODY_DONE mode=#{CHILD_MODE} threads=#{librdkafka_threads.length}"
  exit(0)
end

$stdout.sync = true

unless RUBY_PLATFORM.include?("linux")
  puts "SKIP: #{RUBY_PLATFORM} has no exit-time dlclose path for this hazard"
  exit(0)
end

def run_child(mode)
  read, write = IO.pipe

  env = {
    "RDKAFKA_CHILD_MODE" => mode,
    "WAIT_SECONDS" => WAIT_SECONDS.to_s,
    "CLIENTS" => CLIENTS.to_s
  }

  pid = Process.spawn(
    env,
    RbConfig.ruby, "-I#{File.expand_path("../../lib", __dir__)}", __FILE__,
    out: write, err: write,
    rlimit_core: 0
  )
  write.close
  output = read.read
  read.close
  _, status = Process.waitpid2(pid)

  if status.signaled?
    [:crashed, "#{output}died on SIG#{Signal.signame(status.termsig)}\n"]
  elsif output.include?("UNMAP_UNAVAILABLE")
    [:unmap_unavailable, output]
  elsif status.success? && output.include?("SURVIVED")
    [:survived, output]
  else
    [:error, "#{output}exited with #{status.exitstatus}\n"]
  end
end

def tally(mode)
  results = Array.new(RUNS) { run_child(mode) }
  counts = results.map(&:first).tally
  puts "#{mode}: #{counts.map { |outcome, n| "#{outcome}=#{n}" }.join(" ")}"
  [counts, results]
end

puts "Running #{RUNS} children per mode, #{CLIENTS} clients each"

leaked_counts, leaked_results = tally("leaked")

if leaked_counts[:unmap_unavailable] == RUNS
  puts "SKIP: librdkafka cannot be unmapped on this platform (musl dlclose is a no-op)"
  exit(0)
end

unless leaked_counts[:crashed] == RUNS
  warn "SKIP: leaked native handles did not crash on the unmap, so this environment does not " \
    "reproduce the hazard and the managed result would prove nothing"
  leaked_results.each { |outcome, output| warn "  #{outcome}: #{output.lines.last(2).join("  ")}" }
  exit(0)
end

managed_counts, managed_results = tally("managed")
unexpected = managed_counts.except(:survived)

if unexpected.any?
  warn "FAIL: #{unexpected.values.sum}/#{RUNS} managed children did not survive the unmap - " \
    "live clients are reaching the shutdown sweep again"
  managed_results.each { |outcome, output| warn "  #{outcome}: #{output}" }
  exit(1)
end

puts "PASS: leaked native handles crash on the unmap (#{leaked_counts[:crashed]}/#{RUNS}) while the " \
  "gem's cleanup keeps every managed child alive (#{managed_counts[:survived]}/#{RUNS})"
exit(0)
