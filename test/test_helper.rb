# frozen_string_literal: true

require "warning"

if Warning.respond_to?(:categories)
  (Warning.categories - %i[experimental]).each do |cat|
    Warning[cat] = true
  end
end
$VERBOSE = true

Warning.process do |warning|
  next unless warning.include?(Dir.pwd)
  # Allow OpenStruct usage only in tests
  next if warning.include?("OpenStruct use") && warning.include?("_test")

  raise "Warning in your code: #{warning}"
end

unless ENV["CI"] == "true"
  require "simplecov"
  SimpleCov.start do
    command_name "Minitest"
    skip "/spec/"
    skip "/test/"
  end
end

require "minitest/autorun"
require "minitest/spec"
require "mocha/minitest"
require "rdkafka"
require "timeout"
require "securerandom"
require "digest"

require_relative "../spec/support/kafka_config_helpers"
require_relative "../spec/support/kafka_wait_helpers"
require_relative "../spec/support/native_client_helpers"
require_relative "../spec/support/admin_topic_auto_wait"
require_relative "../spec/support/test_topics"

admin = KafkaConfigHelpers.rdkafka_config.admin
{
  TestTopics.example_topic => 1,
  "test" => 1
}.each do |topic, partitions|
  create_topic_handle = admin.create_topic(topic, partitions, 1)
  begin
    create_topic_handle.wait(max_wait_timeout_ms: TestTopics::CREATE_TIMEOUT_MS)
  rescue Rdkafka::RdkafkaError => ex
    raise unless ex.message.match?(/topic_already_exists/)
  end
end
admin.close

class Minitest::Spec
  include KafkaConfigHelpers
  include KafkaWaitHelpers
  include NativeClientHelpers

  class << self
    alias_method :context, :describe
  end

  # Returns the first Class or Module passed to an enclosing describe block
  def described_class
    klass = self.class

    while klass.respond_to?(:desc)
      return klass.desc if klass.desc.is_a?(Module)

      klass = klass.superclass
    end

    nil
  end

  def before_setup
    super

    Rdkafka::Config.statistics_callback = nil
    Rdkafka::Config.error_callback = nil
    Rdkafka::Config.oauthbearer_token_refresh_callback = nil
    # We need to clear it so state does not leak between tests
    Rdkafka::Producer.partition_count_cache.to_h.clear
  end

  # Times out tests after 1.5 minute. If they take longer they are probably stuck
  def run
    Timeout.timeout(90) { super }
  end

  # The handle registry (`AbstractHandle::REGISTRY`) is one process-global hash shared by every
  # handle class. After each test it must be empty; a handle left behind - e.g. a
  # `CreateTopicHandle` orphaned when topic setup times out against a slow broker - is otherwise
  # visible to every later test, turning a single leak into a cascade of identical failures.
  #
  # We capture, CLEAR unconditionally, then assert - so the clear always runs and a leak fails only
  # the test that caused it. `after_teardown` runs after each test's own `after` blocks, so the
  # clients that own the handles have already been closed and only genuinely-orphaned handles
  # remain.
  def after_teardown
    registry = Rdkafka::AbstractHandle::REGISTRY

    # Async delivery/background callbacks may not have fired yet; give them a brief moment.
    10.times do
      break if registry.empty?

      sleep(0.05)
    end

    leaked_handles = registry.values
    registry.clear

    leaked_names = leaked_handles.map { |handle| handle.class.name }.uniq.sort

    assert_empty(leaked_handles, "Leaked handles in: #{leaked_names.join(", ")}")

    super
  end
end
