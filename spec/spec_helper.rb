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
  # Allow OpenStruct usage only in specs
  next if warning.include?("OpenStruct use") && warning.include?("_spec")

  raise "Warning in your code: #{warning}"
end

unless ENV["CI"] == "true"
  require "simplecov"
  SimpleCov.start do
    skip "/spec/"
  end
end

require "pry"
require "rspec"
require "rdkafka"
require "timeout"
require "securerandom"
require "digest"

# Load support modules
require_relative "support/kafka_config_helpers"
require_relative "support/kafka_wait_helpers"
require_relative "support/native_client_helpers"
require_relative "support/admin_topic_auto_wait"
require_relative "support/test_topics"

RSpec.configure do |config|
  config.disable_monkey_patching!

  config.include KafkaConfigHelpers
  config.include KafkaWaitHelpers
  config.include NativeClientHelpers

  config.filter_run focus: true
  config.run_all_when_everything_filtered = true

  config.before do
    Rdkafka::Config.statistics_callback = nil
    Rdkafka::Config.error_callback = nil
    Rdkafka::Config.oauthbearer_token_refresh_callback = nil
    # We need to clear it so state does not leak between specs
    Rdkafka::Producer.partitions_count_cache.to_h.clear
  end

  config.before(:suite) do
    admin = KafkaConfigHelpers.rdkafka_config.admin
    {
      TestTopics.example_topic => 1,
      "test" => 1
    }.each do |topic, partitions|
      create_topic_handle = admin.create_topic(topic, partitions, 1)
      begin
        create_topic_handle.wait(max_wait_timeout_ms: 1_000)
      rescue Rdkafka::RdkafkaError => ex
        raise unless ex.message.match?(/topic_already_exists/)
      end
    end
    admin.close
  end

  config.around do |example|
    # Timeout specs after 1.5 minute. If they take longer they are probably stuck
    Timeout.timeout(90) do
      example.run
    end
  end

  # The handle registry (`AbstractHandle::REGISTRY`) is one process-global hash shared by every
  # handle class. After each example it must be empty; a handle left behind - e.g. a
  # `CreateTopicHandle` orphaned when topic setup times out against a slow broker - is otherwise
  # visible to every later example, turning a single leak into a cascade of identical failures.
  #
  # We capture, CLEAR unconditionally, then assert - so the clear always runs (a raising assertion
  # must never skip it) and a leak fails only the example that caused it. `append_after` runs this
  # after each group's own `after` hooks, so the clients that own the handles have already been
  # closed and only genuinely-orphaned handles remain.
  config.append_after do
    registry = Rdkafka::AbstractHandle::REGISTRY

    # Async delivery/background callbacks may not have fired yet; give them a brief moment.
    10.times do
      break if registry.empty?

      sleep(0.05)
    end

    leaked_handles = registry.values
    registry.clear

    leaked_names = leaked_handles.map { |handle| handle.class.name }.uniq.sort

    expect(leaked_handles).to be_empty, "Leaked handles in: #{leaked_names.join(", ")}"
  end
end
