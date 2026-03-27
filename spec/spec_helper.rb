# frozen_string_literal: true

Warning[:performance] = true if RUBY_VERSION >= "3.3"
Warning[:deprecated] = true
$VERBOSE = true

require "warning"

Warning.process do |warning|
  next unless warning.include?(Dir.pwd)
  # Allow OpenStruct usage only in specs
  next if warning.include?("OpenStruct use") && warning.include?("_spec")

  raise "Warning in your code: #{warning}"
end

unless ENV["CI"] == "true"
  require "simplecov"
  SimpleCov.start do
    add_filter "/spec/"
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
      TestTopics.example_topic => 1
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
    # Timeout specs after 1.5 minute. If they take longer
    # they are probably stuck
    Timeout.timeout(90) do
      example.run
    end
  end
end
