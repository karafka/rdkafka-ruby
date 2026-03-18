# frozen_string_literal: true

Warning[:performance] = true if RUBY_VERSION >= "3.3"
Warning[:deprecated] = true
$VERBOSE = true

require "warning"

Warning.process do |warning|
  next unless warning.include?(Dir.pwd)
  # Allow OpenStruct usage only in tests
  next if warning.include?("OpenStruct use") && warning.include?("_test")

  raise "Warning in your code: #{warning}"
end

unless ENV["CI"] == "true"
  require "simplecov"
  SimpleCov.start do
    add_filter "/test/"
  end
end

require "minitest/autorun"
require "mocha/minitest"
require "pry"
require "rdkafka"
require "timeout"
require "securerandom"

# Load support modules
require_relative "support/kafka_config_helpers"
require_relative "support/kafka_wait_helpers"
require_relative "support/native_client_helpers"
require_relative "support/test_topics"

# Add `context` as an alias for `describe` in minitest/spec
Minitest::Spec.class_eval do
  class << self
    alias_method :context, :describe
  end
end

# Provide `described_class` helper that walks up the Minitest::Spec hierarchy
module DescribedClassHelper
  def described_class
    klass = self.class
    while klass && klass < Minitest::Spec
      return klass.desc if klass.desc.is_a?(Class) || klass.desc.is_a?(Module)
      klass = klass.superclass
    end
    nil
  end
end

# Provide `with_stubbed_const` for stub_const replacement
module StubbedConstHelper
  def with_stubbed_const(mod, const_name, temp_value)
    old_value = mod.const_get(const_name)
    mod.send(:remove_const, const_name)
    mod.const_set(const_name, temp_value)
    yield
  ensure
    mod.send(:remove_const, const_name)
    mod.const_set(const_name, old_value)
  end
end

# 90-second timeout wrapper per test
module TimeoutWrapper
  def run
    Timeout.timeout(90) do
      super
    end
  end
end

Minitest::Test.prepend(TimeoutWrapper)

# Include support modules into all specs
Minitest::Spec.class_eval do
  include KafkaConfigHelpers
  include KafkaWaitHelpers
  include NativeClientHelpers
  include DescribedClassHelper
  include StubbedConstHelper
end

# One-time suite setup: create example topic before any tests run
SUITE_SETUP_MUTEX = Mutex.new
SUITE_SETUP_DONE = { value: false }

module SuiteSetup
  def before_setup
    super
    SUITE_SETUP_MUTEX.synchronize do
      unless SUITE_SETUP_DONE[:value]
        admin = KafkaConfigHelpers.rdkafka_config.admin
        begin
          create_topic_handle = admin.create_topic(TestTopics.example_topic, 1, 1)
          create_topic_handle.wait(max_wait_timeout_ms: 1_000)
        rescue Rdkafka::RdkafkaError => ex
          raise unless ex.message.match?(/topic_already_exists/)
        ensure
          admin.close
        end
        SUITE_SETUP_DONE[:value] = true
      end
    end
  end
end

Minitest::Spec.prepend(SuiteSetup)

# Global before each: clear callbacks and partition cache
module GlobalBeforeEach
  def setup
    super
    Rdkafka::Config.statistics_callback = nil
    Rdkafka::Config.error_callback = nil
    Rdkafka::Config.oauthbearer_token_refresh_callback = nil
    Rdkafka::Producer.partitions_count_cache.to_h.clear
  end
end

Minitest::Spec.prepend(GlobalBeforeEach)
