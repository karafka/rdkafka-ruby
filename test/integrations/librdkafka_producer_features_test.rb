# frozen_string_literal: true

# This integration test verifies that librdkafka producer is compiled with all expected builtin features.
# These features are critical for Karafka and rdkafka-ruby to function properly.

require_relative "../test_helper"
require "stringio"

# Expected features that should be present in our compiled librdkafka
PRODUCER_EXPECTED_BUILTIN_FEATURES = %w[
  gzip
  snappy
  ssl
  sasl
  regex
  lz4
  sasl_plain
  sasl_scram
  plugins
  zstd
  sasl_oauthbearer
].freeze

# Precompiled builds include GSSAPI (via MIT Kerberos + Cyrus SASL)
PRODUCER_PRECOMPILED_FEATURES = (PRODUCER_EXPECTED_BUILTIN_FEATURES + %w[sasl_gssapi]).freeze

describe "Librdkafka Producer Features" do
  before do
    @captured_output = StringIO.new
    @logger = Logger.new(@captured_output)
    @logger.level = Logger::DEBUG

    @original_logger = Rdkafka::Config.logger
    Rdkafka::Config.logger = @logger
    Rdkafka::Config.ensure_log_thread

    config = Rdkafka::Config.new(
      "bootstrap.servers": "localhost:9092",
      "client.id": "producer-feature-test",
      debug: "all"
    )

    @producer = config.producer

    # Wait for log messages to be processed
    sleep 2
  end

  after do
    @producer&.close
    Rdkafka::Config.logger = @original_logger
  end

  it "includes all expected builtin features in producer client logs" do
    log_content = @captured_output.string

    # Find the initialization line that contains builtin.features
    feature_line = log_content.lines.find { |line| line.include?("builtin.features") }

    refute_nil feature_line, "Could not find builtin.features in producer log output"

    # Extract the features list from the line
    # Format: "... (builtin.features gzip,snappy,ssl,..., ...)"
    match = feature_line.match(/builtin\.features\s+([^,]+(?:,[^,\s]+)*)/i)

    refute_nil match, "Could not parse builtin.features from log line: #{feature_line}"

    features_string = match[1]
    actual_features = features_string.split(",").map(&:strip)

    # Verify all expected features are present
    expected = if ENV["RDKAFKA_PRECOMPILED"] == "true"
                 PRODUCER_PRECOMPILED_FEATURES
               else
                 PRODUCER_EXPECTED_BUILTIN_FEATURES
               end

    missing_features = expected - actual_features

    assert_empty missing_features,
                 "Missing expected builtin features: #{missing_features.join(', ')}"
  end
end
