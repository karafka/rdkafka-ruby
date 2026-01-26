# frozen_string_literal: true

# This integration test verifies that librdkafka admin is compiled with all expected builtin features.
# These features are critical for Karafka and rdkafka-ruby to function properly.
#
# Exit codes:
# - 0: All expected features found (test passes)
# - 1: Missing expected features or parsing failed (test fails)

require "rdkafka"
require "logger"
require "stringio"

$stdout.sync = true

# Expected features that should be present in our compiled librdkafka
EXPECTED_BUILTIN_FEATURES = %w[
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
PRECOMPILED_FEATURES = (EXPECTED_BUILTIN_FEATURES + %w[sasl_gssapi]).freeze

captured_output = StringIO.new
logger = Logger.new(captured_output)
logger.level = Logger::DEBUG

Rdkafka::Config.logger = logger
Rdkafka::Config.ensure_log_thread

config = Rdkafka::Config.new(
  "bootstrap.servers": "localhost:9092",
  "client.id": "admin-feature-test",
  debug: "all"
)

admin = config.admin

# Wait for log messages to be processed
sleep 2

admin.close

# Get all log output
log_content = captured_output.string

# Find the initialization line that contains builtin.features
feature_line = log_content.lines.find { |line| line.include?("builtin.features") }

unless feature_line

  exit(1)
end

# Extract the features list from the line
# Format: "... (builtin.features gzip,snappy,ssl,..., ...)"
match = feature_line.match(/builtin\.features\s+([^,]+(?:,[^,\s]+)*)/i)

unless match

  exit(1)
end

features_string = match[1]
actual_features = features_string.split(",").map(&:strip)

# Verify all expected features are present
expected = (ENV["RDKAFKA_PRECOMPILED"] == "true") ? PRECOMPILED_FEATURES : EXPECTED_BUILTIN_FEATURES
missing_features = expected - actual_features

if missing_features.any?

  exit(1)
end
