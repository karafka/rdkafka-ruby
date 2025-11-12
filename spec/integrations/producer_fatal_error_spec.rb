# frozen_string_literal: true

# This integration test verifies that fatal error handling works correctly in karafka-rdkafka.
# Fatal errors occur when librdkafka detects conditions that make the client instance unusable,
# primarily with idempotent and transactional producers when delivery guarantees cannot be maintained.
#
# Key concepts tested:
# - When librdkafka triggers error callback with ERR__FATAL (-150), we call rd_kafka_fatal_error()
# - rd_kafka_fatal_error() returns the actual underlying error code (e.g., 47 for invalid_producer_epoch)
# - The error is properly remapped with the fatal flag set to true
# - Common fatal errors: 47 (invalid_producer_epoch), 59 (unknown_producer_id),
#   45 (out_of_order_sequence_number), 90 (producer_fenced)
#
# Exit codes:
# - 0: All tests pass
# - 1: Test failures

require 'rdkafka'
require 'rdkafka/producer/testing'
require 'securerandom'

# Include testing utilities for Producer in test environment
Rdkafka::Producer.include(Rdkafka::Testing)

$stdout.sync = true

def test_fatal_error_remapping(producer, error_code, error_symbol, description)
  error_received = nil
  error_callback = lambda do |error|
    # Only capture the first error to avoid overwriting with subsequent broker errors
    error_received = error if error.fatal?
  end

  Rdkafka::Config.error_callback = error_callback

  # Trigger a test fatal error
  result = producer.trigger_test_fatal_error(error_code, description)

  # Should return RD_KAFKA_RESP_ERR_NO_ERROR (0) if successful
  unless result == Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR
    puts "ERROR: rd_kafka_test_fatal_error returned #{result}, expected 0"
    return false
  end

  # Immediately check the fatal error details before any other errors can occur
  # This is important in environments without Kafka where broker connection errors
  # could trigger another fatal error and overwrite our test error
  fatal_details = producer.fatal_error

  unless fatal_details
    puts "ERROR: No fatal error details found immediately after trigger"
    return false
  end

  unless fatal_details[:error_code] == error_code
    puts "ERROR: Expected error code #{error_code}, got #{fatal_details[:error_code]}"
    return false
  end

  # Give some time for the error callback to be triggered
  sleep 0.2

  # Verify the error callback was called
  unless error_received
    puts "ERROR: Error callback was not called"
    return false
  end

  # The error should have the actual fatal error code, not -150
  unless error_received.rdkafka_response == error_code
    puts "ERROR: Expected error code #{error_code} from callback, got #{error_received.rdkafka_response}"
    return false
  end

  unless error_received.code == error_symbol
    puts "ERROR: Expected error symbol #{error_symbol}, got #{error_received.code}"
    return false
  end

  # The fatal flag should be set
  unless error_received.fatal?
    puts "ERROR: Fatal flag not set for error #{error_code}"
    return false
  end

  # The error message should contain our test reason
  unless error_received.broker_message.include?("test_fatal_error")
    puts "ERROR: Error message doesn't contain 'test_fatal_error': #{error_received.broker_message}"
    return false
  end

  unless error_received.broker_message.include?(description)
    puts "ERROR: Error message doesn't contain description '#{description}': #{error_received.broker_message}"
    return false
  end

  true
end

def test_rd_kafka_fatal_error_function
  # Test 1: Should return nil when no fatal error has occurred
  config = Rdkafka::Config.new(
    'bootstrap.servers' => 'localhost:9092',
    'enable.idempotence' => true
  )
  producer = config.producer

  result = producer.fatal_error

  unless result.nil?
    puts "ERROR: fatal_error returned #{result} when no error occurred, expected nil"
    producer.close
    return false
  end

  # Test 2: Should return error details after a fatal error is triggered
  producer.trigger_test_fatal_error(47, "Test fatal error")

  sleep 0.2

  # Now check for fatal error
  result = producer.fatal_error

  unless result
    puts "ERROR: fatal_error returned nil after triggering error"
    producer.close
    return false
  end

  unless result[:error_code] == 47
    puts "ERROR: fatal_error returned error code #{result[:error_code]}, expected 47"
    producer.close
    return false
  end

  unless result[:error_string].include?("test_fatal_error")
    puts "ERROR: Error string doesn't contain 'test_fatal_error': #{result[:error_string]}"
    producer.close
    return false
  end

  unless result[:error_string].include?("Test fatal error")
    puts "ERROR: Error string doesn't contain 'Test fatal error': #{result[:error_string]}"
    producer.close
    return false
  end

  # Mark for cleanup instead of closing (fatal error makes close hang/crash)
  producer.close
  true
end

# Main test execution
begin
  # Create producer with idempotent mode enabled (required for fatal errors in production)
  config = Rdkafka::Config.new(
    'bootstrap.servers' => 'localhost:9092',
    'enable.idempotence' => true,
    'client.id' => "fatal-error-test-#{SecureRandom.uuid}"
  )

  producer = config.producer

  # Test fatal error remapping for common idempotent producer errors
  fatal_errors = [
    [47, :invalid_producer_epoch, "Producer epoch is invalid (producer fenced)"],
    [59, :unknown_producer_id, "Producer ID is no longer valid"],
    [45, :out_of_order_sequence_number, "Sequence number desynchronization"],
    [90, :producer_fenced, "Producer has been fenced by newer instance"]
  ]

  all_passed = true

  fatal_errors.each do |error_code, error_symbol, description|
    unless test_fatal_error_remapping(producer, error_code, error_symbol, description)
      all_passed = false
    end

    # Mark for cleanup instead of closing (fatal error makes close hang/crash)
    producer.close

    # Create a new producer for next test (can't reuse after fatal error)
    producer = config.producer
  end

  # Mark the last producer for cleanup
  producer.close

  # Test rd_kafka_fatal_error function
  unless test_rd_kafka_fatal_error_function
    all_passed = false
  end

  if all_passed
    exit(0)
  else
    puts "Some fatal error handling tests failed"
    exit(1)
  end

rescue => e
  puts "ERROR: Unexpected exception: #{e.class}: #{e.message}"
  puts e.backtrace.join("\n")
  exit(1)
end
