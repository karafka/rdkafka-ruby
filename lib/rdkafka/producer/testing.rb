# frozen_string_literal: true

module Rdkafka
  # Testing utilities for Producer instances.
  # This module is NOT included by default and should only be used in test environments.
  #
  # This module provides librdkafka native testing utilities that are needed to trigger certain
  # behaviours that are hard to reproduce in stable environments, particularly fatal error
  # scenarios in idempotent and transactional producers.
  #
  # To use in tests for producers:
  #   producer.singleton_class.include(Rdkafka::Testing)
  #
  # Or include it for all producers in your test suite:
  #   Rdkafka::Producer.include(Rdkafka::Testing)
  #
  # @note Fatal errors leave the producer client in an unusable state. After triggering
  #   a fatal error, the producer should be closed and discarded. Do not attempt to reuse a
  #   producer that has experienced a fatal error.
  module Testing
    # Triggers a test fatal error using rd_kafka_test_fatal_error.
    # This is useful for testing fatal error handling without needing actual broker issues.
    #
    # @param error_code [Integer] The error code to trigger (e.g., 47 for invalid_producer_epoch)
    # @param reason [String] Descriptive reason for the error
    # @return [Integer] Result code from rd_kafka_test_fatal_error (0 on success)
    #
    # @example
    #   producer.trigger_test_fatal_error(47, "Test producer fencing")
    def trigger_test_fatal_error(error_code, reason)
      @native_kafka.with_inner do |inner|
        Rdkafka::Bindings.rd_kafka_test_fatal_error(inner, error_code, reason)
      end
    end

    # Checks if a fatal error has occurred and retrieves error details.
    # Calls rd_kafka_fatal_error to get the actual fatal error code and message.
    #
    # @return [Hash, nil] Hash with :error_code and :error_string if fatal error occurred, nil otherwise
    #
    # @example
    #   if fatal_error = producer.fatal_error
    #     puts "Fatal error #{fatal_error[:error_code]}: #{fatal_error[:error_string]}"
    #   end
    def fatal_error
      @native_kafka.with_inner do |inner|
        Rdkafka::Bindings.extract_fatal_error(inner)
      end
    end
  end
end
