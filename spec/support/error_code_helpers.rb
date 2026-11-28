# frozen_string_literal: true

# Resolves librdkafka error names to their codes, so tests name the error they use.
module ErrorCodeHelpers
  extend self

  # librdkafka error codes by the name `Rdkafka::RdkafkaError#code` reports for them
  CODES = Rdkafka::Admin.describe_errors.each_value.with_object({}) do |error, codes|
    next if error[:name].empty?

    codes[error[:name].downcase.delete_prefix("_").to_sym] = error[:code]
  end.freeze

  # @param name [Symbol] error name, for example `:transport` or `:topic_authorization_failed`
  # @return [Integer] the librdkafka error code
  # @raise [KeyError] if librdkafka has no error with this name
  def error_code(name)
    CODES.fetch(name)
  end
end
