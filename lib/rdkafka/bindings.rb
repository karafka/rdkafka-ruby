# frozen_string_literal: true

module Rdkafka
  # @private
  #
  # @note
  #   There are two types of responses related to errors:
  #     - rd_kafka_error_t - a C object that we need to remap into an error or null when no error
  #     - rd_kafka_resp_err_t - response error code (numeric) that we can use directly
  #
  #   It is critical to ensure, that we handle them correctly. The result type should be:
  #     - rd_kafka_error_t - :pointer
  #     - rd_kafka_resp_err_t - :int
  module Bindings
    extend FFI::Library

    # Returns the library extension based on the host OS
    # @return [String] 'dylib' on macOS, 'so' on other systems
    def self.lib_extension
      if /darwin/.match?(RbConfig::CONFIG["host_os"])
        "dylib"
      else
        "so"
      end
    end

    # Wrap ffi_lib to provide better error messages for glibc compatibility issues
    begin
      ffi_lib File.join(__dir__, "../../ext/librdkafka.#{lib_extension}")
    rescue LoadError => e
      error_message = e.message

      # Check if this is a glibc version mismatch error
      if /GLIBC_[\d.]+['"` ]?\s*not found/i.match?(error_message)
        glibc_version = error_message[/GLIBC_([\d.]+)/, 1] || "unknown"

        raise Rdkafka::LibraryLoadError, <<~ERROR_MSG.strip
          Failed to load librdkafka due to glibc compatibility issue.

          The precompiled librdkafka binary requires glibc version #{glibc_version} or higher,
          but your system has an older version installed.

          To resolve this issue, you have two options:

          1. Upgrade your system to a supported platform (recommended)

          2. Force compilation from source by reinstalling without the precompiled binary:
             gem install rdkafka --platform=ruby

             Or if using Bundler, add to your Gemfile:
             gem 'rdkafka', force_ruby_platform: true

          Original error: #{error_message}
        ERROR_MSG
      else
        # Re-raise the original error if it's not a glibc issue
        raise
      end
    end

    RD_KAFKA_RESP_ERR__TIMED_OUT = -185
    RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS = -175
    RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS = -174
    RD_KAFKA_RESP_ERR__STATE = -172
    RD_KAFKA_RESP_ERR__NOENT = -156
    RD_KAFKA_RESP_ERR_NO_ERROR = 0

    RD_KAFKA_OFFSET_END = -1
    RD_KAFKA_OFFSET_BEGINNING = -2
    RD_KAFKA_OFFSET_STORED = -1000
    RD_KAFKA_OFFSET_INVALID = -1001

    RD_KAFKA_PARTITION_UA = -1
    RD_KAFKA_PARTITION_UA_STR = RD_KAFKA_PARTITION_UA.to_s.freeze

    EMPTY_HASH = {}.freeze

    # FFI struct for size_t pointer wrapper
    class SizePtr < FFI::Struct
      layout :value, :size_t
    end

    # This function comes from our patch on top of librdkafka. It allows os to load all the
    # librdkafka components without initializing the client
    # See: https://github.com/confluentinc/librdkafka/issues/4590
    attach_function :rd_kafka_global_init, [], :void
  end
end

require_relative "bindings/structs"
require_relative "bindings/polling"
require_relative "bindings/metadata"
require_relative "bindings/messages"
require_relative "bindings/errors"
require_relative "bindings/configuration"
require_relative "bindings/callbacks"
require_relative "bindings/handle"
require_relative "bindings/consumer"
require_relative "bindings/producer"
require_relative "bindings/admin"
