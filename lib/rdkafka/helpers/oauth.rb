module Rdkafka
  module Helpers
    # OAuth helper methods for setting and refreshing SASL/OAUTHBEARER tokens
    module OAuth

      # Set the OAuthBearer token
      #
      # @param token [String] the mandatory token value to set, often (but not necessarily) a JWS compact serialization as per https://tools.ietf.org/html/rfc7515#section-3.1.
      # @param lifetime_ms [Integer] when the token expires, in terms of the number of milliseconds since the epoch. See https://currentmillis.com/.
      # @param principal_name [String] the mandatory Kafka principal name associated with the token.
      # @param extensions [Hash] optional SASL extensions key-value pairs to be communicated to the broker as additional key-value pairs during the initial client response as per https://tools.ietf.org/html/rfc7628#section-3.1.
      # @return [Integer] 0 on success
      def oauthbearer_set_token(token:, lifetime_ms:, principal_name:, extensions: nil)
        error_buffer = FFI::MemoryPointer.from_string(" " * 256)
        extensions_ptr, extensions_str_ptrs = map_extensions(extensions)

        begin
          response = @native_kafka.with_inner do |inner|
            Rdkafka::Bindings.rd_kafka_oauthbearer_set_token(
              inner, token, lifetime_ms, principal_name,
              extensions_ptr, extension_size(extensions), error_buffer, 256
            )
          end
        ensure
          extensions_str_ptrs&.each { |ptr| ptr.free }
          extensions_ptr&.free
        end

        return response if response == Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR

        oauthbearer_set_token_failure("Failed to set token: #{error_buffer.read_string}")

        response
      end

      # Marks failed oauth token acquire in librdkafka
      #
      # @param reason [String] human readable error reason for failing to acquire token
      def oauthbearer_set_token_failure(reason)
        @native_kafka.with_inner do |inner|
          Rdkafka::Bindings.rd_kafka_oauthbearer_set_token_failure(
            inner,
            reason
          )
        end
      end

      private

      # Convert extensions hash to FFI::MemoryPointer (`const char **`).
      #
      # @param extensions [Hash, nil] extension key-value pairs
      # @return [Array<FFI::MemoryPointer, Array<FFI::MemoryPointer>>] array pointer and string pointers
      # @note The returned pointers must be freed manually (autorelease = false).
      def map_extensions(extensions)
        return [nil, nil] if extensions.nil? || extensions.empty?

        # https://github.com/confluentinc/librdkafka/blob/master/src/rdkafka_sasl_oauthbearer.c#L327-L347

        # The method argument is const char **
        array_ptr = FFI::MemoryPointer.new(:pointer, extension_size(extensions))
        array_ptr.autorelease = false
        str_ptrs = []

        # Element i is the key, i + 1 is the value.
        extensions.each_with_index do |(k, v), i|
          k_ptr = FFI::MemoryPointer.from_string(k.to_s)
          k_ptr.autorelease = false
          str_ptrs << k_ptr
          v_ptr = FFI::MemoryPointer.from_string(v.to_s)
          v_ptr.autorelease = false
          str_ptrs << v_ptr
          array_ptr[i * 2].put_pointer(0, k_ptr)
          array_ptr[i * 2 + 1].put_pointer(0, v_ptr)
        end

        [array_ptr, str_ptrs]
      end

      # Returns the extension size (number of keys + values).
      #
      # @param extensions [Hash, nil] extension key-value pairs
      # @return [Integer] non-negative even number representing keys + values count
      # @see https://github.com/confluentinc/librdkafka/blob/master/src/rdkafka_sasl_oauthbearer.c#L327-L347
      def extension_size(extensions)
        return 0 unless extensions
        extensions.size * 2
      end
    end
  end
end
