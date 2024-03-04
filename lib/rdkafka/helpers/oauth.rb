module Rdkafka
  module Helpers

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
        @native_kafka.with_inner do |inner|
          response = Rdkafka::Bindings.rd_kafka_oauthbearer_set_token(
            inner, token, lifetime_ms, principal_name,
            flatten_extensions(extensions), extension_size(extensions), error_buffer, 256
          )
          if response != 0
            Rdkafka::Bindings.rd_kafka_oauthbearer_set_token_failure(
              inner,
              "Failed to set token: #{error_buffer.read_string}"
            )
          end

          response
        end
      end

      private

      # Flatten the extensions hash into a string according to the spec, https://datatracker.ietf.org/doc/html/rfc7628#section-3.1
      def flatten_extensions(extensions)
        return nil unless extensions
        "\x01#{extensions.map { |e| e.join("=") }.join("\x01")}"
      end

      def extension_size(extensions)
        return 0 unless extensions
        extensions.size * 2
      end
    end
  end
end
