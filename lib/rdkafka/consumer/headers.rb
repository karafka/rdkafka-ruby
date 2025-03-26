# frozen_string_literal: true

module Rdkafka
  class Consumer
    # Interface to return headers for a consumer message
    module Headers
      EMPTY_HEADERS = {}.freeze

      # Reads a librdkafka native message's headers and returns them as a Ruby Hash
      # where each key maps to either a String (single value) or Array<String> (multiple values)
      # to support duplicate headers per KIP-82
      #
      # @private
      #
      # @param [Rdkafka::Bindings::Message] native_message
      # @return [Hash<String, String|Array<String>>] headers Hash for the native_message
      # @raise [Rdkafka::RdkafkaError] when fail to read headers
      def self.from_native(native_message)
        headers_ptrptr = FFI::MemoryPointer.new(:pointer)
        err = Rdkafka::Bindings.rd_kafka_message_headers(native_message, headers_ptrptr)

        if err == Rdkafka::Bindings::RD_KAFKA_RESP_ERR__NOENT
          return EMPTY_HEADERS
        elsif err != Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR
          raise Rdkafka::RdkafkaError.new(err, "Error reading message headers")
        end

        headers_ptr = headers_ptrptr.read_pointer

        name_ptrptr = FFI::MemoryPointer.new(:pointer)
        value_ptrptr = FFI::MemoryPointer.new(:pointer)
        size_ptr = Rdkafka::Bindings::SizePtr.new

        headers = {}

        idx = 0
        loop do
          err = Rdkafka::Bindings.rd_kafka_header_get_all(
            headers_ptr,
            idx,
            name_ptrptr,
            value_ptrptr,
            size_ptr
          )

          if err == Rdkafka::Bindings::RD_KAFKA_RESP_ERR__NOENT
            break
          elsif err != Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR
            raise Rdkafka::RdkafkaError.new(err, "Error reading a message header at index #{idx}")
          end

          name_ptr = name_ptrptr.read_pointer
          name = name_ptr.respond_to?(:read_string_to_null) ? name_ptr.read_string_to_null : name_ptr.read_string

          size = size_ptr[:value]

          value_ptr = value_ptrptr.read_pointer
          value = value_ptr.read_string(size)

          if headers.key?(name)
            # If we've seen this header before, convert to array if needed and append
            if headers[name].is_a?(Array)
              headers[name] << value
            else
              headers[name] = [headers[name], value]
            end
          else
            # First occurrence - store as single value
            headers[name] = value
          end

          idx += 1
        end

        headers.freeze
      end
    end
  end
end
