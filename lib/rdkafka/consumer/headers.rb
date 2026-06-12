# frozen_string_literal: true

module Rdkafka
  class Consumer
    # Interface to return headers for a consumer message
    module Headers
      # Empty frozen hash used when there are no headers
      EMPTY_HEADERS = {}.freeze

      # Key under which reusable scratch pointers are stored on the current thread/fiber
      SCRATCH_KEY = :rdkafka_headers_scratch

      private_constant :SCRATCH_KEY

      # Reads a librdkafka native message's headers and returns them as a Ruby Hash
      # where each key maps to either a String (single value) or Array<String> (multiple values)
      # to support duplicate headers per KIP-82
      #
      # @private
      #
      # @param native_message [Rdkafka::Bindings::Message] the native message to read headers from
      # @return [Hash{String => String, Array<String>}] headers Hash for the native_message
      # @raise [Rdkafka::RdkafkaError] when fail to read headers
      def self.from_native(native_message)
        # Scratch output pointers for the header FFI calls, reused across messages. They are
        # stored in fiber-local storage (`Thread.current[]` is fiber-local by design), so each
        # thread and each fiber gets its own set and reuse is safe with fiber schedulers. They
        # never escape this method and all data is read out of them before returning.
        # Allocating them per message would cost several native allocations for every consumed
        # message.
        headers_ptrptr, name_ptrptr, value_ptrptr, size_ptr =
          Thread.current[SCRATCH_KEY] ||= [
            FFI::MemoryPointer.new(:pointer),
            FFI::MemoryPointer.new(:pointer),
            FFI::MemoryPointer.new(:pointer),
            Rdkafka::Bindings::SizePtr.new
          ].freeze

        err = Rdkafka::Bindings.rd_kafka_message_headers(native_message, headers_ptrptr)

        if err == Rdkafka::Bindings::RD_KAFKA_RESP_ERR__NOENT
          return EMPTY_HEADERS
        elsif err != Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR
          raise Rdkafka::RdkafkaError.new(err, "Error reading message headers")
        end

        headers_ptr = headers_ptrptr.read_pointer

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
