module Rdkafka
  class Consumer
    # A message headers
    class Headers
      # @private
      def initialize(native_message)
        @headers_ptr = nil

        headers_ptrptr = FFI::MemoryPointer.new(:pointer)

        err = Rdkafka::Bindings.rd_kafka_message_detach_headers(native_message, headers_ptrptr)

        if err == Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR
          ptr = headers_ptrptr.read(:pointer)
          ptr.autorelease = false
          @headers_ptr = FFI::AutoPointer.new(ptr, Rdkafka::Bindings.method(:rd_kafka_headers_destroy))
        end
      end

      # Find last header by name.
      #
      # @param name [String] a header name
      #
      # @return [String, nil] a found header value
      def [](name)
        return unless @headers_ptr

        value_ptrptr = FFI::MemoryPointer.new(:pointer)
        size_ptr = Rdkafka::Bindings::SizePtr.new

        err = Rdkafka::Bindings.rd_kafka_header_get_last(
          @headers_ptr,
          name.to_s,
          value_ptrptr,
          size_ptr
        )

        return unless err == Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR
        ptr = value_ptrptr.read(:pointer)
        ptr.autorelease = false

        "" # ptr.read_string_length(size_ptr[:value])
      end

      # Human readable representation of this headers.
      # @return [String]
      def to_s
        @headers_ptr ? "present" : "empty"
      end
    end
  end
end
