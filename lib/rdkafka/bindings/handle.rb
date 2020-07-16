module Rdkafka
  # @private
  module Bindings
    # Abstract librdkafka native socket connection(s) handle.
    class Handle
      extend FFI::DataConverter
      native_type FFI::Type::POINTER

      attr_accessor :type
      attr_reader :handle_pointer

      # TODO: Raise error when NULL
      def initialize(ptr)
        @handle_pointer = ptr
      end

      def close
        Rdkafka::Bindings.consumer_close(handle_pointer) if type == :consumer
        Rdkafka::Bindings.close_handle(handle_pointer)
      end

      class << self
        def from_native(ptr, _)
          new(ptr)
        end

        def to_native(value, _)
          raise ProducerClosedError if value.nil? && type == :producer
          raise ConsumerClosedError if value.nil? && type == :consumer

          value.is_a?(Handle) ? value.handle_pointer : value
        end
      end
    end

    def self.new_native_handle(config, type)
      error_buffer = FFI::MemoryPointer.from_string(" " * 256)

      handle =
        case type
        when :producer
          create_handle(:rd_kafka_producer, config, error_buffer, 256)&.tap do |h|
            h.type = :producer
          end
        when :consumer
          create_handle(:rd_kafka_consumer, config, error_buffer, 256)&.tap do |h|
            h.type = :consumer
          end
        end

      if handle.nil? || handle.handle_pointer.null?
        raise Rdkafka::ClientCreationError.new(error_buffer.read_string)
      end

      # Redirect log to handle's queue
      Rdkafka::Bindings.rd_kafka_set_log_queue(
        handle,
        Rdkafka::Bindings.rd_kafka_queue_get_main(handle)
      )

      # Return handle which should be closed using rd_kafka_destroy after usage.
      handle
    end
  end
end
