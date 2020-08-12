module Rdkafka
  # @private
  module Bindings
    # Abstract librdkafka native socket connection(s) handle.
    class Handle
      extend FFI::DataConverter
      native_type FFI::Type::POINTER

      attr_accessor :type
      attr_reader :handle_pointer

      def initialize(ptr)
        @handle_pointer = ptr
      end

      def close
        return unless @handle_pointer

        Rdkafka::Bindings.close_consumer(self) if type == :consumer
        Rdkafka::Bindings.close_handle(self)
        @handle_pointer = nil
      end

      class << self
        def from_native(ptr, _)
          new(ptr)
        end

        def to_native(value, _)
          if value.is_a?(self)
            raise ProducerClosedError if value.handle_pointer.nil? && type == :producer
            raise ConsumerClosedError if value.handle_pointer.nil? && type == :consumer

            value.handle_pointer
          else
            raise TypeError.new('Must be a Rdkafka::Bindings::Handle instance')
          end
        end
      end
    end

    # Create a native kafka handle which is the socket connection used by librdkafka
    def self.new_native_handle(native_config, type)
      error_buffer = FFI::MemoryPointer.from_string(" " * 256)

      handle =
        case type
        when :producer
          create_handle(:rd_kafka_producer, native_config, error_buffer, 256)&.tap do |h|
            h.type = :producer
          end
        when :consumer
          create_handle(:rd_kafka_consumer, native_config, error_buffer, 256)&.tap do |h|
            h.type = :consumer
          end
        else
          raise TypeError.new('Type has to be a :consumer or :producer')
        end

      if handle.handle_pointer.nil? || handle.handle_pointer.null?
        raise Rdkafka::ClientCreationError.new(error_buffer.read_string)
      end

      # Redirect log to handle's queue
      Rdkafka::Bindings.rd_kafka_set_log_queue(
        handle,
        Rdkafka::Bindings.rd_kafka_queue_get_main(handle)
      )

      # Return handle which should be closed using #close after usage.
      handle
    end
  end
end
