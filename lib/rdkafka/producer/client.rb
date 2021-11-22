module Rdkafka
  class Producer
    class Client
      def initialize(native)
        @native = native

        # Start thread to poll client for delivery callbacks
        @polling_thread = Thread.new do
          loop do
            Rdkafka::Bindings.rd_kafka_poll(native, 250)
            # Exit thread if closing and the poll queue is empty
            if Thread.current[:closing] && Rdkafka::Bindings.rd_kafka_outq_len(native) == 0
              break
            end
          end
        end
        @polling_thread.abort_on_exception = true
        @polling_thread[:closing] = false
      end

      def native
        @native
      end

      def finalizer
        ->(_) { close }
      end

      def closed?
        @native.nil?
      end

      def close(object_id=nil)
        return unless @native

        # Indicate to polling thread that we're closing
        @polling_thread[:closing] = true
        # Wait for the polling thread to finish up
        @polling_thread.join

        Rdkafka::Bindings.rd_kafka_destroy(@native)

        @native = nil
      end
    end
  end
end
