module Rdkafka
  class Producer
    class Client
      def initialize(native_kafka)
        @native_kafka = native_kafka

        # Start thread to poll client for delivery callbacks
        @polling_thread = Thread.new do
          loop do
            closed_consumer_check(__method__)

            Rdkafka::Bindings.rd_kafka_poll(@native_kafka, 250)
            # Exit thread if closing and the poll queue is empty
            if Thread.current[:closing] && Rdkafka::Bindings.rd_kafka_outq_len(@native_kafka) == 0
              break
            end
          end
        end
        @polling_thread.abort_on_exception = true
        @polling_thread[:closing] = false
      end

      def finalizer
        ->(_) { close }
      end

      def closed?
        @native_kafka.nil?
      end

      def close(object_id=nil)
        return unless @native_kafka

        # Indicate to polling thread that we're closing
        @polling_thread[:closing] = true
        # Wait for the polling thread to finish up
        @polling_thread.join

        Rdkafka::Bindings.rd_kafka_destroy(@native_kafka)
        @native_kafka = nil
      end

      def closed_consumer_check(method)
        raise Rdkafka::ClosedProducerError.new(method) if @native_kafka.nil?
      end
    end
  end
end
