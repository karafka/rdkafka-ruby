# frozen_string_literal: true

module Rdkafka
  # @private
  # A wrapper around a native kafka that polls and cleanly exits
  class NativeKafka
    def initialize(inner)
      @inner = inner

      # Start thread to poll client for delivery callbacks
      @polling_thread = Thread.new do
        loop do
          Rdkafka::Bindings.rd_kafka_poll(inner, 250)
          # Exit thread if closing and the poll queue is empty
          if Thread.current[:closing] && Rdkafka::Bindings.rd_kafka_outq_len(inner) == 0
            break
          end
        end
      end
      @polling_thread.abort_on_exception = true
      @polling_thread[:closing] = false

      @closing = false
    end

    def inner
      @inner
    end

    def finalizer
      ->(_) { close }
    end

    def closed?
      @closing || @inner.nil?
    end

    def close(object_id=nil)
      return if closed?

      # Indicate to the outside world that we are closing
      @closing = true

      # Indicate to polling thread that we're closing
      @polling_thread[:closing] = true
      # Wait for the polling thread to finish up
      @polling_thread.join

      # Destroy the client
      #Rdkafka::Bindings.rd_kafka_destroy(@inner)
      @inner = nil
    end
  end
end
