# frozen_string_literal: true

module Rdkafka
  # @private
  # A wrapper around a native kafka that handles all access
  class NativeKafka
    attr_reader :inner, :poll_mutex, :method_mutex

    def initialize(inner, run_polling_thread:)
      @inner = inner
      @poll_mutex = Mutex.new
      @method_mutex = Mutex.new

      if run_polling_thread
        # Start thread to poll client for delivery callbacks,
        # not used in consumer.
        @polling_thread = Thread.new do
          loop do
            poll_mutex.synchronize do
              Rdkafka::Bindings.rd_kafka_poll(inner, 250)
              # Exit thread if closing and the poll queue is empty
              if Thread.current[:closing] && Rdkafka::Bindings.rd_kafka_outq_len(inner) == 0
                Thread.exit
              end
            end
          end
        end
        @polling_thread.abort_on_exception = true
        @polling_thread[:closing] = false
      end

      @closing = false
    end

    def with_inner_for_poll
      return if @inner.nil?
      @poll_mutex.synchronize do
        yield @inner
      end
    end

    def with_inner
      return if @inner.nil?
      @method_mutex.synchronize do
        yield @inner
      end
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

      if @polling_thread
        # Indicate to polling thread that we're closing
        @polling_thread[:closing] = true
        # Wait for the polling thread to finish up
        @polling_thread.join
      end

      # Run in a thread because this could be called
      # in a trap context
      Thread.new do
        # Lock the mutexes for polling and method access
        # to make sure we are not destroying during an
        # ongoing operation
        @method_mutex.lock
        @poll_mutex.lock

        # Destroy the client
        Rdkafka::Bindings.rd_kafka_destroy(@inner)
      end.join

      @inner = nil
    end
  end
end
