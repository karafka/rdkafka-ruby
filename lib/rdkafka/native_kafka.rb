# frozen_string_literal: true

module Rdkafka
  # @private
  # A wrapper around a native kafka that polls and cleanly exits
  class NativeKafka
    def initialize(inner, run_polling_thread:, opaque:, auto_start: true, timeout_ms: 100)
      @inner = inner
      @opaque = opaque
      # Lock around external access
      @access_mutex = Mutex.new
      # Lock around internal polling
      @poll_mutex = Mutex.new
      # Lock around decrementing the operations in progress counter
      # We have two mutexes - one for increment (`@access_mutex`) and one for decrement mutex
      # because they serve different purposes:
      #
      #   - `@access_mutex` allows us to lock the execution and make sure that any operation within
      #      the `#synchronize` is the only one running and that there are no other running
      #      operations.
      #   - `@decrement_mutex` ensures, that our decrement operation is thread-safe for any Ruby
      #     implementation.
      #
      # We do not use the same mutex, because it could create a deadlock when an already
      # incremented operation cannot decrement because `@access_lock` is now owned by a different
      # thread in a synchronized mode and the synchronized mode is waiting on the decrement.
      @decrement_mutex = Mutex.new
      # counter for operations in progress using inner
      @operations_in_progress = 0

      @run_polling_thread = run_polling_thread

      @timeout_ms = timeout_ms

      start if auto_start

      @closing = false
    end

    def start
      synchronize do
        return if @started

        @started = true

        # Trigger initial poll to make sure oauthbearer cb and other initial cb are handled
        Rdkafka::Bindings.rd_kafka_poll(@inner, 0)

        if @run_polling_thread
          # Start thread to poll client for delivery callbacks,
          # not used in consumer.
          @polling_thread = Thread.new do
            loop do
              @poll_mutex.synchronize do
                Rdkafka::Bindings.rd_kafka_poll(@inner, @timeout_ms)
              end

              # Exit thread if closing and the poll queue is empty
              if Thread.current[:closing] && Rdkafka::Bindings.rd_kafka_outq_len(@inner) == 0
                break
              end
            end
          end

          @polling_thread.name = "rdkafka.native_kafka##{Rdkafka::Bindings.rd_kafka_name(@inner).gsub('rdkafka', '')}"
          @polling_thread.abort_on_exception = true
          @polling_thread[:closing] = false
        end
      end
    end

    def with_inner
      if @access_mutex.owned?
        @operations_in_progress += 1
      else
        @access_mutex.synchronize { @operations_in_progress += 1 }
      end

      @inner.nil? ? raise(ClosedInnerError) : yield(@inner)
    ensure
      @decrement_mutex.synchronize { @operations_in_progress -= 1 }
    end

    def synchronize(&block)
      @access_mutex.synchronize do
        # Wait for any commands using the inner to finish
        # This can take a while on blocking operations like polling but is essential not to proceed
        # with certain types of operations like resources destruction as it can cause the process
        # to hang or crash
        sleep(0.01) until @operations_in_progress.zero?

        with_inner(&block)
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

      synchronize do
        # Indicate to the outside world that we are closing
        @closing = true

        if @polling_thread
          # Indicate to polling thread that we're closing
          @polling_thread[:closing] = true

          # Wait for the polling thread to finish up,
          # this can be aborted in practice if this
          # code runs from a finalizer.
          @polling_thread.join
        end

        # Destroy the client after locking both mutexes
        @poll_mutex.lock

        # This check prevents a race condition, where we would enter the close in two threads
        # and after unlocking the primary one that hold the lock but finished, ours would be unlocked
        # and would continue to run, trying to destroy inner twice
        return unless @inner

        yield if block_given?

        Rdkafka::Bindings.rd_kafka_destroy(@inner)
        @inner = nil
        @opaque = nil
        @poll_mutex.unlock
        @poll_mutex = nil
      end
    end
  end
end
