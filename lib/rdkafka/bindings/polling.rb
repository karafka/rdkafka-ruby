# frozen_string_literal: true

module Rdkafka
  module Bindings
    # Polling

    attach_function :rd_kafka_flush, [:pointer, :int], :int, blocking: true
    attach_function :rd_kafka_poll, [:pointer, :int], :int, blocking: true
    attach_function :rd_kafka_outq_len, [:pointer], :int, blocking: true

    # Non-blocking poll variants (do not release GVL)
    # These are more efficient for poll(0) calls in fiber schedulers where GVL
    # release/reacquire overhead is wasteful since we don't expect to wait.
    # Uses the same underlying C function but with blocking: false to skip GVL release.
    attach_function :rd_kafka_poll_nb, :rd_kafka_poll, [:pointer, :int], :int, blocking: false
  end
end
