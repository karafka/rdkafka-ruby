# frozen_string_literal: true

module Rdkafka
  module Callbacks
    # Base class for admin background-event result handlers.
    #
    # Each subclass handles one librdkafka admin result event type: it extracts the result from
    # the event, looks up the matching {AbstractHandle} in the registry and resolves it (sets the
    # response, builds the report, copies the broker message, then unlocks it).
    #
    # The event is owned by the application and destroyed by {BackgroundEventCallback} right after
    # dispatch, so handlers must copy everything they need out of event-owned memory into Ruby
    # objects before returning.
    #
    # @private
    class BaseHandler
      class << self
        # Processes a background event for the operation this handler is responsible for.
        # @param event_ptr [FFI::Pointer] pointer to the event
        # @return [void]
        def call(event_ptr)
          raise NotImplementedError, "#{name} must implement .call"
        end

        private

        # Copies an event-owned C string into a Ruby string
        # @param ptr [FFI::Pointer] pointer to the C string
        # @return [String, nil] copied string or nil when NULL
        def read_event_string(ptr)
          ptr.null? ? nil : ptr.read_string
        end

        # Resolves a handle directly from an operation-level event error and unlocks it.
        #
        # librdkafka signals a failure of the whole admin operation (e.g. `_TIMED_OUT` when the
        # brokers are unreachable, or `_DESTROY` when the client is closed with the request in
        # flight) by setting the event error and delivering an *empty* results array. The
        # per-element result handlers must not index into that empty array; they call this first
        # and skip results parsing when it returns true. Without this, indexing `results[0]`
        # raises inside the FFI callback, the handle is never unlocked, and the caller's `wait`
        # blocks until its own timeout and raises `WaitTimeoutError`, losing the real error code.
        #
        # @param event_ptr [FFI::Pointer] pointer to the event
        # @param handle [Rdkafka::AbstractHandle] the handle to resolve
        # @return [Boolean] true when the event carried an operation-level error and the handle
        #   was resolved here
        def resolve_operation_error(event_ptr, handle)
          error_code = Rdkafka::Bindings.rd_kafka_event_error(event_ptr)

          return false if error_code == Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR

          handle[:response] = error_code
          handle.broker_message = read_event_string(Rdkafka::Bindings.rd_kafka_event_error_string(event_ptr))
          handle.unlock

          true
        end
      end
    end
  end
end
