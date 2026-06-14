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
      end
    end
  end
end
