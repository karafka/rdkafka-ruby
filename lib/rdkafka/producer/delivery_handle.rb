module Rdkafka
  class Producer
    # Handle to wait for a delivery report which is returned when
    # producing a message.
    class DeliveryHandle < ::FFI::Struct
      layout :pending, :bool,
             :response, :int,
             :partition, :int,
             :offset, :int64

      # Whether the delivery handle is still pending.
      #
      # @return [Boolean]
      def pending?
        self[:pending]
      end

      # Wait for the delivery report or raise an error if this takes longer than the timeout.
      # If there is a timeout this does not mean the message is not delivered, rdkafka might still be working on delivering the message.
      # In this case it is possible to call wait again.
      #
      # @param timeout_in_seconds [Integer, nil] Number of seconds to wait before timing out. If this is nil it does not time out.
      #
      # @raise [RdkafkaError] When delivering the message failed
      # @raise [WaitTimeoutError] When the timeout has been reached and the handle is still pending
      #
      # @return [DeliveryReport]
      def wait(timeout_in_seconds=60)
        timeout = if timeout_in_seconds
                    Time.now.to_i + timeout_in_seconds
                  else
                    nil
                  end
        loop do
          if pending?
            if timeout && timeout <= Time.now.to_i
              raise WaitTimeoutError.new("Waiting for delivery timed out after #{timeout_in_seconds} seconds")
            end
            sleep 0.1
            next
          elsif self[:response] != 0
            raise RdkafkaError.new(self[:response])
          else
            return DeliveryReport.new(self[:partition], self[:offset])
          end
        end
      end

      # Error that is raised when waiting for a delivery handle to complete
      # takes longer than the specified timeout.
      class WaitTimeoutError < RuntimeError; end
    end
  end
end
