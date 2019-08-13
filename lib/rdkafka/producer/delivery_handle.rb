module Rdkafka
  class Producer
    # Handle to wait for a delivery report which is returned when
    # producing a message.
    class DeliveryHandle < FFI::Struct
      layout :pending, :bool,
             :response, :int,
             :partition, :int,
             :offset, :int64

      REGISTRY = {}

      def self.register(address, handle)
        REGISTRY[address] = handle
      end

      def self.remove(address)
        REGISTRY.delete(address)
      end

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
      # @param max_wait_timeout [Integer, nil] Number of seconds to wait before timing out. If this is nil it does not time out.
      # @param wait_timeout [Numeric] Time we should wait before we recheck if there is a delivery report available
      #
      # @raise [RdkafkaError] When delivering the message failed
      # @raise [WaitTimeoutError] When the timeout has been reached and the handle is still pending
      #
      # @return [DeliveryReport]
      def wait(max_wait_timeout = 60, wait_timeout = 0.1)
        timeout = if max_wait_timeout
                    Time.now.to_i + max_wait_timeout
                  else
                    nil
                  end
        loop do
          if pending?
            if timeout && timeout <= Time.now.to_i
              raise WaitTimeoutError.new("Waiting for delivery timed out after #{max_wait_timeout} seconds")
            end
            sleep wait_timeout
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
