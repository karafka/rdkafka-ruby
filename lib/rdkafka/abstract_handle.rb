require "ffi"

module Rdkafka
  class AbstractHandle < FFI::Struct
    # Subclasses must define their own layout, and the layout must start with:
    #
    # layout :pending, :bool,
    #        :response, :int

    REGISTRY = {}

    CURRENT_TIME = -> { Process.clock_gettime(Process::CLOCK_MONOTONIC) }.freeze

    private_constant :CURRENT_TIME

    def self.register(handle)
      address = handle.to_ptr.address
      REGISTRY[address] = handle
    end

    def self.remove(address)
      REGISTRY.delete(address)
    end

    # Whether the handle is still pending.
    #
    # @return [Boolean]
    def pending?
      self[:pending]
    end

    # Wait for the operation to complete or raise an error if this takes longer than the timeout.
    # If there is a timeout this does not mean the operation failed, rdkafka might still be working on the operation.
    # In this case it is possible to call wait again.
    #
    # @param max_wait_timeout [Numeric, nil] Amount of time to wait before timing out. If this is nil it does not time out.
    # @param wait_timeout [Numeric] Amount of time we should wait before we recheck if the operation has completed
    #
    # @raise [RdkafkaError] When the operation failed
    # @raise [WaitTimeoutError] When the timeout has been reached and the handle is still pending
    #
    # @return [Object] Operation-specific result
    def wait(max_wait_timeout: 60, wait_timeout: 0.1)
      timeout = if max_wait_timeout
                  CURRENT_TIME.call + max_wait_timeout
                else
                  nil
                end
      loop do
        if pending?
          if timeout && timeout <= CURRENT_TIME.call
            raise WaitTimeoutError.new("Waiting for #{operation_name} timed out after #{max_wait_timeout} seconds")
          end
          sleep wait_timeout
        elsif self[:response] != 0
          raise_error
        else
          return create_result
        end
      end
    end

    # @return [String] the name of the operation (e.g. "delivery")
    def operation_name
      raise "Must be implemented by subclass!"
    end

    # @return [Object] operation-specific result
    def create_result
      raise "Must be implemented by subclass!"
    end

    # Allow subclasses to override
    def raise_error
      raise RdkafkaError.new(self[:response])
    end

    # Error that is raised when waiting for the handle to complete
    # takes longer than the specified timeout.
    class WaitTimeoutError < RuntimeError; end
  end
end
