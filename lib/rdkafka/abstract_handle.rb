# frozen_string_literal: true

module Rdkafka
  # This class serves as an abstract base class to represent handles within the Rdkafka module.
  # As a subclass of `FFI::Struct`, this class provides a blueprint for other specific handle
  # classes to inherit from, ensuring they adhere to a particular structure and behavior.
  #
  # Subclasses must define their own layout, and the layout must start with:
  #
  # layout :pending, :bool,
  #        :response, :int
  class AbstractHandle < FFI::Struct
    include Helpers::Time

    # Registry for registering all the handles.
    REGISTRY = {}
    # Default wait timeout is 31 years
    MAX_WAIT_TIMEOUT_FOREVER = 10_000_000_000

    private_constant :MAX_WAIT_TIMEOUT_FOREVER

    class << self
      # Adds handle to the register
      #
      # @param handle [AbstractHandle] any handle we want to register
      def register(handle)
        address = handle.to_ptr.address
        REGISTRY[address] = handle
      end

      # Removes handle from the register based on the handle address
      #
      # @param address [Integer] address of the registered handle we want to remove
      def remove(address)
        REGISTRY.delete(address)
      end
    end

    def initialize
      @mutex = Thread::Mutex.new
      @resource = Thread::ConditionVariable.new

      super
    end

    # Whether the handle is still pending.
    #
    # @return [Boolean]
    def pending?
      self[:pending]
    end

    # Wait for the operation to complete or raise an error if this takes longer than the timeout.
    # If there is a timeout this does not mean the operation failed, rdkafka might still be working
    # on the operation. In this case it is possible to call wait again.
    #
    # @param max_wait_timeout [Numeric, nil] Amount of time to wait before timing out.
    #   If this is nil we will wait forever
    # @param raise_response_error [Boolean] should we raise error when waiting finishes
    #
    # @return [Object] Operation-specific result
    #
    # @raise [RdkafkaError] When the operation failed
    # @raise [WaitTimeoutError] When the timeout has been reached and the handle is still pending
    def wait(max_wait_timeout: 60, raise_response_error: true)
      timeout = max_wait_timeout ? monotonic_now + max_wait_timeout : MAX_WAIT_TIMEOUT_FOREVER

      @mutex.synchronize do
        loop do
          if pending?
            to_wait = (timeout - monotonic_now)

            if to_wait.positive?
              @resource.wait(@mutex, to_wait)
            else
              raise WaitTimeoutError.new(
                "Waiting for #{operation_name} timed out after #{max_wait_timeout} seconds"
              )
            end
          elsif self[:response] != 0 && raise_response_error
            raise_error
          else
            return create_result
          end
        end
      end
    end

    # Unlock the resources
    def unlock
      @mutex.synchronize do
        self[:pending] = false
        @resource.broadcast
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
      RdkafkaError.validate!(self[:response])
    end

    # Error that is raised when waiting for the handle to complete
    # takes longer than the specified timeout.
    class WaitTimeoutError < RuntimeError; end
  end
end
