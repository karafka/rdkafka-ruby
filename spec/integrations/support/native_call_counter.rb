# frozen_string_literal: true

require "rdkafka"

# Counts calls to selected `Rdkafka::Bindings` functions, so a leak spec can check that native
# allocations and releases balance where RSS is not a usable signal (JRuby: RSS follows the JVM
# heap, not native memory).
module NativeCallCounter
  COUNTS = Hash.new(0)

  class << self
    # Starts counting calls to a binding by prepending a wrapper to `Rdkafka::Bindings`
    #
    # @param name [Symbol] name of the `Rdkafka::Bindings` function
    # @param counted [Proc, nil] optional filter called with the call result; when given, only calls
    #   for which it returns true are counted (e.g. only successful allocations)
    def watch(name, &counted)
      counts = COUNTS

      wrapper = Module.new do
        define_method(name) do |*args, &block|
          result = super(*args, &block)
          counts[name] += 1 if counted.nil? || counted.call(result)
          result
        end
      end

      Rdkafka::Bindings.singleton_class.prepend(wrapper)
    end

    # @param name [Symbol] name of a watched binding
    # @return [Integer] number of counted calls since the last reset
    def count(name)
      COUNTS[name]
    end

    # Clears all the counts
    def reset
      COUNTS.clear
    end

    # Checks that a watched allocation has a matching release for every counted call
    #
    # @param allocate [Symbol] name of the watched allocating binding
    # @param release [Symbol] name of the watched releasing binding
    # @return [Boolean] true when at least one allocation was counted and releases match them
    def balanced?(allocate, release)
      count(allocate).positive? && count(allocate) == count(release)
    end
  end
end
