# frozen_string_literal: true

module Rdkafka
  # Namespace for some small utilities used in multiple components
  module Helpers
    # Time related methods used across Karafka
    module Time
      # @return [Float] current monotonic time in seconds with microsecond precision
      def monotonic_now
        ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
      end
    end
  end
end
