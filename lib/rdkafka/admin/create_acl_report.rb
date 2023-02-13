# frozen_string_literal: true

module Rdkafka
  class Admin
    class CreateACLReport
      # Any error message generated from the CreateACL
      # @return [String, Integer]
      attr_reader :error_string, :error_code

      def initialize(error_string, error_code)
        @error_string = error_string.read_string if error_string != FFI::Pointer::NULL

        return unless error_code != 0

        @error_code = error_code
      end
    end
  end
end
