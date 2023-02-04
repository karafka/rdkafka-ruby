# frozen_string_literal: true

module Rdkafka
  class Admin
    class CreateAclReport
      # Any error message generated from the CreateAcl
      # @return [String]
      attr_reader :error_string

      def initialize(error_string)
        if error_string != FFI::Pointer::NULL
          @error_string = error_string.read_string
        end
      end
    end
  end
end
