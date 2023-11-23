# frozen_string_literal: true

module Rdkafka
  class Admin
    class DeleteGroupsReport
      # Any error message generated from the DeleteTopic
      # @return [String]
      attr_reader :error_string

      # The name of the topic deleted
      # @return [String]
      attr_reader :result_name

      def initialize(error_string, result_name)
        if error_string != FFI::Pointer::NULL
          @error_string = error_string.read_string
        end
        if result_name != FFI::Pointer::NULL
          @result_name = @result_name = result_name.read_string
        end
      end
    end
  end
end
