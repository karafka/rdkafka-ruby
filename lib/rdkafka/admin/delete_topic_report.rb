# frozen_string_literal: true

module Rdkafka
  class Admin
    # Report for delete topic operation result
    class DeleteTopicReport
      # Any error message generated from the DeleteTopic
      # @return [String]
      attr_reader :error_string

      # The name of the topic deleted
      # @return [String]
      attr_reader :result_name

      # @param error_string [FFI::Pointer] pointer to error string
      # @param result_name [FFI::Pointer] pointer to topic name
      def initialize(error_string, result_name)
        if error_string != FFI::Pointer::NULL
          @error_string = error_string.read_string
        end
        if result_name != FFI::Pointer::NULL
          @result_name = result_name.read_string
        end
      end
    end
  end
end
