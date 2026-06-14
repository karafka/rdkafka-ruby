# frozen_string_literal: true

module Rdkafka
  class Admin
    # Handle for delete topic operation
    class DeleteTopicHandle < AbstractHandle
      layout :pending, :bool,
        :response, :int

      # @return [String] the name of the operation
      def operation_name
        "delete topic"
      end
    end
  end
end
