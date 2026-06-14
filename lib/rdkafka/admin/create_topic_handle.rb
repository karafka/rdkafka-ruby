# frozen_string_literal: true

module Rdkafka
  class Admin
    # Handle for create topic operation
    class CreateTopicHandle < AbstractHandle
      layout :pending, :bool,
        :response, :int

      # @return [String] the name of the operation
      def operation_name
        "create topic"
      end
    end
  end
end
