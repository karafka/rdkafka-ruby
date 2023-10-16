# frozen_string_literal: true

module Rdkafka
  class Admin
    class CreateTopicHandle < AbstractHandle
      layout :pending, :bool,
             :response, :int,
             :error_string, :pointer,
             :result_name, :pointer

      # @return [String] the name of the operation
      def operation_name
        "create topic"
      end

      # @return [Boolean] whether the create topic was successful
      def create_result
        CreateTopicReport.new(self[:error_string], self[:result_name])
      end

      def raise_error
        RdkafkaError.validate!(
          self[:response],
          broker_message: CreateTopicReport.new(
            self[:error_string],
            self[:result_name]
          ).error_string
        )
      end
    end
  end
end
