# frozen_string_literal: true

module Rdkafka
  class Producer
    # Handle to wait for a delivery report which is returned when
    # producing a message.
    class DeliveryHandle < Rdkafka::AbstractHandle
      layout :pending, :bool,
             :response, :int,
             :partition, :int,
             :offset, :int64,
             :topic_name, :pointer

      # @return [Object, nil] label set during message production or nil by default
      attr_accessor :label

      # @return [String] topic where we are trying to send the message
      # We use this instead of reading from `topic_name` pointer to save on memory allocations
      attr_accessor :topic

      # @return [String] the name of the operation (e.g. "delivery")
      def operation_name
        "delivery"
      end

      # @return [DeliveryReport] a report on the delivery of the message
      def create_result
        if self[:response] == 0
          DeliveryReport.new(
            self[:partition],
            self[:offset],
            topic,
            nil,
            label
          )
        else
          DeliveryReport.new(
            self[:partition],
            self[:offset],
            topic,
            Rdkafka::RdkafkaError.build(self[:response]),
            label
          )
        end
      end
    end
  end
end
