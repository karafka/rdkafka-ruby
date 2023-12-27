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

      # @return [String] the name of the operation (e.g. "delivery")
      def operation_name
        "delivery"
      end

      # @return [DeliveryReport] a report on the delivery of the message
      def create_result
        DeliveryReport.new(
          self[:partition],
          self[:offset],
          # For part of errors, we will not get a topic name reference and in cases like this
          # we should not return it
          self[:topic_name].null? ? nil : self[:topic_name].read_string,
          self[:response] != 0 ? RdkafkaError.new(self[:response]) : nil,
          label
        )
      end
    end
  end
end
