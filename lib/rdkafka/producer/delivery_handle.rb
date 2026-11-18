# frozen_string_literal: true

module Rdkafka
  class Producer
    # Handle to wait for a delivery report which is returned when producing a message.
    class DeliveryHandle < Rdkafka::AbstractHandle
      layout :pending, :bool,
        :response, :int,
        :partition, :int,
        :offset, :int64,
        :status, :int,
        :latency, :int64,
        :broker_id, :int32

      # @return [Object, nil] label set during message production or nil by default
      attr_accessor :label

      # @return [String] topic where we are trying to send the message
      # Set in `#produce`, where the topic is known upfront. Keeping it as a Ruby attribute
      # spares a per-message native string copy in the delivery callback.
      attr_accessor :topic

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
          topic,
          (self[:response] != 0) ? RdkafkaError.new(self[:response], broker_message: broker_message) : nil,
          label,
          status: self[:status],
          latency: self[:latency],
          broker_id: self[:broker_id],
          broker_message: broker_message
        )
      end

      # Raises the delivery error with the error text of the failed delivery
      def raise_error
        raise RdkafkaError.new(self[:response], broker_message: broker_message)
      end
    end
  end
end
