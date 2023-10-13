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
            self[:topic_name].read_string
          )
        else
          DeliveryReport.new(
            self[:partition],
            self[:offset],
            # For part of errors, we will not get a topic name reference and in cases like this
            # we should not return it
            self[:topic_name].null? ? nil : self[:topic_name].read_string,
            Rdkafka::RdkafkaError.build(self[:response])
          )
        end
      end
    end
  end
end
