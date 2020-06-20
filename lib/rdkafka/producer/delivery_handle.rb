module Rdkafka
  class Producer
    # Handle to wait for a delivery report which is returned when
    # producing a message.
    class DeliveryHandle < Rdkafka::AbstractHandle
      layout :pending, :bool,
             :response, :int,
             :partition, :int,
             :offset, :int64

      # @return [String] the name of the operation (e.g. "delivery")
      def operation_name
        "delivery"
      end

      # @return [DeliveryReport] a report on the delivery of the message
      def create_result
        DeliveryReport.new(self[:partition], self[:offset])
      end
    end
  end
end
