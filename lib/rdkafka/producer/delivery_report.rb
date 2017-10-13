module Rdkafka
  class Producer
    # Delivery report for a succesfully produced message.
    class DeliveryReport
      # The partition this message was produced to.
      # @return [Integer]
      attr_reader :partition

      # The offset of the produced message.
      # @return [Integer]
      attr_reader :offset

      private

      def initialize(partition, offset)
        @partition = partition
        @offset = offset
      end
    end
  end
end
