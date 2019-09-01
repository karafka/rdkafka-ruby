module Rdkafka
  class Producer
    # Delivery report for a successfully produced message.
    class DeliveryReport
      # The partition this message was produced to.
      # @return [Integer]
      attr_reader :partition

      # The offset of the produced message.
      # @return [Integer]
      attr_reader :offset

      # The response of the produced message.
      # @return [string]
      attr_reader :response

      private

      def initialize(partition, offset, response = nil)
        @partition = partition
        @offset = offset
        @response = response
      end
    end
  end
end
