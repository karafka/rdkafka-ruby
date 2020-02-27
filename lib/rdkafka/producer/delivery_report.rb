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

      # Error in case happen during produce.
      # @return [string]
      attr_reader :error

      private

      def initialize(partition, offset, error = nil)
        @partition = partition
        @offset = offset
        @error = error
      end
    end
  end
end
