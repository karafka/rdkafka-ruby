# frozen_string_literal: true

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

      # The name of the topic this message was produced to or nil in case delivery failed and we
      #   we not able to get the topic reference
      # @return [String, nil]
      attr_reader :topic_name

      # Error in case happen during produce.
      # @return [Integer]
      attr_reader :error

      # @return [Object, nil] label set during message production or nil by default
      attr_reader :label

      # We alias the `#topic_name` under `#topic` to make this consistent with `Consumer::Message`
      # where the topic name is under `#topic` method. That way we have a consistent name that
      # is present in both places
      #
      # We do not remove the original `#topic_name` because of backwards compatibility
      alias topic topic_name

      private

      def initialize(partition, offset, topic_name = nil, error = nil, label = nil)
        @partition = partition
        @offset = offset
        @topic_name = topic_name
        @error = error
        @label = label
      end
    end
  end
end
