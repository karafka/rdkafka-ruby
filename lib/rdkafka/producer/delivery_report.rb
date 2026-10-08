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
      #
      # @return [String, nil]
      attr_reader :topic_name

      # Error in case happen during produce.
      # @return [Integer]
      attr_reader :error

      # @return [Object, nil] label set during message production or nil by default
      attr_reader :label

      # Persistence status of the message, one of `Bindings::RD_KAFKA_MSG_STATUS_*`:
      #   - `RD_KAFKA_MSG_STATUS_NOT_PERSISTED` - the message was not written to the broker. A retry
      #     risks reordering, but not a duplicate.
      #   - `RD_KAFKA_MSG_STATUS_POSSIBLY_PERSISTED` - the message may have been written (e.g. the
      #     request timed out before an ack). A retry risks reordering and a duplicate.
      #   - `RD_KAFKA_MSG_STATUS_PERSISTED` - the broker acknowledged the message. Trust this only
      #     with `acks=all`.
      # `nil` when the report was not built from a delivery callback.
      # @return [Integer, nil]
      attr_reader :status

      # Time from the `produce` call until the delivery report, in microseconds, or `nil` when
      #   not available
      # @return [Integer, nil]
      attr_reader :latency

      # Id of the broker the message was produced to, or `nil` when not known
      # @return [Integer, nil]
      attr_reader :broker_id

      # We alias the `#topic_name` under `#topic` to make this consistent with `Consumer::Message`
      # where the topic name is under `#topic` method. That way we have a consistent name that
      # is present in both places
      #
      # We do not remove the original `#topic_name` because of backwards compatibility
      alias_method :topic, :topic_name

      # @private
      # @param partition [Integer] partition number
      # @param offset [Integer] message offset
      # @param topic_name [String, nil] topic name
      # @param error [Integer, nil] error code if any
      # @param label [Object, nil] user-defined label
      # @param status [Integer, nil] `Bindings::RD_KAFKA_MSG_STATUS_*` persistence status
      # @param latency [Integer, nil] produce latency in microseconds, negative when not available
      # @param broker_id [Integer, nil] broker id, negative when not known
      def initialize(
        partition,
        offset,
        topic_name = nil,
        error = nil,
        label = nil,
        status: nil,
        latency: nil,
        broker_id: nil
      )
        @partition = partition
        @offset = offset
        @topic_name = topic_name
        @error = error
        @label = label
        @status = status
        @latency = (latency.nil? || latency.negative?) ? nil : latency
        @broker_id = (broker_id.nil? || broker_id.negative?) ? nil : broker_id
      end

      # @return [Boolean] true when the broker acknowledged the message
      def persisted?
        status == Bindings::RD_KAFKA_MSG_STATUS_PERSISTED
      end

      # @return [Boolean] true when the message may have been written, so a retry may duplicate it
      def possibly_persisted?
        status == Bindings::RD_KAFKA_MSG_STATUS_POSSIBLY_PERSISTED
      end

      # @return [Boolean] true when the message was not written, so a retry does not duplicate it
      def not_persisted?
        status == Bindings::RD_KAFKA_MSG_STATUS_NOT_PERSISTED
      end
    end
  end
end
